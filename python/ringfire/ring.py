import os
import time
import mmap
import fcntl
import ctypes
import struct
from typing import Optional, Type, List, Tuple
from enum import Enum

RINGFIRE_MAGIC = 0x52494E4746495245
RINGFIRE_VERSION = 2

HEADER_SIZE = 128

# Slot sequence stored while a writer overwrites the slot payload (protocol v2).
SLOT_WRITING = 0xFFFF_FFFF_FFFF_FFFF

FLAG_POLICY_LATEST_WINS = 0x0001
FLAG_MODE_SPMC = 0x0010

# ArenaHeader layout: capacity (u64) @0, mask (u64) @8, reserved (u64) @16, pad to 64.
ARENA_CAPACITY_OFFSET = 0
ARENA_MASK_OFFSET = 8
ARENA_RESERVED_OFFSET = 16
ARENA_HEADER_SIZE = 64

_U64 = struct.Struct("<Q")


class RingHeader(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint64),
        ("version", ctypes.c_uint32),
        ("element_size", ctypes.c_uint32),
        ("capacity", ctypes.c_uint64),
        ("mask", ctypes.c_uint64),
        ("write_seq", ctypes.c_uint64),
        ("claim_seq", ctypes.c_uint64),
        ("flags", ctypes.c_uint32),
        ("futex_word", ctypes.c_uint32),
        ("waiting_consumers", ctypes.c_uint32),
        ("_align_pad", ctypes.c_uint32),
        ("read_seq", ctypes.c_uint64),
        ("schema_sig", ctypes.c_uint64),
        ("arena_offset", ctypes.c_uint64),
        ("arena_size", ctypes.c_uint64),
        ("reader_registry_offset", ctypes.c_uint32),
        ("reader_registry_count", ctypes.c_uint32),
        ("slots_offset", ctypes.c_uint64),
        ("_pad", ctypes.c_uint8 * 16),
    ]


assert ctypes.sizeof(RingHeader) == HEADER_SIZE


class RecvStatus(Enum):
    OK = 1
    EMPTY = 2
    LAPPED = 3


SHM_OFFSET_MAGIC = 0x53484D5F4F464653
SHM_OFFSET_VERSION = 1


def _slot_stride(payload_size: int) -> int:
    return ((8 + payload_size + 7) // 8) * 8


def _oldest_retained(write_seq: int, capacity: int) -> int:
    return write_seq - capacity + 1 if write_seq >= capacity else 1


def _map_ring(path: str):
    """Opens and maps a ring file, validating its header and layout. Returns (fd, mm, header)."""
    fd = os.open(path, os.O_RDWR)
    try:
        file_size = os.fstat(fd).st_size
        if file_size < HEADER_SIZE:
            raise ValueError(f"{path}: file too small ({file_size} bytes) for a ring header")
        mm = mmap.mmap(fd, file_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
    except BaseException:
        os.close(fd)
        raise
    header = RingHeader.from_buffer(mm, 0)
    try:
        if header.magic != RINGFIRE_MAGIC:
            raise ValueError(f"Invalid magic: expected 0x{RINGFIRE_MAGIC:016X}, got 0x{header.magic:016X}")
        if header.version != RINGFIRE_VERSION:
            raise ValueError(f"Version mismatch: expected {RINGFIRE_VERSION}, got {header.version}")
        cap = header.capacity
        if cap == 0 or cap & (cap - 1) or header.mask != cap - 1:
            raise ValueError("Corrupt ring header: capacity is not a power of two")
        if header.slots_offset < HEADER_SIZE or header.slots_offset + cap * header.element_size > file_size:
            raise ValueError("Corrupt ring header: slots extend past the end of the file")
    except BaseException:
        del header
        mm.close()
        os.close(fd)
        raise
    return fd, mm, header


class RingConsumer:
    """Zero-copy reader for ringfire shared memory ring buffer.

    Python readers do not register in the ring's reader registry, so a producer using
    lossless backpressure does not wait for them: they may be lapped like on a lossy ring.
    """

    def __init__(
        self,
        path: str,
        struct_cls: Type[ctypes.Structure],
        start_mode: str = "oldest",  # "latest", "head", "oldest", "sequence"
        offset_file: Optional[str] = None,
        consumer_name: Optional[str] = None,
        start_seq: Optional[int] = None,
    ):
        self.path = path
        self.struct_cls = struct_cls
        self.payload_size = ctypes.sizeof(struct_cls)
        self.offset_file = offset_file
        self.offset_mm = None
        self.offset_fd = -1

        self.fd, self.mm, self.header = _map_ring(path)
        if self.header.element_size != _slot_stride(self.payload_size):
            stride = self.header.element_size
            self.close()
            raise ValueError(f"Element size mismatch: ring slots are {stride} bytes, "
                             f"{struct_cls.__name__} needs {_slot_stride(self.payload_size)}")

        self.slot_stride = self.header.element_size
        self.capacity = self.header.capacity
        self.mask = self.header.mask
        self.slots_offset = self.header.slots_offset

        write_seq = self.header.write_seq
        oldest = _oldest_retained(write_seq, self.capacity)
        self.lapped_count = 0

        # Handle offset file if provided or derived from consumer_name
        if offset_file is None and consumer_name is not None:
            dirname = os.path.dirname(path) or "/dev/shm"
            stem = os.path.splitext(os.path.basename(path))[0]
            safe_name = "".join(c if (c.isalnum() or c in ("_", "-")) else "_" for c in consumer_name)
            self.offset_file = os.path.join(dirname, f"{stem}_{safe_name}.offset")

        saved_seq = None
        if self.offset_file:
            self._init_offset_shm(consumer_name or "py_consumer")
            saved_seq = self._load_offset()

        if saved_seq is not None and saved_seq > 0:
            target = saved_seq + 1
            if target < oldest:
                self.lapped_count += oldest - target
                self.cursor = oldest
            else:
                self.cursor = target
        elif start_mode == "latest":
            self.cursor = write_seq if write_seq > 0 else 1
        elif start_mode == "head":
            self.cursor = write_seq + 1
        elif start_mode == "sequence":
            if start_seq is None:
                raise ValueError("start_seq must be provided when start_mode is 'sequence'")
            start_seq = max(1, start_seq)
            if start_seq < oldest:
                self.lapped_count += oldest - start_seq
                self.cursor = oldest
            else:
                self.cursor = start_seq
        elif start_mode == "oldest":
            self.cursor = oldest
        else:
            raise ValueError(f"Unknown start_mode: '{start_mode}'")

    def _init_offset_shm(self, name: str):
        offset_dir = os.path.dirname(self.offset_file)
        if offset_dir:
            os.makedirs(offset_dir, exist_ok=True)
        self.offset_fd = os.open(self.offset_file, os.O_RDWR | os.O_CREAT, 0o660)
        if os.fstat(self.offset_fd).st_size < 64:
            os.ftruncate(self.offset_fd, 64)
            self.offset_mm = mmap.mmap(self.offset_fd, 64, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
            name_bytes = name.encode("utf-8")[:31].ljust(32, b"\x00")
            header_bytes = struct.pack("<QIIQQ", SHM_OFFSET_MAGIC, SHM_OFFSET_VERSION, os.getpid(), 0, int(time.time() * 1e9))
            self.offset_mm[0:32] = header_bytes
            self.offset_mm[32:64] = name_bytes
        else:
            self.offset_mm = mmap.mmap(self.offset_fd, 64, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)
            magic, version = struct.unpack_from("<QI", self.offset_mm, 0)
            if magic != SHM_OFFSET_MAGIC:
                raise ValueError(f"Invalid SHM offset magic: 0x{magic:016X}")

    def _load_offset(self) -> Optional[int]:
        if self.offset_mm is None:
            return None
        return struct.unpack_from("<Q", self.offset_mm, 16)[0]

    def commit_offset(self):
        """Atomically commits current cursor - 1 to shared memory."""
        if self.offset_mm is None:
            raise ValueError("No offset file configured for this consumer")
        last_seq = max(0, self.cursor - 1)
        now_ns = int(time.time() * 1e9)
        struct.pack_into("<Q", self.offset_mm, 16, last_seq)
        struct.pack_into("<Q", self.offset_mm, 24, now_ns)

    def seek(self, target_seq: int) -> int:
        """Seeks cursor to target sequence number, handling lapping if necessary."""
        oldest = _oldest_retained(self.header.write_seq, self.capacity)
        target_seq = max(1, target_seq)
        if target_seq < oldest:
            skipped = oldest - target_seq
            self.lapped_count += skipped
            self.cursor = oldest
            return skipped
        self.cursor = target_seq
        return 0

    def last_processed_sequence(self) -> int:
        return max(0, self.cursor - 1)

    def try_recv(self) -> Optional[ctypes.Structure]:
        """Attempts to read the next message without blocking."""
        status, item, _ = self.recv_status()
        return item if status in (RecvStatus.OK, RecvStatus.LAPPED) else None

    def _skip_overwritten(self, seen: int) -> int:
        oldest = _oldest_retained(max(self.header.write_seq, seen), self.capacity)
        if oldest > self.cursor:
            skipped = oldest - self.cursor
            self.lapped_count += skipped
            self.cursor = oldest
            return skipped
        return 0

    def recv_status(self) -> Tuple[RecvStatus, Optional[ctypes.Structure], int]:
        """Attempts to read with status and skipped count."""
        skipped = 0
        while True:
            want = self.cursor
            slot_offset = self.slots_offset + (want & self.mask) * self.slot_stride
            s1 = _U64.unpack_from(self.mm, slot_offset)[0]

            if s1 == want:
                item = self.struct_cls.from_buffer_copy(self.mm, slot_offset + 8)
                s2 = _U64.unpack_from(self.mm, slot_offset)[0]
                if s2 == want:
                    self.cursor += 1
                    if skipped:
                        return RecvStatus.LAPPED, item, skipped
                    return RecvStatus.OK, item, 0
                seen = 0 if s2 == SLOT_WRITING else s2
            elif s1 == SLOT_WRITING or s1 < want:
                if self.header.write_seq >= want + self.capacity:
                    seen = 0  # producer is a full ring ahead: the message is gone
                else:
                    return RecvStatus.EMPTY, None, skipped
            else:
                seen = s1

            s = self._skip_overwritten(seen)
            if s == 0:
                return RecvStatus.EMPTY, None, skipped
            skipped += s

    def recv_batch(self, max_count: int = 128) -> List[ctypes.Structure]:
        """Batch read up to max_count messages."""
        items = []
        for _ in range(max_count):
            msg = self.try_recv()
            if msg is None:
                break
            items.append(msg)
        return items

    def close(self):
        if getattr(self, "offset_mm", None) is not None:
            try:
                self.offset_mm.close()
            except Exception:
                pass
            self.offset_mm = None
        if getattr(self, "offset_fd", -1) >= 0:
            try:
                os.close(self.offset_fd)
            except Exception:
                pass
            self.offset_fd = -1

        self.header = None
        if getattr(self, "mm", None) is not None:
            try:
                self.mm.close()
            except Exception:
                pass
            self.mm = None
        if getattr(self, "fd", -1) >= 0:
            try:
                os.close(self.fd)
            except Exception:
                pass
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _create_locked(path: str, total_size: int) -> int:
    """Creates the ring file under an exclusive flock (same protocol as the Rust producer):
    fails without touching a live ring, and replaces a stale file with a fresh inode."""
    for _ in range(8):
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o660)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise FileExistsError(f"{path}: another producer holds this ring")
        try:
            locked = os.fstat(fd)
            current = os.stat(path)
        except FileNotFoundError:
            os.close(fd)
            continue
        if (locked.st_dev, locked.st_ino) != (current.st_dev, current.st_ino):
            os.close(fd)
            continue
        if locked.st_size != 0:
            os.unlink(path)
            os.close(fd)
            continue
        os.ftruncate(fd, total_size)
        return fd
    raise FileExistsError(f"{path}: could not acquire the ring file")


class RingProducer:
    """Writer for ringfire shared memory ring buffer.

    Holds an exclusive flock on the file for its lifetime. Python has no memory fences:
    payload/sequence ordering relies on the hardware (safe on x86-64 TSO; on AArch64 prefer
    a Rust or C producer).
    """

    def __init__(self, path: str, struct_cls: Type[ctypes.Structure], capacity: int = 65536):
        if capacity <= 0 or capacity & (capacity - 1) != 0:
            raise ValueError(f"Capacity {capacity} must be a power of two")

        self.path = path
        self.struct_cls = struct_cls
        self.payload_size = ctypes.sizeof(struct_cls)
        self.slot_stride = _slot_stride(self.payload_size)
        self.capacity = capacity
        self.mask = capacity - 1
        self.slots_offset = HEADER_SIZE
        self.seq = 1

        total_size = HEADER_SIZE + (capacity * self.slot_stride)

        self.fd = _create_locked(path, total_size)
        self.mm = mmap.mmap(self.fd, total_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)

        self.header = RingHeader.from_buffer(self.mm, 0)
        self.header.magic = 0
        self.header.version = RINGFIRE_VERSION
        self.header.element_size = self.slot_stride
        self.header.capacity = capacity
        self.header.mask = capacity - 1
        self.header.write_seq = 0
        self.header.claim_seq = 0
        self.header.flags = FLAG_MODE_SPMC | FLAG_POLICY_LATEST_WINS
        self.header.futex_word = 0
        self.header.waiting_consumers = 0
        self.header.slots_offset = self.slots_offset
        # The file is freshly created (zero-filled): publish the ring by writing magic last.
        self.header.magic = RINGFIRE_MAGIC

    def push(self, item: ctypes.Structure):
        """Publishes a message into the ring buffer."""
        slot_offset = self.slots_offset + (self.seq & self.mask) * self.slot_stride
        _U64.pack_into(self.mm, slot_offset, SLOT_WRITING)
        self.mm[slot_offset + 8:slot_offset + 8 + self.payload_size] = bytes(item)
        _U64.pack_into(self.mm, slot_offset, self.seq)
        self.header.write_seq = self.seq
        self.seq += 1

    def close(self):
        self.header = None
        if getattr(self, "mm", None) is not None:
            self.mm.close()
            self.mm = None
        if getattr(self, "fd", -1) >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class BlobConsumer:
    """Zero-copy reader for variable-sized binary payloads in ringfire shared memory.

    `try_recv` returns a memoryview straight into the shared arena. It is checked against
    the producer's reservation counter before being returned, but the producer keeps
    writing: the bytes stay valid only until the arena wraps around to them again. Use
    `try_recv_copy` when the payload must outlive that window: it copies, then validates.
    """

    def __init__(
        self,
        path: str,
        meta_cls: Type[ctypes.Structure],
        consumer_name: Optional[str] = None,
    ):
        self.path = path
        self.meta_cls = meta_cls
        self.meta_size = ctypes.sizeof(meta_cls)

        self.fd, self.mm, self.header = _map_ring(path)
        if self.header.arena_offset == 0:
            self.close()
            raise ValueError("Queue does not contain a PayloadArena")

        self.capacity = self.header.capacity
        self.mask = self.header.mask
        self.slot_stride = self.header.element_size
        self.slots_offset = self.header.slots_offset

        # Arena layout
        self.arena_offset = self.header.arena_offset
        self.arena_size = self.header.arena_size
        arena_cap = _U64.unpack_from(self.mm, self.arena_offset + ARENA_CAPACITY_OFFSET)[0]
        self.arena_mask = _U64.unpack_from(self.mm, self.arena_offset + ARENA_MASK_OFFSET)[0]
        if arena_cap != self.arena_size or self.arena_mask != arena_cap - 1:
            self.close()
            raise ValueError("Corrupt arena header")
        self.arena_data_offset = self.arena_offset + ARENA_HEADER_SIZE

        # BlobPacket<M> = { meta: M, blob_ref: BlobRef (u64 offset, u32 len, u32 flags) };
        # BlobRef is 8-aligned, so it starts at meta_size rounded up to 8.
        self.blob_ref_offset = ((self.meta_size + 7) // 8) * 8
        expected = _slot_stride(self.blob_ref_offset + 16)
        if self.slot_stride != expected:
            stride = self.slot_stride
            self.close()
            raise ValueError(f"Element size mismatch: ring slots are {stride} bytes, "
                             f"{meta_cls.__name__} metadata needs {expected}")

        # Default cursor: start from oldest
        self.cursor = _oldest_retained(self.header.write_seq, self.capacity)
        self.lapped_count = 0

    def _arena_reserved(self) -> int:
        return _U64.unpack_from(self.mm, self.arena_offset + ARENA_RESERVED_OFFSET)[0]

    def _blob_lapped(self, blob_offset: int) -> bool:
        return self._arena_reserved() - blob_offset > self.arena_size

    def _next(self):
        """Returns (meta, blob_offset, start, end) for the next intact message, or None."""
        while True:
            want = self.cursor
            slot_off = self.slots_offset + (want & self.mask) * self.slot_stride
            s1 = _U64.unpack_from(self.mm, slot_off)[0]

            if s1 == want:
                meta = self.meta_cls.from_buffer_copy(self.mm, slot_off + 8)
                blob_offset, blob_len, _flags = struct.unpack_from(
                    "<QII", self.mm, slot_off + 8 + self.blob_ref_offset)
                s2 = _U64.unpack_from(self.mm, slot_off)[0]
                if s2 == want:
                    start = self.arena_data_offset + (blob_offset & self.arena_mask)
                    end = start + blob_len
                    in_bounds = (blob_offset & self.arena_mask) + blob_len <= self.arena_size
                    if blob_len and (not in_bounds or self._blob_lapped(blob_offset)):
                        self.cursor += 1
                        self.lapped_count += 1
                        continue
                    self.cursor += 1
                    return meta, blob_offset, start, end
                seen = 0 if s2 == SLOT_WRITING else s2
            elif s1 == SLOT_WRITING or s1 < want:
                return None
            else:
                seen = s1

            oldest = _oldest_retained(max(self.header.write_seq, seen), self.capacity)
            if oldest <= self.cursor:
                return None
            self.lapped_count += oldest - self.cursor
            self.cursor = oldest

    def try_recv(self) -> Optional[Tuple[ctypes.Structure, memoryview]]:
        """Reads the next available (metadata, payload view) tuple without blocking."""
        nxt = self._next()
        if nxt is None:
            return None
        meta, _blob_offset, start, end = nxt
        if start == end:
            return meta, memoryview(b"")
        return meta, memoryview(self.mm)[start:end]

    def try_recv_copy(self) -> Optional[Tuple[ctypes.Structure, bytes]]:
        """Like `try_recv`, but copies the payload and verifies the copy was not overwritten
        while it was taken. Overwritten messages are skipped and counted in `lapped_count`."""
        while True:
            nxt = self._next()
            if nxt is None:
                return None
            meta, blob_offset, start, end = nxt
            data = self.mm[start:end]
            if start == end or not self._blob_lapped(blob_offset):
                return meta, data
            self.lapped_count += 1

    def close(self):
        self.header = None
        if getattr(self, "mm", None) is not None:
            try:
                self.mm.close()
            except BufferError:
                pass
            self.mm = None
        if getattr(self, "fd", -1) >= 0:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
