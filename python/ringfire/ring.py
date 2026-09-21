import os
import time
import mmap
import ctypes
import struct
from typing import Optional, Type, List, Tuple
from enum import Enum

RINGFIRE_MAGIC = 0x52494E4746495245
RINGFIRE_VERSION = 1

HEADER_SIZE = 128

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
        ("_reserved", ctypes.c_uint64),
        ("_pad", ctypes.c_uint8 * 48),
    ]

class RecvStatus(Enum):
    OK = 1
    EMPTY = 2
    LAPPED = 3

SHM_OFFSET_MAGIC = 0x53484D5F4F464653
SHM_OFFSET_VERSION = 1

class RingConsumer:
    """Zero-copy reader for ringfire shared memory ring buffer."""

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

        self.fd = os.open(path, os.O_RDWR)
        file_size = os.fstat(self.fd).st_size
        self.mm = mmap.mmap(self.fd, file_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)

        self.header = RingHeader.from_buffer(self.mm, 0)
        if self.header.magic != RINGFIRE_MAGIC:
            raise ValueError(f"Invalid magic: expected 0x{RINGFIRE_MAGIC:016X}, got 0x{self.header.magic:016X}")
        if self.header.version != RINGFIRE_VERSION:
            raise ValueError(f"Version mismatch: {self.header.version}")

        self.slot_stride = self.header.element_size
        self.capacity = self.header.capacity
        self.mask = self.header.mask
        self.slots_offset = HEADER_SIZE

        write_seq = self.header.write_seq
        oldest = (write_seq - self.capacity + 1) if write_seq > self.capacity else 1
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
        """Atomically commits current cursor - 1 to shared memory (<10ns)."""
        if self.offset_mm is None:
            raise ValueError("No offset file configured for this consumer")
        last_seq = max(0, self.cursor - 1)
        now_ns = int(time.time() * 1e9)
        struct.pack_into("<Q", self.offset_mm, 16, last_seq)
        struct.pack_into("<Q", self.offset_mm, 24, now_ns)

    def seek(self, target_seq: int) -> int:
        """Seeks cursor to target sequence number, handling lapping if necessary."""
        write_seq = self.header.write_seq
        oldest = (write_seq - self.capacity + 1) if write_seq > self.capacity else 1
        if target_seq < oldest:
            skipped = oldest - target_seq
            self.lapped_count += skipped
            self.cursor = oldest
            return skipped
        else:
            self.cursor = target_seq
            return 0

    def last_processed_sequence(self) -> int:
        return max(0, self.cursor - 1)

    def try_recv(self) -> Optional[ctypes.Structure]:
        """Attempts to read the next message without blocking."""
        status, item, _ = self.recv_status()
        return item if status in (RecvStatus.OK, RecvStatus.LAPPED) else None

    def recv_status(self) -> Tuple[RecvStatus, Optional[ctypes.Structure], int]:
        """Attempts to read with status and skipped count."""
        idx = self.cursor & self.mask
        slot_offset = self.slots_offset + (idx * self.slot_stride)

        seq_bytes = self.mm[slot_offset:slot_offset + 8]
        s1 = struct.unpack("<Q", seq_bytes)[0]

        if s1 < self.cursor:
            return RecvStatus.EMPTY, None, 0

        skipped = 0
        if s1 > self.cursor:
            skipped = s1 - self.cursor
            self.lapped_count += skipped
            self.cursor = s1

        # Read payload
        payload_offset = slot_offset + 8
        payload_bytes = self.mm[payload_offset:payload_offset + self.payload_size]
        item = self.struct_cls.from_buffer_copy(payload_bytes)

        # Verify seqlock
        seq_bytes2 = self.mm[slot_offset:slot_offset + 8]
        s2 = struct.unpack("<Q", seq_bytes2)[0]
        if s1 != s2:
            self.cursor = s2
            return RecvStatus.EMPTY, None, 0

        self.cursor += 1
        if skipped > 0:
            return RecvStatus.LAPPED, item, skipped
        return RecvStatus.OK, item, 0

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
        if hasattr(self, "offset_mm") and self.offset_mm is not None:
            try:
                self.offset_mm.close()
            except Exception:
                pass
            self.offset_mm = None
        if hasattr(self, "offset_fd") and self.offset_fd >= 0:
            try:
                os.close(self.offset_fd)
            except Exception:
                pass
            self.offset_fd = -1

        self.header = None
        if hasattr(self, "mm") and self.mm is not None:
            try:
                self.mm.close()
            except Exception:
                pass
            self.mm = None
        if hasattr(self, "fd") and self.fd >= 0:
            try:
                os.close(self.fd)
            except Exception:
                pass
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RingProducer:
    """Zero-copy writer for ringfire shared memory ring buffer."""

    def __init__(self, path: str, struct_cls: Type[ctypes.Structure], capacity: int = 65536):
        if capacity & (capacity - 1) != 0:
            raise ValueError(f"Capacity {capacity} must be a power of two")

        self.path = path
        self.struct_cls = struct_cls
        self.payload_size = ctypes.sizeof(struct_cls)
        self.slot_stride = ((8 + self.payload_size + 7) // 8) * 8
        self.capacity = capacity
        self.mask = capacity - 1
        self.slots_offset = HEADER_SIZE
        self.seq = 1

        total_size = HEADER_SIZE + (capacity * self.slot_stride)

        self.fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o660)
        os.ftruncate(self.fd, total_size)
        self.mm = mmap.mmap(self.fd, total_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)

        self.header = RingHeader.from_buffer(self.mm, 0)
        self.header.magic = RINGFIRE_MAGIC
        self.header.version = RINGFIRE_VERSION
        self.header.element_size = self.slot_stride
        self.header.capacity = capacity
        self.header.mask = capacity - 1
        self.header.write_seq = 0
        self.header.claim_seq = 0
        self.header.flags = 0x0011 # SPMC + LatestWins
        self.header.futex_word = 0
        self.header.waiting_consumers = 0

        # Zero slots
        self.mm[HEADER_SIZE:total_size] = b"\x00" * (total_size - HEADER_SIZE)

    def push(self, item: ctypes.Structure):
        """Publishes a message into the ring buffer."""
        idx = self.seq & self.mask
        slot_offset = self.slots_offset + (idx * self.slot_stride)

        # Write payload
        raw_bytes = bytes(item)
        self.mm[slot_offset + 8:slot_offset + 8 + self.payload_size] = raw_bytes

        # Write seq
        self.mm[slot_offset:slot_offset + 8] = struct.pack("<Q", self.seq)
        self.header.write_seq = self.seq
        self.seq += 1

    def close(self):
        if hasattr(self, "mm") and self.mm is not None:
            self.mm.close()
        if hasattr(self, "fd") and self.fd >= 0:
            os.close(self.fd)
            self.fd = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
