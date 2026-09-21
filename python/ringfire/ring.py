import os
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

class RingConsumer:
    """Zero-copy reader for ringfire shared memory ring buffer."""

    def __init__(self, path: str, struct_cls: Type[ctypes.Structure]):
        self.path = path
        self.struct_cls = struct_cls
        self.payload_size = ctypes.sizeof(struct_cls)

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

        # Start cursor
        write_seq = self.header.write_seq
        if write_seq > self.capacity:
            self.cursor = write_seq - self.capacity + 1
        else:
            self.cursor = 1

        self.lapped_count = 0

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
        if hasattr(self, "mm") and self.mm is not None:
            self.mm.close()
        if hasattr(self, "fd") and self.fd >= 0:
            os.close(self.fd)
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
