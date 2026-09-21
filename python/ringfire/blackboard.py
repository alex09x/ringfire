import os
import mmap
import ctypes
import struct
from typing import Optional, Type

BLACKBOARD_MAGIC = 0x52494E4742424F52
BLACKBOARD_VERSION = 1
HEADER_SIZE = 128

class BlackboardHeader(ctypes.Structure):
    _fields_ = [
        ("magic", ctypes.c_uint64),
        ("version", ctypes.c_uint32),
        ("value_size", ctypes.c_uint32),
        ("slot_size", ctypes.c_uint32),
        ("slot_count", ctypes.c_uint32),
        ("_reserved", ctypes.c_uint64 * 2),
        ("_pad", ctypes.c_uint8 * 88),
    ]

class BlackboardConsumer:
    """Reader for ringfire shared memory Blackboard state table."""

    def __init__(self, path: str, struct_cls: Type[ctypes.Structure]):
        self.path = path
        self.struct_cls = struct_cls
        self.value_size = ctypes.sizeof(struct_cls)

        self.fd = os.open(path, os.O_RDWR)
        file_size = os.fstat(self.fd).st_size
        self.mm = mmap.mmap(self.fd, file_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)

        self.header = BlackboardHeader.from_buffer(self.mm, 0)
        if self.header.magic != BLACKBOARD_MAGIC:
            raise ValueError(f"Invalid magic: expected 0x{BLACKBOARD_MAGIC:016X}, got 0x{self.header.magic:016X}")
        if self.header.version != BLACKBOARD_VERSION:
            raise ValueError(f"Version mismatch: {self.header.version}")
        if self.header.value_size != self.value_size:
            raise ValueError(f"Value size mismatch: expected {self.header.value_size}, got {self.value_size}")

        self.slot_size = self.header.slot_size
        self.slot_count = self.header.slot_count
        self.slots_offset = HEADER_SIZE

    def read(self, key: int) -> Optional[ctypes.Structure]:
        """Reads the snapshot for key using seqlock consistency validation."""
        if key < 0 or key >= self.slot_count:
            raise IndexError(f"Key {key} out of range (count: {self.slot_count})")

        slot_offset = self.slots_offset + (key * self.slot_size)

        for _ in range(100):
            seq_bytes = self.mm[slot_offset:slot_offset + 8]
            s1 = struct.unpack("<Q", seq_bytes)[0]
            if s1 == 0:
                return None
            if s1 & 1 != 0:
                continue # Writer in progress

            val_bytes = self.mm[slot_offset + 8:slot_offset + 8 + self.value_size]
            val = self.struct_cls.from_buffer_copy(val_bytes)

            seq_bytes2 = self.mm[slot_offset:slot_offset + 8]
            s2 = struct.unpack("<Q", seq_bytes2)[0]
            if s1 == s2:
                return val

        return None

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


class BlackboardProducer:
    """Writer for ringfire shared memory Blackboard state table."""

    def __init__(self, path: str, struct_cls: Type[ctypes.Structure], slot_count: int = 1024):
        self.path = path
        self.struct_cls = struct_cls
        self.value_size = ctypes.sizeof(struct_cls)
        raw_slot = 8 + self.value_size
        self.slot_size = ((raw_slot + 63) // 64) * 64
        self.slot_count = slot_count
        self.slots_offset = HEADER_SIZE

        total_size = HEADER_SIZE + (slot_count * self.slot_size)
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o660)
        os.ftruncate(self.fd, total_size)
        self.mm = mmap.mmap(self.fd, total_size, mmap.MAP_SHARED, mmap.PROT_READ | mmap.PROT_WRITE)

        self.header = BlackboardHeader.from_buffer(self.mm, 0)
        self.header.magic = BLACKBOARD_MAGIC
        self.header.version = BLACKBOARD_VERSION
        self.header.value_size = self.value_size
        self.header.slot_size = self.slot_size
        self.header.slot_count = slot_count

        self.mm[HEADER_SIZE:total_size] = b"\x00" * (total_size - HEADER_SIZE)

    def write(self, key: int, item: ctypes.Structure):
        """Writes or updates key using atomic seqlock."""
        if key < 0 or key >= self.slot_count:
            raise IndexError(f"Key {key} out of range (count: {self.slot_count})")

        slot_offset = self.slots_offset + (key * self.slot_size)
        seq_bytes = self.mm[slot_offset:slot_offset + 8]
        s = struct.unpack("<Q", seq_bytes)[0]
        write_s = s + 1 if s % 2 == 0 else s + 2

        # Step 1: odd seq
        self.mm[slot_offset:slot_offset + 8] = struct.pack("<Q", write_s)

        # Step 2: payload
        self.mm[slot_offset + 8:slot_offset + 8 + self.value_size] = bytes(item)

        # Step 3: even seq
        self.mm[slot_offset:slot_offset + 8] = struct.pack("<Q", write_s + 1)

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
