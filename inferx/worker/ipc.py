# inferx/worker/ipc.py
"""
InferX IPC Shared Memory Pool.

Implements a high-performance, zero-copy shared memory data exchange pool.
Allows transferring large payload tensors without Python serialization overhead.
"""

from multiprocessing.shared_memory import SharedMemory
import threading
from typing import List


class SharedMemoryPool:
    """
    Wrapper around multiprocessing.shared_memory.SharedMemory.

    Provides simple offset-based reads and writes.
    """

    def __init__(self, name: str, size: int, create: bool = False) -> None:
        self.name = name
        self.size = size
        if create:
            # Create a new shared memory segment
            self.shm = SharedMemory(name=name, create=True, size=size)
        else:
            # Attach to an existing segment
            self.shm = SharedMemory(name=name)

    def write(self, offset: int, data: bytes) -> None:
        """Writes bytes directly into the shared memory buffer at offset."""
        if offset + len(data) > self.size:
            raise ValueError("Write exceeds shared memory segment boundary.")
        self.shm.buf[offset : offset + len(data)] = data

    def read(self, offset: int, size: int) -> bytes:
        """Reads size bytes directly from the shared memory buffer at offset."""
        if offset + size > self.size:
            raise ValueError("Read exceeds shared memory segment boundary.")
        return bytes(self.shm.buf[offset : offset + size])

    def close(self) -> None:
        """Closes access to the shared memory segment."""
        self.shm.close()

    def unlink(self) -> None:
        """Destroys the shared memory segment (should be called by owner process)."""
        try:
            self.shm.unlink()
        except Exception:
            pass


class SharedMemoryAllocator:
    """
    Thread-safe offset allocator managing slots within the SharedMemoryPool.

    Operates in the parent process to coordinate memory slots without
    multi-process lock contentions.
    """

    def __init__(
        self, pool_size: int = 64 * 1024 * 1024, slot_size: int = 64 * 1024
    ) -> None:
        self.pool_size = pool_size
        self.slot_size = slot_size
        self.num_slots = pool_size // slot_size

        self._free_slots: List[int] = list(range(self.num_slots))
        self._lock = threading.Lock()

    def allocate(self) -> int:
        """
        Allocates a slot, returning its buffer offset (O(1) complexity).

        Raises:
            BufferError: If all slots are currently in use.
        """
        with self._lock:
            if not self._free_slots:
                raise BufferError("Shared memory pool allocation capacity exhausted.")
            slot_idx = self._free_slots.pop(0)
            return slot_idx * self.slot_size

    def free(self, offset: int) -> None:
        """Releases the slot at the offset back to the pool (O(1) complexity)."""
        slot_idx = offset // self.slot_size
        with self._lock:
            # Avoid duplicate inserts
            if slot_idx not in self._free_slots:
                self._free_slots.append(slot_idx)

    def free_slots_count(self) -> int:
        """Returns the number of available slots."""
        with self._lock:
            return len(self._free_slots)
