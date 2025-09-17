"""Provides tools for managing data in shared memory.

There is a feature in shared_memory - even after close() in another process
where memory was linked, unlink() is called when exiting the process.
As a result, this piece of memory cannot be used anywhere else, because it was returned to the OS.
The reason is in the resource_tracker, which is separate for each non-child process.

You can bypass this feature by explicitly excluding a piece of memory from the tracker's observation:
    from multiprocessing.resource_tracker import unregister
    unregister(f'/{shm.name}', 'shared_memory')

Also, this problem does not exist if the process using memory is a child (because there is the same resource tracker)
https://bugs.python.org/issue39959
"""

from abc import (
    ABC,
    abstractmethod,
)

try:
    from multiprocessing import shared_memory
except ImportError:
    raise ImportError('shared_memory module is available since python3.8')
from typing import Any

from ._atomic import ffi
from ._atomic.lib import load_uint32, store_uint32, add_and_fetch_uint32, sub_and_fetch_uint32


class AtomicCounter:
    def __init__(self, view: memoryview):
        self._ptr = ffi.cast('uint32_t*', ffi.from_buffer(view[:self.size()]))

    def get(self):
        return load_uint32(self._ptr)

    def set(self, n):
        return store_uint32(self._ptr, n)

    def inc(self):
        return add_and_fetch_uint32(self._ptr, 1)

    def dec(self):
        return sub_and_fetch_uint32(self._ptr, 1)

    @staticmethod
    def size():
        return ffi.sizeof('uint32_t')


class SharedMemoryWrapper:
    """Manages the shared memory lifecycle.

    Allocates shared memory, manages reference counts and returns shared memory to OS.
    """

    def __init__(self, size: int, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        self._shm: shared_memory.SharedMemory = None  # noqa

        if shm is None and shm_name is None:
            rc_size = AtomicCounter.size()
            shm = shared_memory.SharedMemory(create=True, size=size + rc_size)
            self._rc = AtomicCounter(shm.buf)
            self._rc.set(1)

        self._attach_shm(shm, shm_name)

        # it's necessary for deserialization
        self._shm_name = self._shm.name

    @property
    def buf(self) -> memoryview:
        return self._shm.buf[AtomicCounter.size():]

    @property
    def ref_count(self) -> int:
        return self._rc.get()

    def _attach_shm(self, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        if shm is None:
            shm = shared_memory.SharedMemory(shm_name)
        self._shm = shm
        self._shm_name = shm.name

    def __getstate__(self) -> dict:
        self._rc.inc()
        state = {k: v for k, v in self.__dict__.items() if k not in ('_shm', '_rc')}
        return state

    def __setstate__(self, state: dict):
        self.__dict__.update(state)
        self._attach_shm(shm_name=self._shm_name)
        self._rc = AtomicCounter(self._shm.buf)

    def __del__(self):
        curr_rc = self._rc.dec()
        if curr_rc == 0:
            self._shm.unlink()


class SharedData(ABC):
    """Manages data in shared memory."""

    def __init__(self, shm_wrapper: SharedMemoryWrapper):
        self.shm_wrapper = shm_wrapper

    @abstractmethod
    def get_data(self) -> Any:
        """Returns a ref to the data in shared memory."""

    @classmethod
    @abstractmethod
    def create_from_data(cls, data: Any) -> 'SharedData':
        """Copies the data to shared memory and returns an instance of the class."""
