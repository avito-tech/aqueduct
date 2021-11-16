"""Предоставляет инструменты для управления данными в разделяемой памяти.

В shared_memory есть особенность - даже после close() в другом процессе, где была прилинкована память,
при выходе из процесса вызывается unlink(). Как следствие - этот кусок памяти не может быть больше
нигде использован, т.к. возвращен ОС. Причина - в resource_tracker'е, который для каждого не дочернего
процесса - отдельный.

Обойти эту особенность можно, явно исключив из наблюдения трекера, кусок памяти:
from multiprocessing.resource_tracker import unregister
unregister(f'/{shm.name}', 'shared_memory')

Также этой проблемы нет, если процесс, использующий память, дочерний (т.к. там тот же самый трекер ресурсов)
https://bugs.python.org/issue39959
"""

import warnings
from abc import (
    ABC,
    abstractmethod,
)

from cffi import FFI

try:
    from multiprocessing import shared_memory
except ImportError as e:
    raise ImportError('shared_memory module is available since python3.8')
from typing import (
    Any,
    Dict,
    Tuple,
)

import numpy as np

ffi = FFI()

ffi.cdef("""
uint32_t load_uint32(uint32_t *v);
void store_uint32(uint32_t *v, uint32_t n);
uint32_t add_and_fetch_uint32(uint32_t *v, uint32_t i);
uint32_t sub_and_fetch_uint32(uint32_t *v, uint32_t i);
""")

atomic = ffi.verify("""
uint32_t load_uint32(uint32_t *v) {
    return __atomic_load_n(v, __ATOMIC_SEQ_CST);
};
void store_uint32(uint32_t *v, uint32_t n) {
    uint32_t i = n;
    __atomic_store(v, &i, __ATOMIC_SEQ_CST);
};
uint32_t add_and_fetch_uint32(uint32_t *v, uint32_t i) {
    return __atomic_add_fetch(v, i, __ATOMIC_SEQ_CST);
};
uint32_t sub_and_fetch_uint32(uint32_t *v, uint32_t i) {
    return __atomic_sub_fetch(v, i, __ATOMIC_SEQ_CST);
};
""")


class AtomicCounter:
    def __init__(self, view: memoryview):
        self._ptr = ffi.cast('uint32_t*', ffi.from_buffer(view[:self.size()]))

    def get(self):
        return atomic.load_uint32(self._ptr)

    def set(self, n):
        return atomic.store_uint32(self._ptr, n)

    def inc(self):
        return atomic.add_and_fetch_uint32(self._ptr, 1)

    def dec(self):
        return atomic.sub_and_fetch_uint32(self._ptr, 1)

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
    """Управляет данными в разделяемой памяти."""

    def __init__(self, shm_wrapper: SharedMemoryWrapper):
        self.shm_wrapper = shm_wrapper

    @abstractmethod
    def get_data(self) -> Any:
        """Возвращает ссылку на данные, лежащие в разделяемой памяти."""

    @classmethod
    @abstractmethod
    def create_from_data(cls, data: Any) -> 'SharedData':
        """Копирует данные в разделяемую память и возвращает экземпляр класса."""


class NPArraySharedData(SharedData):
    def __init__(self, shm_wrapper: SharedMemoryWrapper, shape: Tuple[int, ...], dtype: np.dtype):
        self._shape = shape
        self._dtype = dtype
        super().__init__(shm_wrapper)

    def get_data(self) -> np.ndarray:
        if self.shm_wrapper.buf is None:
            raise ValueError('No shared memory buffer')
        return np.ndarray(self._shape, dtype=self._dtype, buffer=self.shm_wrapper.buf)

    @classmethod
    def create_from_data(cls, data: np.ndarray) -> 'NPArraySharedData':
        shm_wrapper = SharedMemoryWrapper(data.nbytes)
        shared_data = cls(shm_wrapper, data.shape, data.dtype)
        array = shared_data.get_data()
        array[:] = data[:]
        return shared_data


class SharedFieldsMixin:
    """Позволяет заменить значение поля экземпляра класса на его копию в разделяемой памяти."""

    def __init__(self, *args, **kwargs):
        self._shared_fields: Dict[str, SharedData] = {}
        super().__init__(*args, **kwargs)

    def share_value(self, field_name: str):
        """Заменяет значение поля на его копию в разделяемой памяти.

        Все последующие изменения значения будут производиться в разделяемой памяти, т.е. эти изменения
        будут отражаться во всех объектах, которые ссылаются на эту же область памяти.
        В случае замены значения необходимо заново вызвать данный метод, чтобы значение попало в
        разделяемую память, при этом будет выделена новая область разделяемой памяти.
        """
        if field_name not in self.__dict__:
            raise ValueError('Unknown field name')
        if field_name in self._shared_fields:
            raise ValueError(f'The {field_name} field value is already shared')

        value = getattr(self, field_name)
        shared_value = self._get_shared_value(value)
        self._set_shared_value(field_name, shared_value)

    @staticmethod
    def _get_shared_value(value: Any) -> SharedData:
        if isinstance(value, np.ndarray):
            return NPArraySharedData.create_from_data(value)
        raise ValueError(f'Type {type(value)} cannot be shared')

    def _set_shared_value(self, field_name: str, shared_value: SharedData):
        self.__dict__[field_name] = shared_value.get_data()
        self._shared_fields[field_name] = shared_value

    def __setattr__(self, key, value):
        if isinstance(value, SharedData):
            self._set_shared_value(key, value)
        else:
            super().__setattr__(key, value)
            if key in self._shared_fields:
                self._shared_fields.pop(key)
                warnings.warn(f'The {key} field value is not shared anymore')

    def __getstate__(self) -> dict:
        state = {k: v for k, v in self.__dict__.items() if k not in self._shared_fields}
        return state

    def __setstate__(self, state: dict):
        self.__dict__.update(state)
        for f_name, shared_value in self._shared_fields.items():
            self.__dict__[f_name] = shared_value.get_data()
