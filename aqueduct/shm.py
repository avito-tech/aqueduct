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
import contextlib
import multiprocessing as mp
import warnings
from abc import ABC, abstractmethod

try:
    from multiprocessing import shared_memory
except ImportError as e:
    raise ImportError('shared_memory module is available since python3.8')
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

import numpy as np


class _ShmRefCounter:
    """Calculates the number of references to shared memory."""
    def __init__(self):
        self._counter = mp.Manager().dict()
        self._lock = mp.RLock()

    def get_count(self, shm_name: str) -> Optional[int]:
        return self._counter.get(shm_name)

    def on_create(self, shm_name: str):
        self._lock.acquire()
        if shm_name in self._counter:
            self._counter[shm_name] += 1
        else:
            self._counter[shm_name] = 1
        self._lock.release()

    def on_delete(self, shm_name: str):
        self._lock.acquire()
        self._counter[shm_name] -= 1
        if self.get_count(shm_name) == 0:
            self._counter.pop(shm_name)
        self._lock.release()

    def on_send(self, shm_name: str):
        self._lock.acquire()
        self._counter[shm_name] += 1
        self._lock.release()

    @contextlib.contextmanager
    def on_receive(self, shm_name: str):
        self._lock.acquire()
        yield
        self._counter[shm_name] -= 1
        self._lock.release()


_shm_ref_counter = _ShmRefCounter()


class SharedMemoryWrapper:
    """Manages the shared memory lifecycle.

    Allocates shared memory, manages reference counts and returns shared memory to OS.
    """
    def __init__(self, size: int, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        self._shm: shared_memory.SharedMemory = None  # noqa

        if shm is None and shm_name is None:
            shm = shared_memory.SharedMemory(create=True, size=size)
        self._attach_shm(shm, shm_name)

        # it's necessary for deserialization
        self._shm_name = self._shm.name

    @property
    def shm(self) -> shared_memory.SharedMemory:
        return self._shm

    @property
    def ref_count(self) -> int:
        return _shm_ref_counter.get_count(self._shm_name)

    def _attach_shm(self, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        if shm is None:
            shm = shared_memory.SharedMemory(shm_name)
        self._shm = shm
        self._shm_name = shm.name
        _shm_ref_counter.on_create(self._shm_name)

    def __getstate__(self) -> dict:
        _shm_ref_counter.on_send(self._shm_name)
        state = {k: v for k, v in self.__dict__.items() if k != '_shm'}
        return state

    def __setstate__(self, state: dict):
        self.__dict__.update(state)
        with _shm_ref_counter.on_receive(self._shm_name):
            self._attach_shm(shm_name=self._shm_name)

    def __del__(self):
        _shm_ref_counter.on_delete(self._shm_name)
        if _shm_ref_counter.get_count(self._shm_name) == 0:
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
        if self.shm_wrapper.shm.buf is None:
            raise ValueError('No shared memory buffer')
        return np.ndarray(self._shape, dtype=self._dtype, buffer=self.shm_wrapper.shm.buf)

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
