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
import multiprocessing as mp
import warnings
from abc import ABC, abstractmethod
try:
    from multiprocessing import shared_memory
except ImportError as e:
    raise ImportError('shared_memory module is available since python3.8')
from typing import Any, Dict, Tuple

import numpy as np

from .exceptions import BadReferenceCount


class SharedMemoryWrapper:
    """Управляет разделяемой памятью.

    Предоставляет память нужного размера, следит за количеством ссылок на память, при отсутствии
    ссылок на память возвращает память ОС.
    """
    def __init__(self, size: int, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        # поле _size нужно, чтобы найти в разделяемой памяти ref_counter
        self._size = size
        self._shm: shared_memory.SharedMemory = None  # noqa
        self._ref_counter: np.ndarray = None  # noqa

        if shm is None and shm_name is None:
            # 4 байта на счетчик ссылок (int32)
            shm = shared_memory.SharedMemory(create=True, size=self._size + 4)
        self._attach_shm(shm, shm_name)

        # поле _shm_name нужно для десериализации
        self._shm_name = self._shm.name

    @property
    def shm(self) -> shared_memory.SharedMemory:
        return self._shm

    @property
    def ref_count(self) -> int:
        return self._ref_counter[0]

    def _attach_shm(self, shm: shared_memory.SharedMemory = None, shm_name: str = None):
        if shm is None:
            shm = shared_memory.SharedMemory(shm_name)
        self._shm = shm
        self._ref_counter = self._get_ref_counter()
        # нужно только для процесса, выделившего разделяемую память
        if self.ref_count == 0:
            self._update_ref_counter()

    def _update_ref_counter(self, incr: bool = True):
        """Обновляет количество ссылок на используемую область разделяемой памяти."""
        if incr:
            self._ref_counter[0] += 1
        else:
            if self._ref_counter[0] == 0:
                raise BadReferenceCount('Reference count should not be less than 0')
            self._ref_counter[0] -= 1

    def _get_ref_counter(self) -> np.ndarray:
        # todo попробовать сделать ссылку на целое число, а не np.array
        return np.frombuffer(self._shm.buf, dtype=np.int32, offset=self._size, count=1)

    def _release_shm(self):
        # if shm is already closed (for example manually). Unexpected behavior
        if self._shm.buf is None:
            # todo log as warning? ref_counter would be wrong -> memory leak
            return

        self._update_ref_counter(False)
        ref_count = self._ref_counter[0]
        self._shm.close()
        if ref_count == 0:
            self._shm.unlink()

    def __getstate__(self) -> dict:
        # счетчик ссылок увеличивается здесь, т.к. если объект пересылается долго (долго находится в очереди),
        # gc удаляет объект и память возвращается ОС до того, как объект будет вычитан из очереди
        # из-за этого при вычитывании объекта и попытке присоединить разделяемую память возникает ошибка
        self._update_ref_counter()
        state = {k: v for k, v in self.__dict__.items() if k not in ('_shm', '_ref_counter')}
        return state

    def __setstate__(self, state: dict):
        self.__dict__.update(state)
        self._attach_shm(shm_name=self._shm_name)

    def __del__(self):
        self._release_shm()


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
