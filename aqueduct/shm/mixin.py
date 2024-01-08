from typing import (
    Any,
    Dict,
    Protocol
)
import warnings

from .base import SharedData, SharedMemoryWrapper
from .bytes import BytesSharedData



class HasReadany(Protocol):
    """We need only single method from aiohttp.streams.StreamReader"""
    async def readany(self) -> bytes:
        pass


class SharedFieldsMixin:
    """Allows you to replace the value of a class instance field with its copy in shared memory."""

    def __init__(self, *args, **kwargs):
        self._shared_fields: Dict[str, SharedData] = {}
        super().__init__(*args, **kwargs)
    
    async def share_value_with_data(self, field_name: str, content: HasReadany, size: int):
        """Creates a shared memory in field_name attribute of type bytes of size
        and loads data there from a source that has async method readany
        """

        shm_wrapper = SharedMemoryWrapper(size)
        shared_value = BytesSharedData(shm_wrapper, size, bytes)

        offset = 0
        while True:
            block = await content.readany()
            if not block:
                break
            shm_wrapper.buf[offset: offset+len(block)] = block
            offset += len(block)
        
        self._set_shared_value(field_name, shared_value)

    def share_value(self, field_name: str):
        """Replaces the value of the field with its copy in shared memory.
           All subsequent changes to the value will be made in shared memory, i.e. these changes
           they will be reflected in all objects that refer to the same memory area.
           If the value is replaced, this method must be called again so that the value gets into
           shared memory, and a new area of shared memory will be allocated.
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
        try:
            import numpy as np
        except ImportError:
            if isinstance(value, (bytes, bytearray)):
                return BytesSharedData.create_from_data(value)
        else:
            from .numpy import NPArraySharedData
            if isinstance(value, np.ndarray):
                return NPArraySharedData.create_from_data(value)
            if isinstance(value, (bytes, bytearray)):
                return BytesSharedData.create_from_data(value)

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
