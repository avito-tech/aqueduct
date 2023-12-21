from typing import Type, Union

from .base import SharedData, SharedMemoryWrapper


class BytesSharedData(SharedData):
    def __init__(self, shm_wrapper: SharedMemoryWrapper, size: int, cls_byte: Type[Union[bytes, bytearray]]):
        self._size = size
        self._cls_byte = cls_byte
        super().__init__(shm_wrapper)

    def get_data(self) -> bytes:
        if self.shm_wrapper.buf is None:
            raise ValueError('No shared memory buffer')
        return self._cls_byte(self.shm_wrapper.buf[0:self._size])

    @classmethod
    def create_from_data(cls, data: Union[bytes, bytearray]) -> 'BytesSharedData':
        nbytes = len(data)
        shm_wrapper = SharedMemoryWrapper(nbytes)
        shared_data = cls(shm_wrapper, nbytes, type(data))
        shm_wrapper.buf[0:nbytes] = data
        return shared_data
