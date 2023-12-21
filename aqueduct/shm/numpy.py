from typing import Tuple

import numpy as np

from .base import SharedData, SharedMemoryWrapper


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
