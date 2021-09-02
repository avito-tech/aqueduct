import time
from enum import Enum
from multiprocessing import Process, Queue
from typing import Any, Tuple

import numpy as np
import torch
from torch.multiprocessing import Process as t_Process, Queue as t_Queue

MAGIC_NUMBER = 42


class ImViewType(Enum):
    """Тип представления картинки."""
    NUMPY = 'numpy'
    TT = 'torch tensor'


class MPType(Enum):
    """Реализация мультипроцессинга."""
    PYTHON = 'python'
    TORCH = 'torch'


def get_mp_classes(mp_type: MPType) -> Tuple[type, type]:
    if mp_type == MPType.PYTHON:
        return Queue, Process
    if mp_type == MPType.TORCH:
        return t_Queue, t_Process


def get_image(im_size: Tuple[int, int] = (1280, 960)) -> np.ndarray:
    im = np.random.randint(0, 255, (*im_size, 3)).astype(np.float32)
    im[0][0][0] = MAGIC_NUMBER
    return im


def get_image_view(im: np.ndarray, view_type: ImViewType) -> Any:
    if view_type == ImViewType.NUMPY:
        return im
    if view_type == ImViewType.TT:
        return torch.from_numpy(im)


class StopProcess:
    pass


class timeit:  # noqa
    def __enter__(self):
        self.seconds = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.seconds = time.perf_counter() - self.seconds
