"""Counts the time of transferring an object to the queue and reading it from it.

Compares different implementations: torch tensor, numpy array, with and without shared memory.
Run: `python -m tests.perf.queue_transfer`
"""
import multiprocessing as mp
import time
from multiprocessing import Queue

import logging
import torch

from aqueduct.shm import SharedFieldsMixin
from .utils import (
    ImViewType,
    MAGIC_NUMBER,
    MPType,
    StopProcess,
    get_image,
    get_mp_classes,
    timeit,
)


class BaseTask:
    def __init__(self, image):
        self.im = image


class Task(SharedFieldsMixin, BaseTask):
    pass


def worker(q_in: Queue):
    while True:
        try:
            start, task = q_in.get()
        except TypeError:
            break
        assert task.im[0][0][0] == MAGIC_NUMBER
        logging.info(f'queue transfer time: {(time.monotonic() - start) * 10**3:.4f}')
        # для правильного замера времени - чтобы gc мог удалить task и вернуть shared память в ОС сразу
        # после обработки задачи, а не после того, как получена новая задача из очереди: q_in.get()
        # todo обсудить момент, что unlink занимает столько же или больше времени, чем трансфер объекта
        # возможно это не так важно, т.к. это время несравнимо со временем работы с объектом
        task = None


def run(view_type: ImViewType, mp_type: MPType, share: bool = False):
    logging.info(f'{view_type.value} image, {mp_type.value} multiprocessing, share: {share}')
    q_class, p_class = get_mp_classes(mp_type)
    task_type = Task if view_type == ImViewType.NUMPY and share else BaseTask

    q_task = q_class()
    p = p_class(target=worker, args=(q_task,))
    p.start()

    for _ in range(10):
        im = get_image()

        if view_type == ImViewType.NUMPY:
            if share:
                with timeit() as t:
                    task = task_type(im)
                    task.share_value('im')
            else:
                with timeit() as t:
                    task = task_type(im)
        elif view_type == ImViewType.TT:
            if share:
                with timeit() as t:
                    tensor = torch.from_numpy(im)
                    np_array = tensor.numpy()
                # it takes less than 0.1 ms
                # logging.info(f'tensor converting time, ms:  {t.seconds * 10 ** 3:.4f}')
                with timeit() as t:
                    task = task_type(tensor)
                    task.im.share_memory_()
            else:
                with timeit() as t:
                    task = task_type(torch.from_numpy(im))
        logging.info(f'task creating time, ms:  {t.seconds * 10**3:.4f}')
        q_task.put((time.monotonic(), task))
        time.sleep(0.001)

    q_task.put(StopProcess())

    p.join()


def main():
    run(ImViewType.NUMPY, MPType.PYTHON)
    run(ImViewType.TT, MPType.TORCH)
    run(ImViewType.TT, MPType.TORCH, True)
    run(ImViewType.NUMPY, MPType.PYTHON, True)


if __name__ == '__main__':
    mp.set_start_method('fork')
    main()
