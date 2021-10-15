import asyncio
import logging
import multiprocessing as mp
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from multiprocessing import (
    Value,
    queues,
)
from typing import Tuple

import numpy as np
import pytest

from aqueduct.flow import Flow
from aqueduct.handler import BaseTaskHandler
from aqueduct.logger import LOGGER_NAME
from aqueduct.multiprocessing import ProcessContext
from aqueduct.shm import NPArraySharedData
from aqueduct.task import BaseTask

pytest_plugins = 'aiohttp.pytest_plugin'

# необходимо для корректной работы тестов на MacOS
mp.set_start_method('fork')


class SharedCounter:
    """ A synchronized shared counter.

    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.

    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/

    """

    def __init__(self, n: int = 0):
        self.count = Value('i', n)

    def increment(self, n: int = 1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value


class MultiPlatformQueue(queues.Queue):
    """ A portable implementation of multiprocessing.Queue.

    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.size = SharedCounter(0)

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)
        self.size.increment(1)

    def get(self, *args, **kwargs):
        elem = super().get(*args, **kwargs)
        self.size.increment(-1)
        return elem

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        return not self.qsize()


if sys.platform == 'darwin':
    queues.Queue = MultiPlatformQueue


async def terminate_worker(flow: Flow) -> Tuple[mp.process.BaseProcess, BaseTaskHandler]:
    handler, context = next(iter(flow._contexts.items()))  # type: BaseTaskHandler, ProcessContext
    process = context.processes[0]
    process.terminate()
    await asyncio.sleep(1)
    return process, handler


@pytest.fixture
def array() -> np.ndarray:
    return np.array([1, 2])


@pytest.fixture
def array_sh_data(array: np.ndarray) -> NPArraySharedData:
    return NPArraySharedData.create_from_data(array)


class Task(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None


@pytest.fixture
def task() -> Task:
    return Task()


class SleepHandler(BaseTaskHandler):
    def __init__(self, handler_sec: float):
        self._handler_sec: float = handler_sec

    def handle(self, *tasks: BaseTask):
        time.sleep(self._handler_sec)


class SleepHandler1(SleepHandler):
    pass


class SleepHandler2(SleepHandler):
    pass


class SetResultSleepHandler(SleepHandler):
    def handle(self, *tasks: Task):
        super().handle(*tasks)
        for task in tasks:
            task.result = 'test'


@pytest.fixture
def log_file():
    f = tempfile.NamedTemporaryFile()
    yield f
    f.close()


@pytest.fixture
def aqueduct_logger(log_file):
    logger = logging.getLogger(LOGGER_NAME)
    logger.addHandler(logging.FileHandler(log_file.name))


@pytest.fixture
def sleep_handlers() -> Tuple[SleepHandler, ...]:
    return (
        SleepHandler1(0.001),
        SleepHandler2(0.003),
        SetResultSleepHandler(0.002),
    )


@pytest.fixture
def slow_sleep_handlers() -> Tuple[SleepHandler, ...]:
    return (
        SleepHandler1(0.1),
        SleepHandler2(0.3),
        SetResultSleepHandler(0.2),
    )


@asynccontextmanager
async def run_flow(flow: Flow):
    flow.start()
    yield flow
    await flow.stop(graceful=False)


@pytest.fixture
async def simple_flow(loop, sleep_handlers: Tuple[SleepHandler, ...]) -> Flow:
    async with run_flow(Flow(*sleep_handlers)) as flow:
        yield flow


@pytest.fixture
async def slow_simple_flow(loop, slow_sleep_handlers: Tuple[SleepHandler, ...]) -> Flow:
    async with run_flow(Flow(*slow_sleep_handlers)) as flow:
        yield flow
