import multiprocessing
import time
from threading import Thread
from typing import List, Tuple

import pytest

from aqueduct import BaseTask, BaseTaskHandler
from aqueduct.worker import Worker


class FakeTaskHandler(BaseTaskHandler):
    def handle(self, *tasks: BaseTask):
        pass


class Task(BaseTask):
    def __init__(self, data):
        super().__init__()
        self.metrics.start_transfer_timer('test')
        self.data = data
        self.set_timeout(5000.)

    def __eq__(self, other: 'Task'):
        return self.data == other.data


class Batch:
    def __init__(self, *data: int):
        self.tasks = [Task(d) for d in data]

    def __eq__(self, other: List[Task]):
        return self.tasks == other


def long_elements_producer(iq: multiprocessing.Queue):
    """Long non-blocking producer of elements."""
    tic_elements = {1: 1, 8: 2, 10: 3, 24: 4}

    for tic in range(25):
        if tic in tic_elements:
            iq.put(Task(tic_elements[tic]))

        time.sleep(0.01)


@pytest.fixture
def worker():
    def init(batch_size, batch_timeout) -> Tuple[Worker, multiprocessing.Queue]:
        iq = multiprocessing.Queue(10000)
        oq = multiprocessing.Queue(10000)
        worker = Worker(
            queue_in=iq,
            queue_out=oq,
            task_handler=FakeTaskHandler(),
            handle_condition=lambda _: True,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            batch_lock=None,
            step_number=1)
        return worker, iq

    yield init


class TestWorker:
    @pytest.mark.parametrize('elements, batch_size, result', [
        (Batch(1, 2, 3), 1, [Batch(1), Batch(2), Batch(3)]),
        (Batch(1, 2, 3), 2, [Batch(1, 2), Batch(3)]),
        (Batch(1, 2, 3), 3, [Batch(1, 2, 3)]),
        (Batch(1, 2, 3), 4, [Batch(1, 2, 3)]),
    ])
    def test_iter_batches(self, worker, elements, batch_size, result):
        w, iq = worker(batch_size, 1.0)
        for item in elements.tasks:
            iq.put(item)

        it = w._iter_batches()
        for res_batch in result:
            assert next(it) == res_batch

    def test_iter_batches_read_all_queue(self, worker):
        """Tests that batcher read all available tasks from queue all the time.

        Problem is queue.get() can raise Empty exception even thou queue is in fact not empty.
         For example, if we have 10 tasks in a queue, we expect batch size to be 10, but it is not always true, because
         after reading, say, 5 tasks, Python queue can tell us that there is nothing left, which is in fact false.
        In our implementation we use an additional spin over a queue to guarantee consistent results.
        More info: https://bugs.python.org/issue23582"""
        batch_size = 100
        elements = Batch(*range(0, 5000))
        expected = [Batch(*range(i, i + batch_size)) for i in range(0, 5000, batch_size)]

        w, iq = worker(100, 0.)

        for item in elements.tasks:
            iq.put(item)

        it = w._iter_batches()
        for res_batch in expected:
            assert next(it) == res_batch

    @pytest.mark.parametrize('timeout, result', [
        (0.01, [Batch(1), Batch(2), Batch(3), Batch(4)]),
        (0.03, [Batch(1), Batch(2, 3), Batch(4)]),
    ])
    def test_iter_batches_timeouts(self, worker, timeout, result):
        w, iq = worker(batch_size=4, batch_timeout=timeout)

        th = Thread(target=long_elements_producer, args=(iq,))
        th.start()

        it = w._iter_batches()
        for res_batch in result:
            assert next(it) == res_batch

        th.join()
