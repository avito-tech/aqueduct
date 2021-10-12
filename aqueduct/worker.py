import multiprocessing as mp
import queue
import time
from typing import Callable, Iterable, Iterator, List, Optional

from .handler import BaseTaskHandler
from .logger import log
from .metrics.timer import timeit
from .task import BaseTask, StopTask


def batches(
        elements: Iterable,
        batch_size: int,
        timeout: float,
) -> Iterator[List]:
    batch = []
    timeout_end = time.monotonic() + timeout

    for elem in elements:
        if elem:
            batch.append(elem)
        if time.monotonic() >= timeout_end or len(batch) == batch_size:
            if batch:
                yield batch
                batch = []
            timeout_end = time.monotonic() + timeout

    if batch:
        yield batch


def batches_with_lock(batches_gen: Iterator[List], lock: mp.Lock) -> Iterator[List]:
    while True:
        lock.acquire()
        try:
            batch = next(batches_gen)
        except StopIteration:
            return
        finally:
            lock.release()
        yield batch


class Worker:
    """Обертка над классом BaseTaskHandler.

    Достает из входной очереди задачу, пропускает ее через обработчика task_handler и кладет ее
    в выходную очередь.
    """

    def __init__(
            self,
            queue_in: mp.Queue,
            queue_out: mp.Queue,
            task_handler: BaseTaskHandler,
            handle_condition: Callable[[BaseTask], bool],
            batch_size: int,
            batch_timeout: float,
            batch_lock: Optional[mp.RLock],
            step_number: int,
    ):
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.task_handler = task_handler
        self.handle_condition = handle_condition
        self.name = task_handler.__class__.__name__
        self.step_name = self.task_handler.get_step_name(step_number)
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._batch_lock = batch_lock
        self._stop_task: BaseTask = None  # noqa

    def _start(self):
        """Runs something huge (e.g. model) in child process."""
        self.task_handler.on_start()

    def _tasks(self) -> Iterator[Optional[BaseTask]]:
        """Provides suitable for processing tasks."""
        while True:
            try:
                task: BaseTask = self.queue_in.get(block=False)
            except queue.Empty:
                # returns control
                yield
                time.sleep(0.001)
                continue
            if isinstance(task, StopTask):
                self._stop_task = task
                break
            log.debug(f'[{self.name}] Have message')
            task.metrics.stop_transfer_timer(self.step_name)
            # dont't pass an expired task to the next steps
            if task.is_expired():
                log.debug(f'[{self.name}] Task expired. Skip: {task}')
                continue
            # don't process unsuitable tasks
            if not self.handle_condition(task):
                self._post_handle(task)
                continue
            yield task

    def _tasks_batches(self) -> Iterator[Optional[List[BaseTask]]]:
        if self._batch_size == 1:
            # pseudo batching
            for task in self._tasks():
                if task:
                    yield [task]
        else:
            tasks_batches: Iterator[List[BaseTask]] = batches(
                self._tasks(),
                batch_size=self._batch_size,
                timeout=self._batch_timeout,
            )
            if self._batch_lock:
                # to take a queue_in lock for the duration of batch filling time
                tasks_batches = batches_with_lock(tasks_batches, self._batch_lock)

            while True:
                try:
                    with timeit() as timer:
                        tasks_batch = next(tasks_batches)
                    tasks_batch[0].metrics.batch_times.add(self.step_name, timer.seconds)
                    tasks_batch[0].metrics.batch_sizes.add(self.step_name, len(tasks_batch))
                except StopIteration:
                    return
                yield tasks_batch

    def _post_handle(self, task: BaseTask):
        task.metrics.start_transfer_timer(self.step_name)
        self.queue_out.put(task)

    def loop(self, pid: int, start_barrier: mp.Barrier):
        """Main worker loop.

        The code below is executed in a new process.
        """
        log.info(f'[Worker] initialising handler {self.name}')
        self._start()
        log.info(f'[Worker] handler {self.name} ok, waiting for others to start')
        start_barrier.wait()

        log.info(f'[Worker] handler {self.name} ok, starting loop')

        for tasks_batch in self._tasks_batches():
            with timeit() as timer:
                self.task_handler.handle(*tasks_batch)

            for task in tasks_batch:
                task.metrics.handle_times.add(self.step_name, timer.seconds)
                self._post_handle(task)

        if self._stop_task:
            self.queue_out.put(self._stop_task)
