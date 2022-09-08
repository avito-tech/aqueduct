import multiprocessing as mp
import queue
import sys
from typing import Callable, Iterator, List, Optional

from .handler import BaseTaskHandler
from .logger import log
from .metrics.timer import (
    Timer,
    timeit,
)
from .task import BaseTask, StopTask

MAX_SPIN_ITERATIONS=1000


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

    def _wait_task(self, timeout: float) -> Optional[BaseTask]:
        task = None
        # There is a problem with Python MP Queue implementation.
        # queue.get() can raise Empty exception even thou queue is in fact not empty.
        # For example, if we have 10 tasks in a queue, we expect batch size to be 10, but it is not always true, because
        #  after reading, say, 5 tasks, Python queue can tell as that there is nothing left, which is in fact false.
        # In our implementation we use an additional spin over a queue to guarantee consistent results.
        # More info: https://bugs.python.org/issue23582
        i = 0
        while not task and i < MAX_SPIN_ITERATIONS:
            # Limit our spin with maximum of MAX_SPIN_ITERATIONS just in case.
            # We do not want long, purposeless iteration
            i += 1
            try:
                if timeout == 0:
                    task = self.queue_in.get(block=False)
                else:
                    task = self.queue_in.get(timeout=timeout)
            except queue.Empty:
                # additionaly check queue size to make sure that there is in fact no tasks there.
                # if size > 0 then Empty exception was fake -> we should try to call get() again
                # since macos does not support qsize method - ignore that check
                # (it would be greate to make a proper queue later)
                if sys.platform == 'darwin' or self.queue_in.qsize() == 0:
                    break

        if not task:
            return

        if isinstance(task, StopTask):
            self._stop_task = task
            return

        log.debug(f'[{self.name}] Have message')

        task.metrics.stop_transfer_timer(self.step_name)
        task_size = getattr(self.queue_in, 'task_size', None)
        if task_size:
            task.metrics.save_task_size(task_size, self.step_name)

        # don't pass an expired task to the next steps
        if task.is_expired():
            log.debug(f'[{self.name}] Task expired. Skip: {task}')
            return

        # don't process unsuitable tasks
        if not self.handle_condition(task):
            self._post_handle(task)
            return

        return task

    def _get_batch_with_timeout(self, batch: List[BaseTask]) -> List[BaseTask]:
        """ Collecting incoming tasks into batch.
        We will be waiting until number of tasks in batch would be equal `batch_size`.
        If batch could not be fully collected in batch_timeout time -> return what we have at that time.
        """
        timeout = self._batch_timeout
        while True:
            with timeit() as passed_time:
                task = self._wait_task(timeout)

            if task:
                batch.append(task)

                if len(batch) == self._batch_size:
                    return batch

            timeout -= passed_time.seconds

            if self._stop_task or timeout <= 0:
                return batch

            # timeout should not be less then 1ms, to avoid unnecessary short sleeps
            timeout = max(timeout, 0.001)

    def _get_batch_dynamic(self, batch):
        """ Collecting incoming tasks into batch.
        This method will not be waiting for batch_timeout to collect full batch,
        we just simply get all tasks that are currently in the queue and making batch only from them.
        """
        while True:
            task = None
            task = self._wait_task(timeout=0.)

            if task:
                batch.append(task)
                if len(batch) == self._batch_size:
                    break
            else:
                break

        return batch

    def _wait_batch(self) -> List[BaseTask]:
        batch = []
        timer = Timer()

        # wait for the first task
        while True:
            # wait for some long amount of time (10 secs), and wait again,
            # until we eventually get a task
            task = self._wait_task(10.)
            if task:
                batch.append(task)
                timer.start()
                break
            elif self._stop_task:
                return []

        # waiting for the rest of the batch if batch_size > 1
        if self._batch_size > 1:
            if self._batch_timeout > 0:
                batch = self._get_batch_with_timeout(batch)
            else:
                batch = self._get_batch_dynamic(batch)

        timer.stop()
        if self._batch_size > 1:
            batch[0].metrics.batch_times.add(self.step_name, timer.seconds)
            batch[0].metrics.batch_sizes.add(self.step_name, len(batch))

        return batch

    def _iter_batches(self) -> Iterator[Optional[List[BaseTask]]]:
        """ Returns iterator over input task batches.
        If there is no tasks in queue -> block until task appears.
        This iterator takes into account batch_timeout and batch size.
        If input task is expired or filtered by condition - it would not be placed in batch.
        """
        while True:
            if self._batch_lock:
                # to take a queue_in lock for the duration of batch filling time
                with self._batch_lock:
                    tasks_batch = self._wait_batch()
            else:
                tasks_batch = self._wait_batch()

            if tasks_batch:
                yield tasks_batch

            if self._stop_task:
                break

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

        for tasks_batch in self._iter_batches():
            with timeit() as timer:
                self.task_handler.handle(*tasks_batch)

            for task in tasks_batch:
                task.metrics.handle_times.add(self.step_name, timer.seconds)
                self._post_handle(task)

        if self._stop_task:
            self.queue_out.put(self._stop_task)
