import concurrent.futures
import multiprocessing as mp
import queue
import signal
import sys
from threading import BrokenBarrierError
from time import monotonic
import time
from typing import Iterator, List, Optional, Union
import asyncio
import gc

from .handler import AsyncTaskHandler, BaseTaskHandler
from .logger import log
from .metrics.timer import (
    Timer,
    timeit,
)
from .queues import FlowStepQueue, select_next_queue
from .task import BaseTask, DEFAULT_PRIORITY, StopTask

MAX_SPIN_ITERATIONS = 1000
MIN_SLEEP_DURATION = 0.001
MAX_SLEEP_DURATION = 0.01
GC_COLLECT_FREQUENCY = 100


class Worker:
    """Wrapper over the BaseTaskHandler class.

    Takes a task from the input queue, passes it through task_handler and puts it in the output queue.
    """

    def __init__(
            self,
            queues: List[List[FlowStepQueue]],
            task_handler: Union[BaseTaskHandler, AsyncTaskHandler],
            batch_size: int,
            batch_timeout: float,
            batch_lock: Optional[mp.RLock],
            read_lock: mp.RLock,
            step_number: int,
    ):
        self._queues = queues
        self._queue_priorities = len(self._queues)
        self.task_handler = task_handler
        self.name = task_handler.__class__.__name__
        self.step_name = self.task_handler.get_step_name(step_number)
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._batch_lock = batch_lock
        self._stop_task: BaseTask = None  # noqa
        self._read_lock = read_lock
        self._step_number = step_number
        self._read_queues = [
            self._queues[priority][self._step_number - 1].queue
            for priority in reversed(range(self._queue_priorities))
        ]
        self._loop = None  # Initialize to None to prevent dangling references

    def _start(self):
        """Runs something huge (e.g. model) in child process."""
        if isinstance(self.task_handler, AsyncTaskHandler):
            if not self._loop:
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self.task_handler.on_start())
        else:
            self.task_handler.on_start()

    def _wait_task_no_timeout(self) -> Optional[BaseTask]:
        # There is a problem with Python MP Queue implementation.
        # queue.get() can raise Empty exception even thou queue is in fact not empty.
        # For example, if we have 10 tasks in a queue, we expect batch size to be 10, but it is not always true, because
        #  after reading, say, 5 tasks, Python queue can tell as that there is nothing left, which is in fact false.
        # In our implementation we use an additional spin over a queue to guarantee consistent results.
        # More info: https://bugs.python.org/issue23582
        task: Optional[BaseTask] = None
        i = 0
        if self._read_lock.acquire(block=False):
            try:
                while not task and i < MAX_SPIN_ITERATIONS:
                    # Limit our spin with maximum of MAX_SPIN_ITERATIONS just in case.
                    # We do not want long, purposeless iteration
                    i += 1

                    for queue_in in self._read_queues:
                        try:
                            task = queue_in.get(block=False)
                        except queue.Empty:
                            continue
                        else:
                            task_size = getattr(queue_in, 'task_size', None)
                            if task_size and not isinstance(task, StopTask):
                                task.metrics.save_task_size(task_size, self.step_name, task.priority)
                            break

                    if task is None:
                        # additionally check queue size to make sure that there is in fact no tasks there.
                        # if size > 0 then Empty exception was fake -> we should try to call get() again
                        # since macos does not support qsize method - ignore that check
                        # (it would be great to make a proper queue later)
                        if sys.platform == 'darwin' or all(queue_in.qsize() == 0 for queue_in in self._read_queues):
                            break
            finally:
                self._read_lock.release()
        return task

    def _wait_task_with_timeout(self, timeout: float) -> Optional[BaseTask]:
        task: Optional[BaseTask] = None
        deadline = monotonic() + timeout
        if self._read_lock.acquire(block=True, timeout=timeout):
            try:
                remaining_timeout = deadline - monotonic()
                if remaining_timeout > 0:
                    mp.connection.wait(
                        [queue._reader for queue in self._read_queues],
                        timeout=remaining_timeout,
                    )
                task = self._wait_task_no_timeout()
            finally:
                self._read_lock.release()
        return task

    def _wait_task(self, timeout: float) -> Optional[BaseTask]:
        task: Optional[BaseTask] = None
        if timeout == 0:
            task = self._wait_task_no_timeout()
        else:
            task = self._wait_task_with_timeout(timeout)

        if not task:
            return

        if isinstance(task, StopTask):
            self._stop_task = task
            return

        log.debug(f'[{self.name}] Have message')

        task.metrics.stop_transfer_timer(self.step_name, task.priority)

        # don't pass an expired task to the next steps
        if task.is_expired():
            log.debug(f'[{self.name}] Task expired. Skip: {task}')
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

    def _get_batch_dynamic(self, batch: List[BaseTask]) -> List[BaseTask]:
        """ Collecting incoming tasks into batch.
        This method will not be waiting for batch_timeout to collect full batch,
        we just simply get all tasks that are currently in the queue and making batch only from them.
        """
        while True:
            task = None
            task = self._wait_task(timeout=0.0)

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
            task = self._wait_task(10.0)
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
        queue_out = select_next_queue(
            queues=self._queues,
            task=task,
            start_index=self._step_number,
        )
        queue_out.put(task)

    def _fix_signals(self):
        """ Sometimes some web frameworks (e.g. unicorn) override signal handlers.
        If processes use start method fork,
        so when we call flow.stop() processes do not terminate and hang.
        Here we restore the signals we need.
        """
        signal.signal(signal.SIGTERM, signal.Handlers.SIG_DFL)
        signal.signal(signal.SIGINT, signal.default_int_handler)

    def loop(self, pid: int, start_barrier: mp.Barrier):
        """Main worker loop.

        The code below is executed in a new process.
        """
        log.info(f'[Worker] initialising handler {self.name}')
        self._fix_signals()
        self._start()
        log.info(f'[Worker] handler {self.name} ok, waiting for others to start')

        try:
            start_barrier.wait()
        except BrokenBarrierError:
            raise TimeoutError('Starting timeout expired')

        log.info(f'[Worker] handler {self.name} ok, starting loop')

        try:
            if isinstance(self.task_handler, AsyncTaskHandler):
                if not self._loop:
                    raise ValueError('Loop is not initialized')
                try:
                    self._loop.run_until_complete(self.loop_async())
                finally:
                    # Proper cleanup of the event loop
                    try:
                        # Cancel all pending tasks
                        pending = asyncio.all_tasks(self._loop)
                        if pending:
                            for task in pending:
                                task.cancel()
                            # Wait for all tasks to be cancelled
                            self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception:
                        pass  # Ignore cleanup errors
                    finally:
                        self._loop.close()
                        self._loop = None
            else:
                self.loop_sync()
        finally:
            # Ensure stop task is forwarded even if an exception occurs
            if self._stop_task:
                self._queues[DEFAULT_PRIORITY][self._step_number].queue.put(self._stop_task)

    def end_task(self, task: BaseTask, seconds: float):
        task.metrics.handle_times.add(self.step_name, seconds)
        self._post_handle(task)

    def loop_sync(self):
        for tasks_batch in self._iter_batches():
            timer = timeit()
            with timer:
                self.task_handler.handle(*tasks_batch)

            for task in tasks_batch:
                task.metrics.handle_times.add(self.step_name, timer.seconds)
                self._post_handle(task)

    def _run_async_handler_in_thread(self, tasks_batch):
        """Wrapper function to run async handler in a thread with its own event loop.
        
        The async handler should set task.done() when each task is finished
        to enable immediate forwarding to the next step instead of waiting for the entire batch.
        
        Example usage in your AsyncTaskHandler:
            async def handle(self, *tasks):
                first_batch = tasks[:self._batch_size] # e.g. batch of images of same size
                second_batch = tasks[self._batch_size:] # e.g. another batch of images of other same size
                
                # e.g. torch cuda operation on images
                result = some_io_bound_operation(first_batch)
                for task, result in zip(first_batch, result):
                    task.result = result
                    task.done()

                # here the tasks in first_batch are done and headed to the next step
                result = some_io_bound_operation(second_batch)
                for task, result in zip(second_batch, result):
                    task.result = result
                    task.done()

                # here the tasks in second_batch are done and headed to the next step as usual
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            return loop.run_until_complete(self.task_handler.handle(*tasks_batch))
        finally:
            # Properly close the event loop to prevent memory leaks
            try:
                # Cancel all pending tasks
                pending = asyncio.all_tasks(loop)
                if pending:
                    for task in pending:
                        task.cancel()
                    # Wait for all tasks to be cancelled
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass  # Ignore cleanup errors
            finally:
                loop.close()

    async def loop_async(self):
        if not isinstance(self.task_handler, AsyncTaskHandler):
            raise ValueError('AsyncTaskHandler is not supported for sync loop')

        batch_counter = 0  # Track batch count for periodic GC
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            for tasks_batch in self._iter_batches():
                start_time = time.monotonic()

                batch_counter += 1
                

                batch_size = len(tasks_batch)
                processed_tasks = set()  
                

                for task in tasks_batch:
                    task.undone()


                executor_future = self._loop.run_in_executor(
                    pool, 
                    self._run_async_handler_in_thread, 
                    tasks_batch
                )

                sleep_duration = MIN_SLEEP_DURATION
                consecutive_empty_checks = 0
                
                while not executor_future.done():

                    current_completed = 0
                    for task in tasks_batch:

                        if task.task_id in processed_tasks or not task.is_done():
                            continue
                            

                        processed_tasks.add(task.task_id)
                        current_completed += 1
                        log.debug(f'[{self.name}] Task completed.')
                        self.end_task(task, time.monotonic() - start_time)
                        log.debug(f'[{self.name}] Task ended in {time.monotonic() - start_time} seconds')
                    
                    if current_completed > 0:
                        log.debug(f'[{self.name}] Processed {current_completed} tasks this iteration')
                        log.debug(f'[{self.name}] Total processed: {len(processed_tasks)}/{batch_size}')
                    

                    if current_completed == 0:
                        consecutive_empty_checks += 1

                        sleep_duration = min(MAX_SLEEP_DURATION, sleep_duration * 1.1)
                    else:
                        consecutive_empty_checks = 0
                        sleep_duration = MIN_SLEEP_DURATION  
                    
                    log.debug(f'[{self.name}] Sleeping for {sleep_duration} seconds')
                    await asyncio.sleep(sleep_duration)


                try:
                    executor_future.result()
                except Exception:

                    for task in tasks_batch:
                        if task.is_done():
                            task.undone()
                    raise


                if len(processed_tasks) < batch_size:
                    log.debug(f'[{self.name}] Processing remaining task.')
                    for task in tasks_batch:
                        if task.task_id not in processed_tasks:
                            self.end_task(task, time.monotonic() - start_time)
                            log.debug(f'[{self.name}] Task ended in {time.monotonic() - start_time} seconds')
                

                for task in tasks_batch:
                    if task.is_done():
                        task.undone()
                

                del tasks_batch
                

                if batch_counter % GC_COLLECT_FREQUENCY == 0:
                    gc.collect()
                    batch_counter = 0

