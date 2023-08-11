import asyncio
import multiprocessing as mp
import operator
import os
import queue
import signal
import sys
from enum import Enum
from functools import cached_property
from functools import reduce
from itertools import chain
from multiprocessing import Barrier
from threading import BrokenBarrierError
from time import monotonic
from typing import Dict, List, Literal, Optional, Union

from concurrent.futures import ThreadPoolExecutor, Future
from multiprocessing.resource_tracker import _resource_tracker  # noqa

from .exceptions import FlowError, MPStartMethodValueError, NotRunningError
from .handler import BaseTaskHandler, HandleConditionType
from .logger import log
from .metrics import MAIN_PROCESS, MetricsTypes
from .metrics.collect import Collector, TasksStats
from .metrics.export import Exporter
from .metrics.manager import get_metrics_manager
from .metrics.queue import TaskMetricsQueue
from .metrics.timer import timeit
from .multiprocessing import (
    ProcessContext,
    ProcessExitedException,
    ProcessRaisedException,
    start_processes,
)
from .queues import FlowStepQueue, select_next_queue
from .task import BaseTask, DEFAULT_PRIORITY, StopTask
from .worker import Worker

# just for using common ResourceTracker in main and child processes and avoiding
# unnecessary shared memory resource_tracker "No such file or directory" warnings
_resource_tracker.ensure_running()


def _check_env():
    if not sys.version_info >= (3, 8):
        raise RuntimeError('Requires python 3.8 or higher to use multiprocessing.shared_memory')


class FlowStep:

    def __init__(
            self,
            handler: BaseTaskHandler,
            handle_condition: HandleConditionType = operator.truth,
            nprocs: int = 1,
            batch_size: int = 1,
            batch_timeout: float = 0,
            on_start_wait: float = 0,
    ):
        _check_env()
        self.handler = handler
        self.handle_condition = handle_condition
        self.nprocs = nprocs
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.on_start_wait = on_start_wait


class FlowState(Enum):
    RUNNING = 'running'
    STARTING = 'starting'
    STOPPING = 'stopping'
    STOPPED = 'stopped'


class Flow:

    def __init__(
            self,
            *steps: Union[FlowStep, BaseTaskHandler],
            metrics_enabled: bool = True,
            metrics_collector: Collector = None,
            metrics_exporter: Exporter = None,
            queue_size: Optional[int] = None,
            queue_priorities: int = 1,
            mp_start_method: Literal['fork', 'spawn', 'forkserver'] = 'fork',
    ):
        _check_env()

        self._steps: List[FlowStep] = [step if isinstance(step, FlowStep)
                                       else FlowStep(step) for step in steps]
        self._contexts: Dict[BaseTaskHandler, ProcessContext] = {}
        self._queue_priorities = queue_priorities
        self._queues: List[List[FlowStepQueue]] = []
        self._task_futures: Dict[str, asyncio.Future] = {}
        self._queue_size: Optional[int] = queue_size

        if mp_start_method != "fork" and mp_start_method != mp.get_start_method():
            log.error(f'MP start method {mp_start_method!r} is set for Flow, it should also be set'
                      f' in the if __name__ == "__main__" clause of the main module')
            raise MPStartMethodValueError(f'Multiprocessing start method mismatch: '
                                          f'got {mp.get_start_method()!r} for main process '
                                          f'and {mp_start_method!r} for Flow')
        self._mp_start_method = mp_start_method

        if not metrics_enabled:
            log.warn('Metrics collecting is disabled')
            metrics_collector = Collector(collectible_metrics=[])
        self._metrics_manager = get_metrics_manager(metrics_collector, metrics_exporter)

        self._state: FlowState = FlowState.STOPPED
        self._tasks: List[asyncio.Future] = []

    @property
    def state(self):
        return self._state

    @property
    def is_running(self) -> bool:
        return self._state == FlowState.RUNNING

    def start(self, timeout: Optional[int] = None):
        """
        Starts Flow and waits for all subprocesses to initialize.

        raising FlowError if 'timeout' is set and some handler was not able to initialize in time.
        """
        log.info('Flow is starting')
        self._state = FlowState.STARTING
        self._run_steps(timeout)
        self._run_tasks()
        self._state = FlowState.RUNNING
        log.info('Flow was started')

    async def process(self, task: BaseTask, timeout_sec: float = 5.) -> bool:
        """
        Starts processing task through pipeline and returns True if all was ok.
        The result is saved in the source task
        """

        if not self.is_running:
            raise NotRunningError

        with timeit() as timer:

            future = asyncio.Future()
            self._task_futures[task.task_id] = future

            task.priority = min(task.priority, self._queue_priorities - 1)
            task.set_timeout(timeout_sec)
            task.metrics.start_transfer_timer(MAIN_PROCESS)

            start_time = monotonic()
            to_queue = select_next_queue(
                queues=self._queues,
                task=task,
            )
            while self.state != FlowState.STOPPED and (monotonic() - start_time) < timeout_sec:
                try:
                    to_queue.put(task, block=False)
                except queue.Full:
                    await asyncio.sleep(0.001)
                else:
                    break

            elapsed_time = monotonic() - start_time

            tasks_stats = TasksStats()
            try:
                finished_task: BaseTask = await asyncio.wait_for(
                    future,
                    timeout=(timeout_sec - elapsed_time),
                )
            # todo is it correct to hide a specific error behind a general FlowError?
            except asyncio.TimeoutError:
                tasks_stats.timeout += 1
                raise FlowError('Task timeout error')
            except asyncio.CancelledError:
                tasks_stats.cancel += 1
                if self.state in (FlowState.STOPPING, FlowState.STOPPED):
                    raise FlowError('Task was cancelled')
                else:
                    # process was cancelled by external actor, so reraise
                    raise

            else:
                tasks_stats.complete += 1
            finally:
                self._metrics_manager.collector.add_tasks_stats(tasks_stats)
                del self._task_futures[task.task_id]

        finished_task.metrics.handle_times.add('total', timer.seconds)

        if self.need_collect_task_timers:
            self._metrics_manager.collector.add_task_metrics(finished_task.metrics)

        task.update(finished_task)

        return True

    async def stop(self, graceful: bool = True):
        if not self.is_running:
            log.info('Flow is not running')
            return

        self._state = FlowState.STOPPING
        log.info('Flow is stopping')

        if graceful:
            self._queues[DEFAULT_PRIORITY][0].queue.put(StopTask())
            await asyncio.sleep(3)

        self._metrics_manager.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._task_futures.values():
            task.cancel()

        self._join_context(self._processes_context)

        self._state = FlowState.STOPPED
        log.info('Flow was stopped')

    @cached_property
    def need_collect_task_timers(self) -> bool:
        return self._metrics_manager.collector.is_collectible(MetricsTypes.TASK_TIMERS)

    def _calc_queue_size(self, step: FlowStep):
        """ If queue size not specified manually, get queue size based on batch size for handler.
        We need at least batch_size places in queue and then some additional space
        """
        if self._queue_size:
            return self._queue_size

        # queue should be able to store at least 20 task, that's seems reasonable
        return max(step.batch_size*3, 20)

    def _run_steps(self, timeout: Optional[int]):
        if len(self._steps) == 0:
            log.info('Flow has zero steps -> do nothing')
            return

        # count how many processes we will create, to setup a barrier
        total_procs = reduce(lambda a, b: a + b.nprocs, self._steps, 0)
        # also count main process
        total_procs += 1
        start_barrier = Barrier(total_procs)

        for _ in range(self._queue_priorities):
            queues: List[FlowStepQueue] = []
            for step in self._steps:
                queue_size = self._calc_queue_size(step)
                queues.append(
                    FlowStepQueue(
                        queue=TaskMetricsQueue(queue_size),
                        handle_condition=step.handle_condition,
                    )
                )

            # Add out queue
            queue_size = self._calc_queue_size(self._steps[-1])
            queues.append(
                FlowStepQueue(
                    queue=TaskMetricsQueue(queue_size),
                    handle_condition=operator.truth,
                )
            )
            self._queues.append(queues)

        for step_number, step in enumerate(self._steps, 1):
            worker_curr = Worker(
                queues=self._queues,
                task_handler=step.handler,
                batch_size=step.batch_size,
                batch_timeout=step.batch_timeout,
                batch_lock=mp.RLock() if step.nprocs > 1 and step.batch_size > 1 else None,
                read_lock=mp.RLock(),
                step_number=step_number,
            )
            self._contexts[step.handler] = start_processes(
                worker_curr.loop,
                nprocs=step.nprocs, join=False, daemon=True, start_method=self._mp_start_method,
                args=(start_barrier,),
                on_start_wait=step.on_start_wait,
            )
            log.info(f'Created step {step.handler}, '
                     f'queue_in: {self._queues[DEFAULT_PRIORITY][step_number - 1].queue}, '
                     f'queue_out: {self._queues[DEFAULT_PRIORITY][step_number].queue}')

        # fix to avoid deadlock on program exit
        for step_queue in chain.from_iterable(self._queues):
            step_queue.queue.cancel_join_thread()

        try:
            log.info(f'Waiting for all workers to startup for {timeout} seconds...')
            start_barrier.wait(timeout)
        except BrokenBarrierError:
            raise TimeoutError('Starting timeout expired')

    def _run_tasks(self):
        self._tasks.append(asyncio.ensure_future(self._fetch_processed()))
        self._tasks.append(asyncio.ensure_future(self._check_is_alive()))

        self._metrics_manager.start(queues_info=self._get_queues_info())

    def _get_queues_info(self) -> Dict[mp.Queue, str]:
        """Returns queues between Step handlers and its names.

        The queue name consists of two handler names that are connected by this queue.
        """

        def step_names(handlers):
            from_step = MAIN_PROCESS
            for step_number, handler in enumerate(handlers, 1):
                to_step = handler.get_step_name(step_number)
                yield from_step, to_step
                from_step = to_step
            yield from_step, MAIN_PROCESS
        result = {}
        for priority in range(self._queue_priorities):
            result.update(
                {
                    queue_.queue: f'p_{priority}_from_{from_}_to_{to}' if priority > 0 else f'from_{from_}_to_{to}'
                    for queue_, (from_, to) in zip(self._queues[priority], step_names(self._contexts))
                }
            )
        return result

    @staticmethod
    def _fetch_from_queue(out_queue: mp.Queue) -> Union[BaseTask, None]:
        try:
            task = out_queue.get(timeout=1.)
            return task
        except queue.Empty:
            return None

    async def _fetch_processed(self):
        """ Fetching messages from output queue.

        To handle messages from another process and not block asyncio loop, we run queue.get()
        in a separate thread

        """
        loop = asyncio.get_event_loop()
        running_futures: Dict[int, Optional[Future]] = {}
        with ThreadPoolExecutor(max_workers=self._queue_priorities) as queue_fetch_executor:
            while self.state != FlowState.STOPPED:
                for priority in reversed(range(self._queue_priorities)):
                    if running_futures.get(priority) is None:
                        future = loop.run_in_executor(
                            queue_fetch_executor,
                            self._fetch_from_queue,
                            self._queues[priority][-1].queue,
                        )
                        running_futures[priority] = future
                await asyncio.wait(running_futures.values(), return_when=asyncio.FIRST_COMPLETED)

                tasks = []
                for priority in reversed(range(self._queue_priorities)):
                    future = running_futures.get(priority)
                    if future is not None and future.done():
                        running_futures[priority] = None
                        task = future.result()
                        if task is not None:
                            tasks.append(task)

                for task in tasks:
                    task.metrics.stop_transfer_timer(MAIN_PROCESS, task.priority)
                    task_size = getattr(self._queues[task.priority][-1].queue, 'task_size', None)
                    if task_size:
                        task.metrics.save_task_size(task_size, MAIN_PROCESS, task.priority)

                    future = self._task_futures.get(task.task_id)

                    if future and not future.cancelled() and not future.done():
                        future.set_result(task)

    async def _check_is_alive(self, sleep_sec: float = 1.):
        """Checks that all child processes are alive.

        If at least one process is not alive, it stops Flow.
        """
        while self.state != FlowState.STOPPED:
            for handler, context in self._contexts.items():
                for proc in context.processes:
                    if not proc.is_alive():
                        if self.is_running:
                            log.error('The process %s for %s handler is dead',
                                      proc.pid, handler.__class__.__name__)
                            await self.stop(graceful=False)
            await asyncio.sleep(sleep_sec)

    @staticmethod
    def _join_context(context: ProcessContext, timeout_sec: float = 0.01):
        try:
            context.join(timeout_sec)
        except (ProcessExitedException, ProcessRaisedException):
            pass

        for p in context.processes:
            if p.is_alive():
                os.kill(p.pid, signal.SIGKILL)

    @property
    def _processes_context(self) -> ProcessContext:
        processes, error_queues = [], []
        for context in self._contexts.values():
            processes.extend(context.processes)
            error_queues.extend(context.error_queues)
        return ProcessContext(processes=processes, error_queues=error_queues)
