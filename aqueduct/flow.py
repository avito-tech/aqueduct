import asyncio
import multiprocessing as mp
import os
import queue
import signal
import sys
from enum import Enum
from functools import cached_property
from functools import reduce
from multiprocessing import Barrier
from threading import BrokenBarrierError
from typing import Callable, Dict, List, Optional, Union

from multiprocessing.resource_tracker import _resource_tracker  # noqa

from .exceptions import FlowError, NotRunningError
from .handler import BaseTaskHandler
from .logger import log
from .metrics import MAIN_PROCESS, MetricsTypes
from .metrics.collect import Collector, TasksStats
from .metrics.export import Exporter
from .metrics.manager import get_metrics_manager
from .metrics.timer import timeit
from .multiprocessing import (
    ProcessContext,
    ProcessExitedException,
    ProcessRaisedException,
    start_processes,
)
from .task import BaseTask, StopTask
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
            handle_condition: Callable[[BaseTask], bool] = lambda x: True,
            nprocs: int = 1,
            batch_size: int = 1,
            batch_timeout: float = 0.5,
    ):
        _check_env()
        self.handler = handler
        self.handle_condition = handle_condition
        self.nprocs = nprocs
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout


class FlowState(Enum):
    RUNNING = 'running'
    STOPPING = 'stopping'
    STOPPED = 'stopped'


class Flow:

    def __init__(
            self,
            *steps: Union[FlowStep, BaseTaskHandler],
            metrics_enabled: bool = True,
            metrics_collector: Collector = None,
            metrics_exporter: Exporter = None,
            queue_size: int = 20,
    ):
        _check_env()

        self._steps: List[FlowStep] = [step if isinstance(step, FlowStep)
                                       else FlowStep(step) for step in steps]
        self._contexts: Dict[BaseTaskHandler, ProcessContext] = {}
        self._queues: List[mp.Queue] = []
        self._task_futures: Dict[str, asyncio.Future] = {}
        self._queue_size: int = queue_size

        if not metrics_enabled:
            log.warn('Metrics collecting is disabled')
            metrics_collector = Collector(collectible_metrics=[])
        self._metrics_manager = get_metrics_manager(metrics_collector, metrics_exporter)

        self._state: FlowState = None  # noqa
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

            task.set_timeout(timeout_sec)
            task.metrics.start_transfer_timer(MAIN_PROCESS)
            while True:
                try:
                    self._queues[0].put(task, block=False)
                except queue.Full:
                    await asyncio.sleep(0.001)
                else:
                    break

            tasks_stats = TasksStats()
            try:
                finished_task: BaseTask = await asyncio.wait_for(future, timeout=timeout_sec)
            # todo is it correct to hide a specific error behind a general FlowError?
            except asyncio.TimeoutError:
                tasks_stats.timeout += 1
                raise FlowError('Task timeout error')
            except asyncio.CancelledError:
                tasks_stats.cancel += 1
                raise FlowError('Task was cancelled')
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
            self._queues[0].put(StopTask())
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

    def _run_steps(self, timeout: Optional[int]):
        self._queues.append(mp.Queue(self._queue_size))

        total_procs = reduce(lambda a, b: a + b.nprocs, self._steps, 0)
        # also count main process
        total_procs += 1
        start_barrier = Barrier(total_procs)

        for step_number, step in enumerate(self._steps, 1):
            self._queues.append(mp.Queue(self._queue_size))
            worker_curr = Worker(
                self._queues[-2],
                self._queues[-1],
                step.handler,
                step.handle_condition,
                step.batch_size,
                step.batch_timeout,
                mp.RLock() if step.nprocs > 1 and step.batch_size > 1 else None,
                step_number,
            )
            self._contexts[step.handler] = start_processes(
                worker_curr.loop,
                nprocs=step.nprocs, join=False, daemon=True, start_method='fork',
                args=(start_barrier,),
            )
            log.info(f'Created step {step.handler}, '
                     f'queue_in: {self._queues[-2]}, queue_out:{self._queues[-1]}')

        try:
            log.info('Waiting for all workers to startup...')
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

        return {queue_: f'from_{from_}_to_{to}'
                for queue_, (from_, to) in zip(self._queues, step_names(self._contexts))}

    async def _fetch_processed(self):
        while True:
            try:
                task: BaseTask = self._queues[-1].get(block=False)
                task.metrics.stop_transfer_timer(MAIN_PROCESS)
            except queue.Empty:
                await asyncio.sleep(0.001)
                continue
            if task.task_id in self._task_futures:
                future = self._task_futures[task.task_id]
                if not future.cancelled() and not future.done():
                    future.set_result(task)

    async def _check_is_alive(self, sleep_sec: float = 1.):
        """Checks that all child processes are alive.

        If at least one process is not alive, it stops Flow.
        """
        while True:
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
