import asyncio
import os
import time
from typing import List

import numpy as np
import pytest

from aqueduct.exceptions import (
    FlowError,
    NotRunningError,
)
from aqueduct.flow import (
    Flow,
    FlowState,
    FlowStep,
)
from aqueduct.handler import BaseTaskHandler
from aqueduct.metrics import MAIN_PROCESS
from aqueduct.task import BaseTask
from tests.unit.conftest import (
    Task,
    run_flow,
    terminate_worker,
)
from tests.unit.test_shm import ArrayFieldTask

TASKS_BATCH_SIZE = 16


class HandlerWithError(BaseTaskHandler):
    def handle(self, *tasks: BaseTask):
        raise ValueError


@pytest.fixture
async def flow_with_failed_handler(loop):
    async with run_flow(Flow(HandlerWithError())) as flow:
        yield flow


class CatDetector:
    """GPU model emulator that predicts the presence of the cat in the image."""
    IMAGE_PROCESS_TIME = 0.01
    BATCH_REDUCTION_FACTOR = 0.7
    OVERHEAD_TIME = 0.02
    BATCH_PROCESS_TIME = IMAGE_PROCESS_TIME * TASKS_BATCH_SIZE * BATCH_REDUCTION_FACTOR + OVERHEAD_TIME

    def predict(self, images: np.array) -> np.array:
        """Always says that there is a cat in the image.

        The image is represented by a one-dimensional array.
        The model spends less time for processing batch of images due to GPU optimizations. It's emulated
        with BATCH_REDUCTION_FACTOR coefficient.
        """
        batch_size = images.shape[0]
        if batch_size == 1:
            time.sleep(self.IMAGE_PROCESS_TIME)
        else:
            time.sleep(self.IMAGE_PROCESS_TIME * batch_size * self.BATCH_REDUCTION_FACTOR)
        return np.ones(batch_size, dtype=bool)


class CatDetectorHandler(BaseTaskHandler):
    def handle(self, *tasks: ArrayFieldTask):
        images = np.array([task.array for task in tasks])
        predicts = CatDetector().predict(images)
        for task, predict in zip(tasks, predicts):
            task.result = predict


@pytest.fixture
async def flow_with_not_batch_handler(loop) -> Flow:
    """Actually the handler has batching, but batch size is one."""
    async with run_flow(Flow(CatDetectorHandler())) as flow:
        yield flow


@pytest.fixture
async def flow_with_batch_handler(loop) -> Flow:
    async with run_flow(Flow(
            FlowStep(
                CatDetectorHandler(),
                batch_size=TASKS_BATCH_SIZE,
            ))) as flow:
        yield flow


async def process_tasks(flow: Flow, tasks: List[ArrayFieldTask]):
    await asyncio.gather(*(flow.process(task) for task in tasks))


@pytest.fixture
def tasks_batch(array):
    return [ArrayFieldTask(array) for _ in range(TASKS_BATCH_SIZE)]


class ShareArrayHandler(BaseTaskHandler):
    def __init__(self, array: np.array):
        super().__init__()
        self._array = array

    def handle(self, *tasks: BaseTask):
        for task in tasks:
            task.array = self._array
            task.share_value('array')


class UpdateSharedArrayFieldHandler(BaseTaskHandler):
    def handle(self, *tasks: BaseTask):
        for task in tasks:
            task.array[0] = task.array[-1]  # noqa


@pytest.fixture
async def flow_for_shared_array(loop, array) -> Flow:
    async with run_flow(Flow(ShareArrayHandler(array), UpdateSharedArrayFieldHandler())) as flow:
        yield flow


class MultiProcBatchHandler(BaseTaskHandler):
    def handle(self, *tasks: Task):
        for task in tasks:
            task.result = os.getpid()


@pytest.fixture
async def flow_with_multiproc_bathcer(loop):
    async with run_flow(
            Flow(FlowStep(MultiProcBatchHandler(), nprocs=2, batch_size=TASKS_BATCH_SIZE))
    ) as flow:
        yield flow


class SlowStartingHandler(BaseTaskHandler):
    start_time: int

    def on_start(self):
        time.sleep(self.start_time)

    def handle(self, task: Task):
        task.result = os.getpid()


class TestFlow:
    async def test_start_waiting_for_subproccess_to_start(self, task: Task):
        """flow.start() should block until all subprocesses fully initialized."""
        handler = SlowStartingHandler()
        handler.start_time = 1
        slowest_handler = SlowStartingHandler()
        slowest_handler.start_time = 3
        flow = Flow(FlowStep(handler, nprocs=3), FlowStep(slowest_handler, nprocs=3))

        t0 = time.time()
        flow.start()
        t1 = time.time()

        assert t1 - t0 == pytest.approx(slowest_handler.start_time, 0.5)

    async def test_start_waiting_timeout(self, task: Task):
        """flow.start(timeout) should raise FlowError when starting_timeout expired."""
        handler = SlowStartingHandler()
        handler.start_time = 10
        flow = Flow(FlowStep(handler, nprocs=3))

        timeout = 3
        t0 = time.time()
        with pytest.raises(TimeoutError):
            flow.start(timeout=timeout)
        t1 = time.time()

        assert t1 - t0 == pytest.approx(timeout, 0.5)

    async def test_process_set_result(self, simple_flow: Flow, task: Task):
        assert task.result is None
        result = await simple_flow.process(task)
        assert result
        assert task.result == 'test'

    def test_get_queues_info(self, simple_flow: Flow):
        info = simple_flow._get_queues_info()
        queues = simple_flow._queues
        assert len(info) == len(queues)
        assert MAIN_PROCESS in info[queues[0]]
        assert MAIN_PROCESS in info[queues[-1]]

    async def test_process_expired_task(self, log_file, aqueduct_logger,
                                        slow_sleep_handlers, slow_simple_flow, task):
        timeouts = [handler._handler_sec for handler in slow_sleep_handlers]

        with pytest.raises(FlowError, match='timeout'):
            await slow_simple_flow.process(task, timeout_sec=timeouts[0])
        await asyncio.sleep(sum(timeouts))
        log_str = log_file.read().decode()

        # task is processed by first handler
        assert f'[{slow_sleep_handlers[0].__class__.__name__}] Have message' in log_str
        # task arrives in the second handler, but it is not processed because it has expired
        assert f'[{slow_sleep_handlers[1].__class__.__name__}] Task expired. Skip' in log_str
        # doesn't arrive in the third handler
        assert f'[{slow_sleep_handlers[2].__class__.__name__}] Have message' not in log_str

    async def test_process_not_running_flow(self, simple_flow, task):
        await simple_flow.stop(graceful=False)
        with pytest.raises(NotRunningError):
            await simple_flow.process(task)

    async def test_process_huge_count_of_tasks(self, simple_flow):
        """Count of tasks much more than queue size."""
        # queue size = 20
        n_tasks = 90

        coros = [simple_flow.process(Task()) for _ in range(n_tasks)]
        res = await asyncio.gather(*coros)
        assert len(res) == n_tasks

    async def test_stopped_flow_due_to_terminated_step_process(self, simple_flow: Flow, caplog):
        """Checks that flow and event loop were stopped when child Step process was terminated."""
        assert simple_flow.state == FlowState.RUNNING

        process, handler = await terminate_worker(simple_flow)

        assert simple_flow.state == FlowState.STOPPED
        assert f'The process {process.pid} for {handler.__class__.__name__} handler is dead' in caplog.text

    async def test_stopped_flow_due_to_failed_step_process(self, flow_with_failed_handler, task):
        """Checks that flow and event loop were stopped when child Step process finished with error."""
        assert flow_with_failed_handler.state == FlowState.RUNNING

        with pytest.raises(FlowError):
            await flow_with_failed_handler.process(task)
        await asyncio.sleep(1)

        assert flow_with_failed_handler.state == FlowState.STOPPED

    async def test_stopped_flow_due_to_stop_method(self, simple_flow):
        await simple_flow.stop(graceful=False)
        await asyncio.sleep(0.5)

        assert simple_flow.state == FlowState.STOPPED

    async def test_stop_finish_task_on_graceful(self, sleep_handlers, simple_flow, task):
        """Checks that task will be processed to the end after Flow stop command with graceful mode."""
        t = asyncio.ensure_future(simple_flow.process(task))
        await asyncio.sleep(sleep_handlers[0]._handler_sec)
        assert t.done() is False
        await simple_flow.stop()
        await asyncio.sleep(0.1)

        assert t.result()
        assert simple_flow.state == FlowState.STOPPED

    async def test_stop_abort_task(self, sleep_handlers, simple_flow, task):
        """Checks that task will be aborted after Flow stop command without graceful mode."""
        t = asyncio.ensure_future(simple_flow.process(task))
        await asyncio.sleep(sleep_handlers[0]._handler_sec)
        await simple_flow.stop(graceful=False)
        await asyncio.sleep(0.1)

        assert isinstance(t.exception(), FlowError)
        assert simple_flow.state == FlowState.STOPPED

    async def test_process_performance_without_batching(self, flow_with_not_batch_handler, tasks_batch):
        """Checks that will be received TimeoutError with not batch handler."""
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                process_tasks(flow_with_not_batch_handler, tasks_batch),
                timeout=CatDetector.BATCH_PROCESS_TIME,
            )

    async def test_process_performance_with_batching(self, flow_with_batch_handler, tasks_batch):
        """Checks that all tasks will be processed on time with batch handler."""
        await asyncio.wait_for(
            process_tasks(flow_with_batch_handler, tasks_batch),
            timeout=CatDetector.BATCH_PROCESS_TIME,
        )
        assert all(task.result for task in tasks_batch)

    async def test_process_seq_batch_filling(self, tasks_batch, flow_with_multiproc_bathcer):
        """Checks that batches are being filled sequentially."""
        await asyncio.gather(*[flow_with_multiproc_bathcer.process(task) for task in tasks_batch])
        assert len(set(task.result for task in tasks_batch)) == 1


async def test_segfault_due_to_update_shared_fields(array, array_sh_data, simple_flow):
    t1, t2 = BaseTask(), BaseTask()
    t1.sh_array = array_sh_data
    t2.sh_array = array_sh_data

    await simple_flow.process(t1)
    await simple_flow.process(t2)

    # segfault during access to t2.sh_array due to updated `_shared_fields` field
    assert all(t2.sh_array == array)
    # segfault during access to t1.sh_array due to updated `sh_array` field
    assert all(t1.sh_array == array)


async def test_segfault_due_to_share_field_in_handler(array, flow_for_shared_array):
    t = BaseTask()
    await flow_for_shared_array.process(t)
    assert t.array[0] == array[-1]  # noqa
