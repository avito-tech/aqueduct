import os
import signal

import numpy as np
import pytest

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.multiprocessing import start_processes
from aqueduct.task import BaseTask
from tests.conftest import only_py310


flow_module = pytest.importorskip(
    'aqueduct.sockets.flow',
    reason='aqueduct.sockets.flow requires Python 3.10+',
)
connection_pool_module = pytest.importorskip(
    'aqueduct.sockets.connection_pool',
    reason='aqueduct.sockets.connection_pool requires Python 3.10+',
)
flow_server_module = pytest.importorskip(
    'aqueduct.sockets.flow_server',
    reason='aqueduct.sockets.flow_server requires Python 3.10+',
)


class Task(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None


class ArrayFieldTask(Task):
    def __init__(self, array: np.array, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.array = array


class SimpleHandler(BaseTaskHandler):
    def handle(self, *tasks: Task):
        for task in tasks:
            task.result = 'done'


class ChangeFieldHandler(BaseTaskHandler):
    def handle(self, *tasks: ArrayFieldTask):
        for task in tasks:
            task.array[0] = 2


@pytest.fixture
def simple_flow() -> Flow:
    return Flow(FlowStep(SimpleHandler()))


@pytest.fixture
def shm_flow() -> Flow:
    return Flow(FlowStep(ChangeFieldHandler()))


@only_py310
async def test_sockets(simple_flow: Flow):
    simple_flow.init_processes()
    flow_server = flow_server_module.FlowSocketServer(simple_flow)
    flow_server_proc_ctx = start_processes(
        flow_server.start,
        args=(),
        join=False,
        daemon=True,
        start_method='fork',
    )
    pool = connection_pool_module.SocketConnectionPool()

    result = await pool.handle([Task()])
    assert len(result) == 1
    assert result[0].result == 'done'

    await pool.close()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)


@only_py310
async def test_shm_task_sockets(shm_flow: Flow, array):
    shm_flow.init_processes()
    flow_server = flow_server_module.FlowSocketServer(shm_flow)
    flow_server_proc_ctx = start_processes(
        flow_server.start,
        args=(),
        join=False,
        daemon=True,
        start_method='fork',
    )
    pool = connection_pool_module.SocketConnectionPool()
    task = ArrayFieldTask(array)
    task.share_value('array')

    result = await pool.handle([task])
    assert len(result) == 1
    assert result[0].array[0] == 2

    await pool.close()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)


@only_py310
async def test_socket_flow(simple_flow: Flow):
    flow = flow_module.SocketFlow()
    flow_server_proc_ctx = flow.preload(simple_flow)
    await flow.start()

    task = Task()
    res = await flow.process([task])
    assert res is True
    assert task.result == 'done'

    await flow.stop()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)
