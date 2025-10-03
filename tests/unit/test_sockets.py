import os
import signal

import numpy as np
import pytest

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.multiprocessing import start_processes
from aqueduct.sockets.connection_pool import SocketConnectionPool
from aqueduct.sockets.flow import SocketFlow
from aqueduct.task import BaseTask
from aqueduct.sockets.flow_server import FlowSocketServer


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


async def test_sockets(simple_flow: Flow):
    simple_flow.init_processes(None)
    flow_server = FlowSocketServer(simple_flow)
    flow_server_proc_ctx = start_processes(
        flow_server.start,
        args=(),
        join=False,
        daemon=True,
        start_method='fork',
    )
    pool = SocketConnectionPool()

    result = await pool.handle([Task()])
    assert len(result) == 1
    assert result[0].result == 'done'

    await pool.close()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)


async def test_socket_flow(simple_flow: Flow):
    flow = SocketFlow()
    flow_server_proc_ctx = flow.preload(simple_flow)
    await flow.start()

    task = Task()
    res = await flow.process([task])
    assert res is True
    assert task.result == 'done'

    await flow.stop()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)


async def test_shm_task_sockets(shm_flow: Flow, array):
    shm_flow.init_processes(None)
    flow_server = FlowSocketServer(shm_flow)
    flow_server_proc_ctx = start_processes(
        flow_server.start,
        args=(),
        join=False,
        daemon=True,
        start_method='fork',
    )
    pool = SocketConnectionPool()

    result = await pool.handle([ArrayFieldTask(array)])
    assert len(result) == 1
    assert result[0].array[0] == 2

    await pool.close()
    for p in flow_server_proc_ctx.processes:
        os.kill(p.pid, signal.SIGKILL)
