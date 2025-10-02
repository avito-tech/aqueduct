import asyncio
import os
import signal
from multiprocessing import Process

import pytest

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.sockets.connection_pool import SocketConnectionPool
from aqueduct.task import BaseTask
from aqueduct.sockets.flow_server import BaseFlowBuilder, FlowSocketServer


class Task(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None


class SimpleHandler(BaseTaskHandler):
    def handle(self, *tasks: Task):
        for task in tasks:
            task.result = 'done'


class FlowBuilder(BaseFlowBuilder):
    async def build_flow(self) -> Flow:
        return Flow(FlowStep(SimpleHandler()))


@pytest.fixture
def flow_server() -> FlowSocketServer:
    return FlowSocketServer(
        flow_builder=FlowBuilder(),
    )


async def test_sockets(flow_server: FlowSocketServer):
    flow_server_proc = Process(target=flow_server.start, daemon=False)
    flow_server_proc.start()
    await asyncio.sleep(0.5)
    pool = SocketConnectionPool()
    result = await pool.send([Task()])
    os.kill(flow_server_proc.pid, signal.SIGINT)
    assert result.ok == True
    assert len(result.result) == 1
    assert result.result[0].result == 'done'
