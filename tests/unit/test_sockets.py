import asyncio
from multiprocessing import Process
from typing import Any

import pytest

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.sockets.connection_pool import SocketConnectionPool
from aqueduct.sockets.protocol import SocketResponse
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

    def build_tasks(self, payload: Any) -> list[BaseTask]:
        return [Task()]

    def extract_result(self, tasks: list[BaseTask]) -> Any:
        return {'result_data': tasks[0].result}


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
    result = await pool.handle('some data')
    assert result == SocketResponse(ok= True, result={'result_data': 'done'})
    flow_server_proc.terminate()
