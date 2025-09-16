import asyncio
from multiprocessing import Process

import pytest

from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.sockets.connection_pool import SocketConnectionPool
from aqueduct.task import BaseTask
from aqueduct.sockets.flow_server import FlowSocketServer


class Task(BaseTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None


class SimpleHandler(BaseTaskHandler):
    def handle(self, *tasks: Task):
        for task in tasks:
            task.result = 'done'


class TestServer(FlowSocketServer):
    async def _build_flow(self) -> Flow:
        return Flow(FlowStep(SimpleHandler()))


@pytest.fixture
def flow_server() -> TestServer:
    return TestServer(
        build_task=lambda data: [Task()],
        extract_result=lambda tasks: {'result_data': tasks[0].result},
    )


async def test_sockets(flow_server: TestServer):
    flow_server_proc = Process(target=flow_server.start, daemon=False)
    flow_server_proc.start()
    await asyncio.sleep(0.5)
    pool = SocketConnectionPool()
    result = await pool.handle('some data')
    assert result == {'ok': True, 'result': {'result_data': 'done'}}
    flow_server_proc.terminate()
