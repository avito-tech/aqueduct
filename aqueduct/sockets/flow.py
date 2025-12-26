import asyncio
import os
import sys
from typing import List, Optional

from .connection_pool import SocketConnectionPool
from .flow_server import FlowSocketServer
from ..flow import Flow
from ..multiprocessing import ProcessContext, start_processes
from ..task import BaseTask
from ..utils import get_env_with_prefix


if sys.version_info < (3, 10):
    raise ImportError('aqueduct socket extension requires Python >= 3.10')


class SocketFlow:
    def __init__(self, name: str = '') -> None:
        self._name: str = name
        self._pool: Optional[SocketConnectionPool] = None

    def preload(self, flow: Flow, timeout: Optional[int] = None) -> ProcessContext:
        """Initialize socket server process with given Flow instance."""
        if not flow.are_steps_initialized:
            flow.init_processes(timeout)
        flow_server = FlowSocketServer(
            flow=flow,
            socket_path=os.getenv(self._env_name('SOCKET_PATH'), default='/tmp/flow.sock'),
            process_timeout_sec=int(os.getenv(self._env_name('FLOW_PROCESS_TIMEOUT'), default=1)),
            connection_idle_timeout_sec=int(os.getenv(self._env_name('SOCKET_SERVER_IDLE_TIMEOUT'), default=900)),
            backlog_size=int(os.getenv(self._env_name('SOCKET_SERVER_BACKLOG_SIZE'), default=4096)),
        )
        return start_processes(
            flow_server.start,
            args=(),
            join=False,
            daemon=True,
            start_method='fork',
        )

    async def start(self) -> None:
        """Initialize socket connection pool."""
        self._pool = SocketConnectionPool(
            socket_path=os.getenv(self._env_name('SOCKET_PATH'), default='/tmp/flow.sock'),
            size=int(os.getenv(self._env_name('SOCKET_POOL_SIZE'), default=10)),
            connect_timeout_sec=float(os.getenv(self._env_name('SOCKET_POOL_CONNECT_TIMEOUT'), default=0.1)),
            read_timeout_sec=int(os.getenv(self._env_name(f'SOCKET_POOL_READ_TIMEOUT'), default=1)),
            connection_retries=int(os.getenv(self._env_name(f'SOCKET_POOL_CONNECTION_RETRIES'), default=1)),
        )
        await self._pool.init()

    async def stop(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    async def process(self, tasks: List[BaseTask], timeout_sec: float = 5.) -> bool:
        if self._pool is None:
            raise RuntimeError('Connection pool is not initialized. Call SocketFlow.start method')
        result_tasks = await asyncio.wait_for(
            self._pool.handle(tasks),
            timeout=timeout_sec,
        )
        for task, result_task in zip(tasks, result_tasks):
            task.update(result_task)
        return True


    def _env_name(self, env: str) -> str:
        return get_env_with_prefix(self._name, env)
