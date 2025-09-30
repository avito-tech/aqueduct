import os
from abc import ABC, abstractmethod

from aqueduct.sockets.connection_pool import SocketConnectionPool

class BaseFlowClient(ABC):
    def __init__(self) -> None:
        self._pool = SocketConnectionPool(
            socket_path=os.getenv('SOCKET_PATH', default='/tmp/flow.sock'),
            size=int(os.getenv('SOCKET_POOL_SIZE', default=10)),
            connect_timeout_sec=float(os.getenv('SOCKET_POOL_CONNECT_TIMEOUT', default=0.1)),
            read_timeout_sec=int(os.getenv('SOCKET_POOL_READ_TIMEOUT', default=1)),
            connection_retries=int(os.getenv('SOCKET_POOL_CONNECTION_RETRIES', default=1)),
        )

    async def connect(self) -> None:
        await self._pool.init()

    async def disconnect(self) -> None:
        await self._pool.close()

    @abstractmethod
    async def process(self, *args, **kwargs) -> None:
        raise NotImplementedError
