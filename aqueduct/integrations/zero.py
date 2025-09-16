from abc import ABC, abstractmethod

from aqueduct.sockets.connection_pool import SocketConnectionPool

class BaseFlowClient(ABC):
    def __init__(self, connection_pool: SocketConnectionPool) -> None:
        self._pool = connection_pool

    async def connect(self) -> None:
        await self._pool.init()

    async def disconnect(self) -> None:
        await self._pool.close()

    @abstractmethod
    async def process(self, *args, **kwargs) -> None:
        raise NotImplementedError
