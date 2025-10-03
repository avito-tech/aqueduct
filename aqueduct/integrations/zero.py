from abc import ABC, abstractmethod

from aqueduct.sockets.flow import SocketFlow


class BaseFlowClient(ABC):
    _name = ''

    def __init__(self) -> None:
        self._flow = SocketFlow(self._name)

    async def connect(self) -> None:
        await self._flow.start()

    async def disconnect(self) -> None:
        await self._flow.stop()

    @abstractmethod
    async def process(self, *args, **kwargs) -> None:
        # define your logic here.
        # Build your custom Task and call self._flow.process() method
        raise NotImplementedError
