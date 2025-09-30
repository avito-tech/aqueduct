import asyncio
import os
import pickle
import signal
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Any, Optional

from .protocol import SocketProtocol, SocketResponse
from ..flow import Flow
from ..logger import log
from ..task import BaseTask


class BaseFlowBuilder(ABC):
    @abstractmethod
    async def build_flow(self) -> Flow:
        """Factory method to build Flow for socket server."""
        pass

    @abstractmethod
    def build_tasks(self, payload: Any) -> list[BaseTask]:
        """Factory method to build user defined Tasks for socket server Flow."""
        pass

    @abstractmethod
    def extract_result(self, tasks: list[BaseTask]) -> Any:
        """Factory method to extract result from user defined Tasks."""
        pass


class FlowSocketServer(SocketProtocol):
    def __init__(
        self,
        flow_builder: BaseFlowBuilder,
        socket_path: str = '/tmp/flow.sock',
        process_timeout_sec: float = 1,
        connection_idle_timeout_sec: int = 900,
        backlog_size: int = 4096,
    ) -> None:
        self._flow_builder = flow_builder
        self._process_timeout_sec = process_timeout_sec
        self._connection_idle_timeout_sec = connection_idle_timeout_sec
        self._backlog_size = backlog_size

        self._socket_path = socket_path
        self._server: Optional[asyncio.base_events.Server] = None
        self._flow: Optional[Flow] = None
        self._closing = asyncio.Event()

    def start(self) -> None:
        asyncio.run(self._start())

    async def _start(self) -> None:
        with suppress(FileNotFoundError):
            os.unlink(self._socket_path)

        self._flow = await self._flow_builder.build_flow()
        assert self._flow is not None
        self._flow.start()
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=self._socket_path,
            backlog=self._backlog_size,
        )

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._closing.set)

        async with self._server:
            await self._closing.wait()

    async def close(self) -> None:
        self._closing.set()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        await self._flow.stop()
        with suppress(FileNotFoundError):
            os.unlink(self._socket_path)

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while not self._closing.is_set():
                try:
                    payload_bytes = await asyncio.wait_for(
                        self._read_msg(reader),
                        timeout=self._connection_idle_timeout_sec,
                    )
                    payload = pickle.loads(payload_bytes)
                    tasks = self._flow_builder.build_tasks(payload)
                    assert self._flow
                    try:
                        await asyncio.gather(
                            *(
                                self._flow.process(task, timeout_sec=self._process_timeout_sec)
                                for task in tasks
                            ),
                            return_exceptions=True,
                        )
                        resp = SocketResponse(
                            ok=True,
                            result=self._flow_builder.extract_result(tasks),
                        )
                    except Exception:
                        log.exception('Flow error while processing task')
                        resp = SocketResponse(
                            ok=False,
                            error='flow error',
                        )
                except asyncio.TimeoutError:
                    break  # idle connection
                except asyncio.IncompleteReadError:
                    break  # connection closed by client
                except Exception:
                    log.exception('Exception while handling client connection')
                    resp = SocketResponse(
                        ok=False,
                        error='socket error',
                    )
                try:
                    await self._write_msg(
                        writer, pickle.dumps(resp, protocol=pickle.HIGHEST_PROTOCOL)
                    )
                except Exception:
                    log.exception('Server write error')
                    break
        finally:
            with suppress(Exception):
                writer.close()
                await writer.wait_closed()
