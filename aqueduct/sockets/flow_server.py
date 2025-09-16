import asyncio
import os
import pickle
import signal
from abc import ABC, abstractmethod
from contextlib import suppress
from typing import Any, Callable, Optional

from .protocol import SocketProtocol
from ..flow import Flow
from ..logger import log


class FlowSocketServer(SocketProtocol, ABC):
    def __init__(
        self,
        build_task: Callable[[Any], Any],
        extract_result: Callable[[Any], Any],
        socket_path: str = '/tmp/flow.sock',
        process_timeout_sec: float = 1,
        connection_idle_timeout_sec: int = 900,
    ) -> None:
        self._build_task = build_task
        self._extract_result = extract_result
        self._process_timeout_sec = process_timeout_sec
        self._connection_idle_timeout_sec = connection_idle_timeout_sec

        self._socket_path = socket_path
        self._server: Optional[asyncio.base_events.Server] = None
        self._flow: Optional[Flow] = None
        self._closing = asyncio.Event()

    def start(self) -> None:
        asyncio.run(self._start())

    async def _start(self) -> None:
        with suppress(FileNotFoundError):
            os.unlink(self._socket_path)

        self._flow = await self._build_flow()
        assert self._flow is not None
        self._flow.start()
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=self._socket_path,
            backlog=4096,
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

    @abstractmethod
    async def _build_flow(self) -> Flow:
        pass

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
                    tasks = self._build_task(payload)
                    assert self._flow
                    try:
                        await asyncio.gather(
                            *(
                                self._flow.process(task, timeout_sec=self._process_timeout_sec)
                                for task in tasks
                            ),
                            return_exceptions=True,
                        )
                        resp = {'ok': True, 'result': self._extract_result(tasks)}
                    except Exception:
                        log.exception('Exception while processing task')
                        resp = {'ok': False, 'error': 'process error'}
                except asyncio.TimeoutError:
                    break  # idle connection
                except asyncio.IncompleteReadError:
                    break  # connection closed by client
                except Exception:
                    log.exception('Exception while handling client connection')
                    resp = {'ok': False, 'error': 'socket error'}
                try:
                    await self._write_msg(
                        writer, pickle.dumps(resp, protocol=pickle.HIGHEST_PROTOCOL)
                    )
                except Exception:
                    break
        finally:
            with suppress(Exception):
                writer.close()
                await writer.wait_closed()
