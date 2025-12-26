import asyncio
import pickle
import sys
from contextlib import suppress
from typing import List, Tuple

from .flow_server import SOCKET_ERROR, FLOW_ERROR
from .protocol import SocketProtocol, SocketResponse
from .. import BaseTask
from ..exceptions import FlowError
from ..logger import log


if sys.version_info < (3, 10):
    raise ImportError('aqueduct socket extension requires Python >= 3.10')


class OpenConnectionError(Exception):
    pass


class SocketServerError(Exception):
    pass


class SocketConnectionPool(SocketProtocol):
    RETRYABLE_EXCEPTIONS = (
        OpenConnectionError,
        SocketServerError,
        ConnectionResetError,
        BrokenPipeError,
        asyncio.IncompleteReadError,
        asyncio.TimeoutError,
    )

    def __init__(
        self,
        socket_path: str = '/tmp/flow.sock',
        size: int = 10,
        connect_timeout_sec: float = 0.1,
        read_timeout_sec: float = 1,
        connection_retries: int = 1,
    ):
        self._path = socket_path
        self._size = size
        self._connect_timeout = connect_timeout_sec
        self._read_timeout = read_timeout_sec
        self._pool: asyncio.Queue[Tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
            asyncio.Queue()
        )
        self._connection_retries = connection_retries
        self._init_lock = asyncio.Lock()
        self._inited = False

    async def init(self) -> None:
        if self._inited:
            return
        async with self._init_lock:
            for _ in range(self._size):
                # if failed to connect it would connect later lazily
                with suppress(OpenConnectionError):
                    rw = await self._open_connection(self._connect_timeout)
                    await self._pool.put(rw)
            self._inited = True

    async def close(self) -> None:
        while not self._pool.empty():
            await self._close_connection(await self._pool.get())

    async def _open_connection(
        self,
        timeout: float,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(
                asyncio.open_unix_connection(self._path),
                timeout=timeout,
            )
        except Exception as e:
            raise OpenConnectionError('failed to open connection') from e

    async def _close_connection(
        self,
        rw: Tuple[asyncio.StreamReader, asyncio.StreamWriter],
    ) -> None:
        _, writer = rw

        # connections close while shutting down or removing from pool.
        # Idle connections are closed on server side so we can ignore failures
        with suppress(Exception):
            writer.close()
            await writer.wait_closed()


    async def handle(self, data: List[BaseTask]) -> List[BaseTask]:
        tries = 0
        while tries < self._connection_retries:
            try:
                tries += 1
                return await self._handle(data)
            except self.RETRYABLE_EXCEPTIONS:
                if tries == self._connection_retries:
                    log.exception('sending data to socket failed')
                    raise
            except Exception:
                log.exception('connection pool broken')
                raise

    async def _handle(self, data: List[BaseTask]) -> List[BaseTask]:
        rw = await self._acquire_connection()
        reader, writer = rw
        broken = False
        try:
            await self._write_msg(writer, pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL))

            resp = await asyncio.wait_for(self._read_msg(reader), timeout=self._read_timeout)
            resp_model: SocketResponse = pickle.loads(resp)
            if resp_model.error == SOCKET_ERROR:
                raise SocketServerError
            if resp_model.error == FLOW_ERROR:
                raise FlowError
            return resp_model.result
        except Exception:
            # mark connection to be removed from pool
            broken = True
            raise
        finally:
            await self._release_connection(rw, broken=broken)

    async def _acquire_connection(
        self,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(self._pool.get(), timeout=self._connect_timeout)
        except asyncio.TimeoutError:
            return await self._open_connection(self._connect_timeout)

    async def _release_connection(
        self,
        rw: Tuple[asyncio.StreamReader, asyncio.StreamWriter],
        broken: bool = False,
    ) -> None:
        if broken or rw[1].is_closing():
            await self._close_connection(rw)
        else:
            await self._pool.put(rw)
