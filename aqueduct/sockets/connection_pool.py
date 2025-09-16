import asyncio
import pickle
from typing import Optional

from .protocol import SocketProtocol
from ..logger import log

RETRYABLE_EXCEPTIONS = (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError)


class SocketConnectionPool(SocketProtocol):
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
        self._pool: asyncio.Queue[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
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
                if rw := await self._open_connection(self._connect_timeout):
                    await self._pool.put(rw)
            self._inited = True

    async def close(self) -> None:
        while not self._pool.empty():
            await self._close_connection(await self._pool.get())

    async def _open_connection(
        self,
        timeout: float,
    ) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        try:
            return await asyncio.wait_for(
                asyncio.open_unix_connection(self._path),
                timeout=timeout,
            )
        except Exception:
            log.info('failed to open connection')
            return None

    async def _close_connection(
        self,
        rw: tuple[asyncio.StreamReader, asyncio.StreamWriter],
    ) -> None:
        _, writer = rw
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


    async def handle(self, data: dict) -> Optional[dict]:
        tries = 0
        while tries < self._connection_retries:
            try:
                return await self._handle(data)
            except RETRYABLE_EXCEPTIONS:
                tries += 1
            except Exception:
                log.exception('connection pool broken')
                raise
        return None

    async def _handle(self, data: dict) -> Optional[dict]:
        rw = await self._acquire_connection()
        if rw is None:
            log.warning('connection pool is empty')
            return None
        reader, writer = rw
        broken = False
        try:
            await self._write_msg(writer, pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL))

            resp = await asyncio.wait_for(self._read_msg(reader), timeout=self._read_timeout)
            return pickle.loads(resp)
        except Exception:
            broken = True
            raise
        finally:
            await self._release_connection(rw, broken=broken)

    async def _acquire_connection(
        self,
    ) -> Optional[tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        try:
            return await asyncio.wait_for(self._pool.get(), timeout=self._connect_timeout)
        except asyncio.TimeoutError:
            return await self._open_connection(self._connect_timeout)

    async def _release_connection(
        self,
        rw: tuple[asyncio.StreamReader, asyncio.StreamWriter],
        broken: bool = False,
    ) -> None:
        if broken or rw[1].is_closing():
            await self._close_connection(rw)
        else:
            await self._pool.put(rw)
