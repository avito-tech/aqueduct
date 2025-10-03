from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass, field
from typing import Optional

from ..task import BaseTask


@dataclass
class SocketResponse:
    ok: bool = False
    result: list[BaseTask] = field(default_factory=list)
    error: Optional[str] = None


class SocketProtocol:
    max_payload_bytes = 16 * 1024 * 1024

    async def _read_msg(self, reader: StreamReader) -> bytes:
        raw_len = await reader.readexactly(4)
        n = int.from_bytes(raw_len, 'big')
        if self.max_payload_bytes is not None and n > self.max_payload_bytes:
            raise ValueError('Payload too large')
        return await reader.readexactly(n)

    async def _write_msg(self, writer: StreamWriter, data: bytes) -> None:
        if len(data) > self.max_payload_bytes:
            raise ValueError('Payload too large')
        writer.write(len(data).to_bytes(4, 'big') + data)
        await writer.drain()
