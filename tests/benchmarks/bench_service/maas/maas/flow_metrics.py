import os
from typing import (
    Optional,
    Tuple,
    Union,
)

from aiodogstatsd import Client
from aiohttp import web
from aqueduct.metrics import ToStatsDMetricsExporter


class StatsDMetricsBuffer:
    def __init__(self, client: Client):
        self._client = client

    def count(self, name: str, value: Union[float, int]):
        self._client.increment(name, value=value)

    def timing(self, name: str, value: Union[float, int]):
        self._client.timing(name, value=value)

    def gauge(self, name: str, value: Union[float, int]):
        self._client.gauge(name, value=value)


def get_statsd_client() -> Optional[Client]:
    if not os.getenv('METRICS_ENABLED'):
        return

    metrics_cfg = dict(
        host=os.getenv('METRICS_HOST', 'localhost'),
        port=os.getenv('METRICS_PORT', 8125),
        namespace=os.getenv('METRICS_PREFIX'),
        pending_queue_size=10 ** 3,
    )
    return Client(**metrics_cfg)


def get_metrics_exporter(client: Client) -> ToStatsDMetricsExporter:
    return ToStatsDMetricsExporter(StatsDMetricsBuffer(client))


async def connect_statsd_client(app: web.Application):
    await app['statsd_client'].connect()


async def close_statsd_client(app: web.Application):
    await app['statsd_client'].close()
