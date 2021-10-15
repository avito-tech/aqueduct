import asyncio
from collections import deque
from typing import (
    List,
    Tuple,
    Union,
)

import pytest

from aqueduct.flow import (
    Flow,
    FlowStep,
)
from aqueduct.metrics.collect import AqueductMetricsStorage
from aqueduct.metrics.export import (
    AQUEDUCT,
    BATCH_SIZE_PREFIX,
    BATCH_TIME_PREFIX,
    DummyExporter,
    HANDLE_TIME_PREFIX,
    QSIZE_PREFIX,
    TASKS_PREFIX,
    TRANSFER_TIME_PREFIX,
    ToStatsDMetricsExporter,
)
from tests.unit.conftest import (
    SleepHandler,
    Task,
    run_flow,
)

EXPORT_PERIOD = 0.001


class StatsDMetricsBuffer:
    def __init__(self):
        self._data = deque()
        self._size = 0

    @property
    def data(self) -> List[bytes]:
        return list(self._data)

    def _format(self, name: Union[str, bytes], value: Union[int, float], _type: Union[str, bytes]) -> bytes:
        name = name if isinstance(name, bytes) else name.encode('utf8')
        _type = _type if isinstance(_type, bytes) else _type.encode('utf8')
        return b'%s:%d|%s' % (name, int(value), _type)

    def _set_metric(self, name: Union[str, bytes], value: Union[int, float], _type: Union[str, bytes]):
        data = self._format(name=name, value=value, _type=_type)
        self._data.append(data)
        self._size += len(data)

    def timing(self, name, seconds: Union[float, int]):
        self._set_metric(name, seconds, b'ms')

    def count(self, name, value):
        self._set_metric(name, value, b'c')

    def gauge(self, name, value):
        self._set_metric(name, value, b'g')


@pytest.fixture
def statsd_metrics_buffer() -> StatsDMetricsBuffer:
    return StatsDMetricsBuffer()


@pytest.fixture
def to_statsd_metrics_exporter(statsd_metrics_buffer) -> ToStatsDMetricsExporter:
    return ToStatsDMetricsExporter(statsd_metrics_buffer, export_period=EXPORT_PERIOD)


@pytest.fixture
async def flow_with_exporter(loop, sleep_handlers: Tuple[SleepHandler, ...],
                             to_statsd_metrics_exporter) -> Flow:
    async with run_flow(Flow(*sleep_handlers, metrics_exporter=to_statsd_metrics_exporter)) as flow:
        yield flow


@pytest.fixture
def dummy_metrics_exporter() -> DummyExporter:
    return DummyExporter(export_period=EXPORT_PERIOD)


@pytest.fixture
async def flow_without_metrics(loop, sleep_handlers: Tuple[SleepHandler, ...]) -> Flow:
    async with run_flow(Flow(*sleep_handlers, metrics_enabled=False)) as flow:
        yield flow


@pytest.fixture
async def flow_with_batching_to_statsd(loop, to_statsd_metrics_exporter) -> Flow:
    async with run_flow(
            Flow(
                FlowStep(SleepHandler(0.001), batch_size=4, batch_timeout=0.05),
                metrics_exporter=to_statsd_metrics_exporter,
            )) as flow:
        yield flow


@pytest.fixture
async def flow_with_batching(loop, dummy_metrics_exporter) -> Flow:
    async with run_flow(
            Flow(
                FlowStep(SleepHandler(0.001), batch_size=4, batch_timeout=0.05),
                metrics_exporter=dummy_metrics_exporter,
            )) as flow:
        yield flow


@pytest.fixture
async def tasks() -> List[Task]:
    return [Task() for _ in range(10)]


class TestToAvioMetricsExporter:
    async def test_export(
            self,
            sleep_handlers,
            statsd_metrics_buffer,
            flow_with_exporter: Flow,
            task,
    ):
        await flow_with_exporter.process(task)
        # wait for metrics export
        await asyncio.sleep(2 * EXPORT_PERIOD)

        metrics = [m.decode() for m in statsd_metrics_buffer.data]
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{HANDLE_TIME_PREFIX}')) == 4
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{TRANSFER_TIME_PREFIX}')) == 4
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{BATCH_TIME_PREFIX}')) == 0
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{BATCH_SIZE_PREFIX}')) == 0
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{QSIZE_PREFIX}')) == 4
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{TASKS_PREFIX}')) == 1

    async def test_export_batch_metrics_for_avio(
            self,
            flow_with_batching_to_statsd,
            tasks: List[Task],
            statsd_metrics_buffer,
    ):
        await asyncio.gather(*[flow_with_batching_to_statsd.process(task) for task in tasks])
        await asyncio.sleep(2 * EXPORT_PERIOD)

        metrics = [m.decode() for m in statsd_metrics_buffer.data]
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{BATCH_TIME_PREFIX}')) == 3
        assert sum(1 for m in metrics if m.startswith(f'{AQUEDUCT}.{BATCH_SIZE_PREFIX}')) == 3

    async def test_export_batch_metrics(self, dummy_metrics_exporter, flow_with_batching: Flow, tasks: List[Task]):
        await asyncio.gather(*[flow_with_batching.process(task) for task in tasks])
        await asyncio.sleep(2 * EXPORT_PERIOD)

        metrics = dummy_metrics_exporter.target
        t1, t2, t3 = (seconds for name, seconds in metrics.batch_times.items)
        assert t1 == pytest.approx(0.01, 1) and t2 == pytest.approx(0.01, 1) and t3 >= 0.05
        s1, s2, s3 = (sizes for name, sizes in metrics.batch_sizes.items)
        assert s1 == s2 == 4 and s3 == 2

    async def test_export_disabled_metrics(self, sleep_handlers, flow_without_metrics: Flow, task):
        await flow_without_metrics.process(task)
        # wait for metrics export
        await asyncio.sleep(2 * EXPORT_PERIOD)

        metrics: AqueductMetricsStorage = flow_without_metrics._metrics_manager.exporter.target
        assert not metrics.queue_sizes.items
        assert not metrics.transfer_times.items
        assert not metrics.handle_times.items
        assert not metrics.batch_times.items
        assert not metrics.batch_sizes.items
        assert not metrics.tasks_stats.complete
