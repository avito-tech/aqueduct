from abc import ABC, abstractmethod
from typing import (
    Any,
    Optional,
    Protocol,
    Union,
)

from . import AQUEDUCT
from .collect import AqueductMetricsStorage

TRANSFER_TIME_PREFIX = 'transfer_time'
HANDLE_TIME_PREFIX = 'handle_time'
BATCH_TIME_PREFIX = 'batch_time'
BATCH_SIZE_PREFIX = 'batch_size'
QSIZE_PREFIX = 'qsize'
TASKS_PREFIX = 'tasks'


class StatsDBuffer(Protocol):
    def count(self, name: str, value: Union[float, int]):
        pass

    def timing(self, name: str, value: Union[float, int]):
        pass

    def gauge(self, name: str, value: Union[float, int]):
        pass


class Exporter(ABC):
    def __init__(self, target: Any, export_period: float = 10., prefix: str = None):
        self.target = target
        self.export_period = export_period
        if prefix is None:
            self.prefix = AQUEDUCT
        else:
            self.prefix = f'{AQUEDUCT}.{prefix}'

    @abstractmethod
    def export(self, metrics: AqueductMetricsStorage):
        pass


class ToStatsDMetricsExporter(Exporter):
    """Translates aqueduct metrics to statsd metrics format."""
    def __init__(self, target: StatsDBuffer, export_period: float = 10., prefix: str = None):
        super().__init__(target, export_period, prefix)

    def export(self, metrics: AqueductMetricsStorage):
        for name, seconds in metrics.handle_times.items:
            self.target.timing(f'{self.prefix}.{HANDLE_TIME_PREFIX}.{name}', seconds * 1000)

        for name, seconds in metrics.transfer_times.items:
            self.target.timing(f'{self.prefix}.{TRANSFER_TIME_PREFIX}.{name}', seconds * 1000)

        for name, seconds in metrics.batch_times.items:
            self.target.timing(f'{self.prefix}.{BATCH_TIME_PREFIX}.{name}', seconds * 1000)

        for name, size in metrics.batch_sizes.items:
            self.target.timing(f'{self.prefix}.{BATCH_SIZE_PREFIX}.{name}', size)

        for name, size in metrics.queue_sizes.items:
            self.target.timing(f'{self.prefix}.{QSIZE_PREFIX}.{name}', size)

        for name, cnt in metrics.tasks_stats.items:
            if cnt > 0:
                self.target.count(f'{self.prefix}.{TASKS_PREFIX}.{name}', cnt)


class DummyExporter(Exporter):
    """Exports collected aqueduct metrics as is."""
    def __init__(self, target: Optional[AqueductMetricsStorage] = None, **kwargs):
        if target is None:
            target = AqueductMetricsStorage()
        super().__init__(target, **kwargs)

    def export(self, metrics: AqueductMetricsStorage):
        self.target.extend(metrics)
