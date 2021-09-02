import asyncio
from dataclasses import asdict, dataclass
from multiprocessing import Queue
from typing import Dict, Iterable, Tuple

from . import IMetricsItems
from .base import MetricsItems, MetricsTypes
from .task import TasksMetricsStorage


@dataclass
class TasksStats(IMetricsItems):
    cancel: int = 0
    timeout: int = 0
    complete: int = 0

    @property
    def items(self) -> Iterable[Tuple[str, int]]:
        return asdict(self).items()

    def extend(self, stats: 'TasksStats'):
        self.cancel += stats.cancel
        self.timeout += stats.timeout
        self.complete += stats.complete


class AqueductMetricsStorage(TasksMetricsStorage):
    def __init__(self):
        super().__init__()
        self.queue_sizes = MetricsItems()
        self.tasks_stats = TasksStats()

    def extend(self, storage: TasksMetricsStorage):
        super().extend(storage)
        if isinstance(storage, AqueductMetricsStorage):
            self.queue_sizes.extend(storage.queue_sizes)
            self.tasks_stats.extend(storage.tasks_stats)


class Collector:
    def __init__(self, collectible_metrics: Iterable[MetricsTypes] = None,
                 qsize_collect_period: float = 1.):
        self._metrics = AqueductMetricsStorage()
        self._collectible_metrics = [MetricsTypes(t) for t in collectible_metrics] \
            if collectible_metrics is not None else list(MetricsTypes)

        self._qsize_collect_period: float = qsize_collect_period

    def is_collectible(self, metrics_type: MetricsTypes):
        return metrics_type in self._collectible_metrics

    async def collect_qsize(self, queues_info: Dict[Queue, str]):
        if not self.is_collectible(MetricsTypes.QUEUE_SIZE):
            return
        while True:
            queues_sizes = [(name, q.qsize()) for q, name in queues_info.items()]
            self._metrics.queue_sizes.add_items(queues_sizes)
            await asyncio.sleep(self._qsize_collect_period)

    def add_task_metrics(self, metrics: TasksMetricsStorage):
        self._metrics.extend(metrics)

    def add_tasks_stats(self, stats: TasksStats):
        self._metrics.tasks_stats.extend(stats)

    def extract_metrics(self) -> AqueductMetricsStorage:
        metrics = self._metrics
        self._metrics = AqueductMetricsStorage()
        return metrics
