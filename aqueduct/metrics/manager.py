import asyncio
import multiprocessing as mp
from typing import Dict, List

from .base import MetricsTypes
from .collect import Collector
from .export import DummyExporter, Exporter


class MetricsManager:
    def __init__(self, collector: Collector, exporter: Exporter):
        self.collector = collector
        self.exporter = exporter
        self._tasks: List[asyncio.Future] = []

    def start(self, queues_info: Dict[mp.Queue, str]):
        if queues_info and self.collector.is_collectible(MetricsTypes.QUEUE_SIZE):
            self._tasks.append(asyncio.ensure_future(self.collector.collect_qsize(queues_info)))
        self._tasks.append(asyncio.ensure_future(self._export_metrics()))

    def stop(self):
        # Остановка фоновых корутин
        for task in self._tasks:
            task.cancel()

    async def _export_metrics(self):
        while True:
            self.exporter.export(self.collector.extract_metrics())
            await asyncio.sleep(self.exporter.export_period)


def get_metrics_manager(collector: Collector = None, exporter: Exporter = None) -> MetricsManager:
    if collector is None:
        collector = Collector()
    if exporter is None:
        exporter = DummyExporter()
    return MetricsManager(collector, exporter)
