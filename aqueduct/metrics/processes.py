from dataclasses import asdict, dataclass
from typing import Iterable, Tuple

from . import IMetricsItems


@dataclass
class ProcessesStats(IMetricsItems):
    dead: int = 0
    running: int = 0

    @property
    def items(self) -> Iterable[Tuple[str, int]]:
        return asdict(self).items()

    def extend(self, stats: 'ProcessesStats'):
        self.dead += stats.dead
        self.running += stats.running

    def add_dead_process(self):
        self.dead += 1

    def add_running_process(self):
        self.running += 1
