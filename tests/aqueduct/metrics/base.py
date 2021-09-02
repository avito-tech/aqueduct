from enum import Enum
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Tuple

MAIN_PROCESS = 'main_process'

AQUEDUCT = 'aqueduct'


class MetricsTypes(Enum):
    TASK_TIMERS = 'task_timers'
    QUEUE_SIZE = 'queue_size'


class IExtendable(ABC):
    @abstractmethod
    def extend(self, obj):
        """Extends self object by object of the same type."""


class IMetricsItems(IExtendable):
    @property
    @abstractmethod
    def items(self) -> Iterable[Tuple[str, Any]]:
        """Returns metrics as set of name and value pairs."""


class MetricsItems(IMetricsItems):
    """Stores metrics as name and value pairs."""
    def __init__(self, items: List[Tuple[str, Any]] = None):
        self._items: List[Tuple[str, Any]] = items or []

    @property
    def items(self) -> List[Tuple[str, Any]]:
        return self._items

    def add(self, name: str, value: Any):
        self._items.append((name, value))

    def add_items(self, items: List[Tuple[str, Any]]):
        self._items.extend(items)

    def extend(self, metrics: 'MetricsItems'):
        self.add_items(metrics.items)
