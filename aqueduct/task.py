import uuid
from time import monotonic
from typing import Sequence, Set, Union

from .metrics.task import TaskMetrics
from .shm import SharedFieldsMixin


class BaseTask(SharedFieldsMixin):
    task_id: Union[str, Sequence[str]] = ''
    expiration_time = None
    metrics: TaskMetrics = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_id = str(uuid.uuid4())
        self.metrics = TaskMetrics()

    def __repr__(self):
        name = self.__class__.__name__
        attrs = self.__dict__
        return f'<{name} {attrs}>'

    def __str__(self):
        return self.__repr__()

    def set_timeout(self, timeout_sec: float):
        self.expiration_time = monotonic() + timeout_sec

    def is_expired(self) -> bool:
        return monotonic() >= self.expiration_time

    def _update_shared_fields(self, source_task: 'BaseTask') -> Set[str]:
        """Updates task shared fields and returns shared fields names.

        We shouldn't update existing shared fields and we use the overridden
        in SharedFieldsMixin `setattr` method to set SharedData values from source task.
        """
        for field, value in source_task._shared_fields.items():
            if field not in self._shared_fields:
                setattr(self, field, value)

        return {'_shared_fields'} | self._shared_fields.keys() | source_task._shared_fields.keys()

    def update(self, source_task: 'BaseTask', excluded_fields: Set[str] = None):
        """Sets data from source task.

        This is necessary in order to set the values from finished task to initial task
        in `process` Flow method.
        """
        immutable_fields = BaseTask.__dict__.keys()
        if excluded_fields:
            immutable_fields = immutable_fields | excluded_fields

        immutable_fields |= self._update_shared_fields(source_task)

        for field, value in source_task.__dict__.items():
            if field not in immutable_fields:
                setattr(self, field, value)


class StopTask(BaseTask):
    """Task for graceful shutdown."""
