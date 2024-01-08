from typing import Callable
from .task import BaseTask


HandleConditionType = Callable[[BaseTask], bool]


class BaseTaskHandler:
    """The base class for your handlers.
       It is used for logic that does not require a model and special initialization.
       If you need a model that starts for a long time and takes up memory, then use  ModelTaskHandler()."""

    @classmethod
    def get_step_name(cls, step_number: int) -> str:
        return f'step{step_number}_{cls.__name__}'

    def on_start(self):
        """Called at startup in a child process.
        So that the model takes up memory only in the subprocess, not in the parent."""
        pass

    def handle(self, *tasks: BaseTask):
        """Called when a task is received from the queue."""
        raise NotImplementedError
