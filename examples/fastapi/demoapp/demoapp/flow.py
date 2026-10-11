from typing import (
    List,
    Optional,
    Tuple,
)

import torch

from aqueduct import (
    BaseTask,
    BaseTaskHandler,
    Flow,
    FlowStep,
)
from .pipeline import (
    Classifier,
    default_producer,
)


class Task(BaseTask):
    def __init__(
            self,
            image: bytes,
    ):
        super().__init__()
        self.image: Optional[bytes, torch.Tensor] = image
        self.pred: Optional[torch.Tensor] = None
        self.h_pred: Optional[List[Tuple[str, float]]] = None


class PreProcessorHandler(BaseTaskHandler):
    def __init__(self):
        self._model = default_producer.get_pre_proc()

    def handle(self, *tasks: Task):
        for task in tasks:
            task.image = self._model.process(task.image)


class ClassifierHandler(BaseTaskHandler):
    def __init__(self, max_batch_size: int = 1):
        self._model: Optional[Classifier] = None
        self.max_batch_size = max_batch_size

    def on_start(self):
        self._model = default_producer.get_classifier()

    def handle(self, *tasks: Task):
        preds = self._model.process_list(data=[task.image for task in tasks])
        for pred, task in zip(preds, tasks):
            task.pred = pred
            task.image = None


class PostProcessorHandler(BaseTaskHandler):
    def __init__(self):
        self._model = default_producer.get_post_proc()

    def handle(self, *tasks: Task):
        for task in tasks:
            task.h_pred = self._model.process(task.pred)


def get_flow() -> Flow:
    return Flow(
        FlowStep(PreProcessorHandler()),
        FlowStep(ClassifierHandler()),
        FlowStep(PostProcessorHandler()),
        metrics_enabled=False,
    )
