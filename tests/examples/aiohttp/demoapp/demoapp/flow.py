import asyncio
import sys
from typing import (
    List,
    Optional,
    Tuple,
)

import torch
from aiohttp import web
from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.logger import log
from aqueduct.task import BaseTask

from .pipeline import Classifier, default_producer


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


async def observe_flow(app):
    asyncio.ensure_future(check_flow_state(app))


async def stop_flow(app):
    flow: Flow = app['flow']
    await flow.stop()


async def check_flow_state(app: web.Application, check_interval: float = 1.0):
    """Следит за состоянием Flow и завершает работу сервиса, если Flow не запущен."""
    flow: Flow = app['flow']
    while flow.is_running:
        await asyncio.sleep(check_interval)
    log.info('Flow is not running, application will be stopped')
    await app.shutdown()
    await app.cleanup()
    sys.exit(1)
