import asyncio
import logging
import sys
from typing import List, Optional, Tuple

import torch
from aiohttp import web
from aqueduct.flow import Flow, FlowStep
from aqueduct.handler import BaseTaskHandler
from aqueduct.logger import log
from aqueduct.metrics.export import Exporter
from aqueduct.task import BaseTask

from .models import Classifier, default_producer

log.setLevel(logging.INFO)


class Task(BaseTask):
    def __init__(
            self,
            image: bytes,
    ):
        super().__init__()
        self.image: Optional[bytes, torch.Tensor] = image
        self.pred: torch.Tensor = None  # noqa
        self.h_pred: List[Tuple[str, float]] = None  # noqa


class PreProcessorHandler(BaseTaskHandler):
    def __init__(self):
        self.model = default_producer.get_pre_proc()

    def handle(self, *tasks: Task):
        for task in tasks:
            task.image = self.model.process(task.image)


class ClassifierHandler(BaseTaskHandler):
    def __init__(self, max_batch_size: int = 1):
        self.model: Dict[int, Classifier] = None  # noqa
        self.max_batch_size = max_batch_size

    def on_start(self):
        self.model = {}
        for bs in range(1, self.max_batch_size + 1):
            self.model[bs] = default_producer.get_classifier()

    def handle(self, *tasks: Task):
        preds = self.model[len(tasks)].process_list(
            data=[task.image for task in tasks])
        for pred, task in zip(preds, tasks):
            task.pred = pred
            task.image = None


class PostProcessorHandler(BaseTaskHandler):
    def __init__(self):
        self.model = default_producer.get_post_proc()

    def handle(self, *tasks: Task):
        for task in tasks:
            task.h_pred = self.model.process(task.pred)
            task.pred = None


class PipelineHandler(BaseTaskHandler):
    def __init__(self):
        self.pre_proc_model = default_producer.get_pre_proc()
        self.classifier_model: Classifier = None  # noqa
        self.post_proc_model = default_producer.get_post_proc()

    def on_start(self):
        self.classifier_model = default_producer.get_classifier()

    def handle(self, *tasks: Task):
        for task in tasks:
            task.image = self.pre_proc_model.process(task.image)
            task.pred = self.classifier_model.process_list(data=[task.image])[0]
            task.h_pred = self.post_proc_model.process(task.pred)


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


def get_flow(nprocs: List[int], metrics_exporter: Optional[Exporter] = None) -> Flow:
    # todo put batch_size parameter in config
    batch_size = 1
    return Flow(
        FlowStep(PreProcessorHandler(), nprocs=nprocs[0]),
        FlowStep(
            ClassifierHandler(max_batch_size=batch_size),
            nprocs=nprocs[1],
            batch_size=batch_size,
            batch_timeout=0.1,
        ),
        FlowStep(PostProcessorHandler(), nprocs=nprocs[2]),
        metrics_exporter=metrics_exporter,
    )


def get_1step_flow(nprocs: int, metrics_exporter: Exporter = None) -> Flow:
    return Flow(
        FlowStep(PipelineHandler(), nprocs=nprocs),
        metrics_exporter=metrics_exporter,
    )
