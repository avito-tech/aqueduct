import multiprocessing as mp
from dataclasses import dataclass

from typing import List

from .handler import HandleConditionType
from .task import BaseTask


@dataclass
class FlowStepQueue:
    queue: mp.Queue
    handle_condition: HandleConditionType


def select_next_queue(queues: List[FlowStepQueue], task: BaseTask, start_index: int = 0) -> mp.Queue:
    for step_queue in queues[start_index:]:
        if step_queue.handle_condition(task):
            return step_queue.queue
    return queues[-1].queue
