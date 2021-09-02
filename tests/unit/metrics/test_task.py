import time
import pytest

from aqueduct.metrics.task import TaskMetrics
from aqueduct.metrics.timer import timeit


@pytest.fixture
def task_metrics():
    return TaskMetrics()


class TestTaskMetrics:
    def test_timeit(self, task_metrics):
        with timeit() as timer:
            time.sleep(0.001)
        task_metrics.handle_times.add('test_metric', timer.seconds)

        assert task_metrics.handle_times.items[0][1] >= 0.001
