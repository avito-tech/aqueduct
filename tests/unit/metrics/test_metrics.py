from aqueduct.metrics import MAIN_PROCESS
from aqueduct.metrics.timer import timeit
from aqueduct.task import BaseTask


def test_metrics_count(task: BaseTask):

    # кладем в очередь
    task.metrics.start_transfer_timer(MAIN_PROCESS)
    # достаем из очереди
    task.metrics.stop_transfer_timer('handler1')
    # выполняем полезную работу
    with timeit() as timer:
        pass
    task.metrics.handle_times.add('handler1', timer.seconds)
    # кладем в очередь
    task.metrics.start_transfer_timer('handler1')
    # достаем из очереди
    task.metrics.stop_transfer_timer('handler2')
    # выполняем полезную работу
    task.metrics.handle_times.add('handler2', timer.seconds)
    # кладем в очередь
    task.metrics.start_transfer_timer('handler2')
    # достаем из очереди
    task.metrics.stop_transfer_timer('handler3')
    # выполняем полезную работу
    task.metrics.handle_times.add('handler3', timer.seconds)
    # кладем в очередь
    task.metrics.start_transfer_timer('handler3')
    # достаем из очереди
    task.metrics.stop_transfer_timer('parent_process')
    # total
    task.metrics.handle_times.add('total', timer.seconds)

    assert len(task.metrics.handle_times.items) == 4
    assert len(task.metrics.transfer_times.items) == 4
