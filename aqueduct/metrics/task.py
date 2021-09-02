from .base import IExtendable, MetricsItems
from .timer import TransferTimer


class TasksMetricsStorage(IExtendable):
    def __init__(self):
        self.transfer_times = MetricsItems()
        self.handle_times = MetricsItems()
        self.batch_times = MetricsItems()
        self.batch_sizes = MetricsItems()

    def extend(self, storage: 'TasksMetricsStorage'):
        self.transfer_times.extend(storage.transfer_times)
        self.handle_times.extend(storage.handle_times)
        self.batch_times.extend(storage.batch_times)
        self.batch_sizes.extend(storage.batch_sizes)


class TaskMetrics(TasksMetricsStorage):
    """Task's "backpack" with metrics.

    It is used to store metrics related to a task passing through the Flow child processes. This mechanic
    works as long as all tasks reach the output queue in the main process.
    """
    def __init__(self):
        super().__init__()
        self._transfer_timer: TransferTimer = None  # noqa

    def start_transfer_timer(self, transfer_from: str):
        self._transfer_timer = TransferTimer(transfer_from)
        self._transfer_timer.start()

    def stop_transfer_timer(self, transfer_to: str):
        self._transfer_timer.stop()
        from_ = self._transfer_timer.transfer_from
        self.transfer_times.add(f'from_{from_}_to_{transfer_to}', self._transfer_timer.seconds)
