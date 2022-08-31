import multiprocessing as mp
from multiprocessing.queues import Queue


class TaskMetricsQueue(Queue):
    def __init__(self, maxsize=0, *, ctx=None):
        ctx = ctx or mp.get_context()
        Queue.__init__(self, maxsize, ctx=ctx)
        self._task_size: int = 0

    def _recv_bytes_wrapper(self, *args, **kwargs):
        """Wraps _recv_bytes to record task byte size"""
        result = self.__recv_bytes(*args, **kwargs)
        self._task_size = len(result)
        return result

    @property
    def _recv_bytes(self):
        return self._recv_bytes_wrapper

    @_recv_bytes.setter
    def _recv_bytes(self, _recv_bytes_function):
        self.__recv_bytes = _recv_bytes_function

    @property
    def task_size(self) -> int:
        return self._task_size
