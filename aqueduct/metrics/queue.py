import multiprocessing as mp
from multiprocessing.queues import Queue
from typing import Optional

from .timer import Timer


class TaskMetricsQueue(Queue):
    def __init__(self, maxsize=0, *, ctx=None):
        ctx = ctx or mp.get_context()
        Queue.__init__(self, maxsize, ctx=ctx)
        self._transfer_timer: Optional[Timer] = None

    def _recv_bytes_wrapper(self, *args, **kwargs):
        """Wraps _recv_bytes to start transfer timer"""
        self._transfer_timer = Timer()
        self._transfer_timer.start()
        return self.__recv_bytes(*args, **kwargs)

    @property
    def _recv_bytes(self):
        return self._recv_bytes_wrapper

    @_recv_bytes.setter
    def _recv_bytes(self, _recv_bytes_function):
        self.__recv_bytes = _recv_bytes_function

    def get(self, block=True, timeout=None):
        """Overrides default get method to stop transfer timer"""
        try:
            result = super().get(block, timeout)
            if self._transfer_timer is not None:
                self._transfer_timer.stop()
        except Exception:
            self._transfer_timer = None
            raise
        return result

    @property
    def transfer_time(self) -> Optional[Timer]:
        return self._transfer_timer
