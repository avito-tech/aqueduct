import time
from typing import Optional


class Timer:
    precision = 5

    def __init__(self):
        self._start = None
        self._stop = None

    @staticmethod
    def _time() -> float:
        return time.monotonic()

    def start(self):
        self._start = self._time()

    def stop(self):
        self._stop = self._time()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def seconds(self) -> Optional[float]:
        if self._start is None or self._stop is None:
            return
        return round(self._stop - self._start, self.precision)


class TransferTimer(Timer):
    def __init__(self, transfer_from: str):
        self.transfer_from = transfer_from
        self.transfer_to: str = None  # noqa
        super().__init__()


timeit = Timer
