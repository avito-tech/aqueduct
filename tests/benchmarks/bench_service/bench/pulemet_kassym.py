"""Allows you to send requests with certain rps.

This tools is written by Vladislav Kassym in Avito.
"""

import asyncio
import math
from typing import (
    Awaitable,
    Callable,
    Iterable,
    Optional,
    Sized,
    Tuple,
)

try:
    from tqdm.auto import tqdm
except ImportError:
    tqdm = None


class Monitor:
    def __init__(self, rps: float, connections: int, tqdm=None):
        self._rps = rps
        self._connections = connections
        self._tqdm = tqdm
        if self._tqdm is None:
            return
        self._pbar_total = tqdm(desc='Total', total=0)
        self._pbar_rps = tqdm(desc='RPS', total=0)
        self._pbar_retry = tqdm(desc='Retry', total=0)

    def __del__(self):
        if self._tqdm is None:
            return
        for pbar in (self._pbar_total, self._pbar_rps, self._pbar_retry):
            pbar.refresh()
            pbar.close()

    def add(self, num: int):
        if self._tqdm is None:
            return
        self._pbar_total.total += num
        self._pbar_total.refresh()

    def update(self, retry: bool = False):
        if self._tqdm is None:
            return
        self._pbar_rps.update()
        if retry:
            self._pbar_retry.update()
        else:
            self._pbar_total.update()


class Pulemet:
    """Класс для контроля скорости выполнения корутин."""

    def __init__(
        self,
        rps: float = 0.1,
        max_connections: Optional[int] = None,
        name: str = '',
        pbar: Optional[bool] = False,
    ):
        """
        Инициализация.

        Args:
            rps: количество запросов в секунду
            max_connections: максимальное количество соединений, которые одновременно ждут ответа
            name: имя пулемета
            pbar: progress bar из tqdm. Пример: from tqdm.auto import tqdm; Pulemet(pbar=tqdm())
            pbar_retry: progress bar из tqdm. Пример: from tqdm.auto import tqdm; Pulemet(pbar=tqdm())
        """
        self._rps_min, self._rps_max = 5, 10
        self._rps, self._burst = self._get_rps_and_burst(rps)

        self._max_connections = max_connections
        if max_connections is None:
            self._max_connections = int(self._rps * 5) + 10  # 5 секунд копим, 10 прост
        self._name = name

        self._semaphore_time = asyncio.BoundedSemaphore(value=math.ceil(self._burst))
        self._semaphore_connections = asyncio.Semaphore(value=self._max_connections)

        self._timer_task = asyncio.ensure_future(self._timer())

        if pbar is None:
            pbar = False
        elif not isinstance(pbar, bool):
            pbar = True
        self._pbar = Monitor(
            rps=rps, connections=self._max_connections, tqdm=tqdm if pbar else None
        )

    def __del__(self):
        self._timer_task.cancel()

    def process(self, coros: [Iterable[Awaitable], Sized]) -> [Iterable[Awaitable], Sized]:
        """
        Запускает _wrap_coro для всех корутин в списке.

        Args:
            coros: Список корутин, которые необходимо ограничить по скорости исполнения

        Returns:
            Новый список корутин

        """
        self._pbar.add(num=len(coros))
        res = [self._wrap_coro(coro) for coro in coros]

        return res

    def process_funcs(
        self,
        coro_func: Callable[..., Awaitable],
        coros_kwargs: [Iterable[dict], Sized],
        exceptions: Tuple[BaseException, ...],
        exceptions_max: Optional[int] = None,
    ) -> [Iterable[Awaitable], Sized]:
        """
        Запускает _wrap_coro для всех корутин в списке.

        Args:
            coros: Список корутин, которые необходимо ограничить по скорости исполнения

        Returns:
            Новый список корутин

        """
        if hasattr(coros_kwargs, '__len__'):
            self._pbar.add(num=len(coros_kwargs))
        res = [
            self._warp_coro_func(
                coro_func=coro_func,
                exceptions=exceptions,
                exceptions_max=exceptions_max,
                **coro_kwargs,
            )
            for coro_kwargs in coros_kwargs
        ]

        return res

    def _get_rps_and_burst(self, rps: float):
        if rps <= self._rps_max:
            rps_target, burst = rps, 1
        else:
            burst_max = int(rps / self._rps_min)
            burst_min = math.ceil(rps / self._rps_max)
            res = []
            for burst in range(burst_min, burst_max + 1):
                rps_target = round(rps / burst, 2)
                prec = abs(rps - rps_target * burst) / rps * 100
                res.append((burst, rps_target, prec))
                if prec < 0.01:
                    break
            burst, rps_target, prec = sorted(res, key=lambda x: x[2])[0]

        return rps_target, burst

    async def _timer(self):
        """Освобождает семафор на исполнение корутины `burst` раз в `1 / rps` секунд."""
        while True:
            await asyncio.sleep(1 / self._rps)
            for ind in range(self._burst):
                try:
                    self._semaphore_time.release()
                except ValueError:
                    continue

    async def _wrap_coro(self, coro: Awaitable, update: bool = True) -> Awaitable:
        """
        Ждем освобождения семафоров на rps и количество соединений и запускаем корутину.

        Args:
            coro: корутина, запуск которой необходимо ограничить по rps

        Returns:

        """
        async with self._semaphore_connections:
            await self._semaphore_time.acquire()
            if update:
                self._pbar.update(retry=False)
            result = await coro

        return result

    async def _warp_coro_func(
        self,
        coro_func: Callable[..., Awaitable],
        exceptions: Tuple[BaseException, ...],
        exceptions_max: Optional[int] = None,
        **coro_kwargs,
    ) -> Awaitable:
        cnt = 0
        while True:
            coro = coro_func(**coro_kwargs)
            try:
                coro = await self._wrap_coro(coro, update=False)
                self._pbar.update(retry=False)
                return coro
            except exceptions as exc:
                cnt += 1
                self._pbar.update(retry=True)
                if exceptions_max is not None and cnt == exceptions_max:
                    raise exc
                continue
