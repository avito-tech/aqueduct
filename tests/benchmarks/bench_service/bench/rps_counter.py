import asyncio
import pathlib
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field
from http import HTTPStatus
import logging
from typing import AsyncContextManager, Awaitable, Callable, List, Optional, Tuple

import aiohttp
import docker
import pandas as pd

from .base import ReportRow, RpsCounterCfg, get_rps_counter_cfg
from .pulemet_kassym import Pulemet
from .utils import MaxNumberFinder, run_cmd

PROJ_ROOT = pathlib.Path(__file__).parent.parent

IMAGES_DIR = pathlib.Path(__file__).parent / 'data'
GPU_QUERY = 'nvidia-smi --query-gpu=%s -l 1 --format=csv'
MAAS_URL = 'http://maas:8080/classify'


def restart_service(ids: List[str], sleep_secs: int = None):
    client = docker.DockerClient(base_url='tcp://172.17.0.1:2376')
    for c in client.containers.list():
        if c.id in ids:
            c.restart()
    if sleep_secs:
        time.sleep(sleep_secs)


@dataclass
class RequestData:
    url: str
    timeout: float
    method: str = 'post'
    json: dict = field(default_factory=dict)
    data: bytes = None


class ReqStats(Counter):
    @property
    def success_rate(self) -> float:
        return self.get(HTTPStatus.OK, 0) / sum(self.values())

    @property
    def error_rate(self) -> float:
        return 1 - self.success_rate


@dataclass
class GPUStatsField:
    name: str
    type_: type
    units: str


GPU_UTIL = GPUStatsField('utilization.gpu', int, '%')
USED_MEM = GPUStatsField('memory.used', int, 'MiB')
MEM_UTIL = GPUStatsField('utilization.memory', int, '%')
POWER = GPUStatsField('power.draw', float, 'W')
GPU_STATS_FIELDS = (GPU_UTIL, USED_MEM, MEM_UTIL, POWER)


class NvidiaSmiStats:
    def __init__(self):
        self._field_names = [f.name for f in GPU_STATS_FIELDS]
        self._f = None
        self._proc = None
        self._df = None

    def _get_gpu_stats(self) -> pd.DataFrame:
        stats = pd.read_csv(self._f.name, names=self._field_names, header=0)
        for col, field in zip(stats, GPU_STATS_FIELDS):
            stats[col] = stats[col].apply(lambda x: field.type_(x.split()[0]))
        return stats

    def __enter__(self):
        self._f = tempfile.NamedTemporaryFile('w')
        self._proc = run_cmd(GPU_QUERY % ','.join(self._field_names), stdout=self._f, wait=False)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._proc.terminate()
        self._df = self._get_gpu_stats()
        self._f.close()

    @property
    def df(self) -> pd.DataFrame:
        return self._df


async def get_requests_stats(
        responses,
        check_ok: Callable[[aiohttp.ClientResponse], Awaitable[bool]] = None,
) -> ReqStats:
    statuses = []
    for resp in responses:
        if isinstance(resp, AsyncContextManager):
            async with resp as r:
                statuses.append(r.status)
                if check_ok:
                    if r.status == HTTPStatus.OK:
                        await check_ok(r)
        elif isinstance(resp, asyncio.TimeoutError):
            statuses.append(499)
        elif isinstance(resp, aiohttp.ClientConnectionError):
            statuses.append(HTTPStatus.SERVICE_UNAVAILABLE.value)
        else:
            raise Exception(f'Unknown response: {type(resp)}, {resp}')
    return ReqStats(statuses)


async def check_resp_body(r: aiohttp.ClientResponse):
    body = await r.json()
    assert isinstance(body, dict), f'Response body is not dict: {body}'
    assert isinstance(body.get('result'), list), f'Bad body: {body}'


async def send_requests(req: RequestData, rps: int, time_interval_secs: int) -> Tuple:
    async with aiohttp.ClientSession() as session:
        coros = [
            session.request(req.method, req.url, json=req.json, timeout=req.timeout)
            for _ in range(rps * time_interval_secs)
        ]
        pulemet = Pulemet(rps=rps, max_connections=10**6)
        pulemet_coros = pulemet.process(coros)

        return await asyncio.gather(*pulemet_coros, return_exceptions=True)


async def warmup_service(cfg: RpsCounterCfg):
    logging.info('start to warmup')
    for i in range(1, 4):
        req_stats, _ = await load_service(cfg.image_name, cfg.min_rps, cfg.load_time_secs, cfg.timeout)
        if req_stats.error_rate > 0:
            time.sleep(2**i)
            continue
        logging.info('warmup successfully finished')
        break
    else:
        raise Exception('warmup failed')


async def load_service(
        image_name: str,
        rps: int,
        time_interval_secs: int,
        timeout: float,
) -> Tuple[ReqStats, NvidiaSmiStats]:
    """Returns statistics of requests sent to the server."""
    logging.info(f'\nstart to load with rps = {rps} during {time_interval_secs} secs')
    r = RequestData(url=MAAS_URL, timeout=timeout, json={'image_name': image_name})
    with NvidiaSmiStats() as gpu_stats:
        responses = await send_requests(r, rps=rps, time_interval_secs=time_interval_secs)
    req_stats = await get_requests_stats(responses, check_ok=check_resp_body)
    logging.info(f'requests stats is {req_stats}, success rate is {req_stats.success_rate * 100:.1f}%')
    logging.info(
        f'max value of GPU_UTIL is {gpu_stats.df[GPU_UTIL.name].max()}{GPU_UTIL.units}, '
        f'mean value of GPU UTIL is {gpu_stats.df[GPU_UTIL.name].mean():.1f}{GPU_UTIL.units}'
    )
    return req_stats, gpu_stats


def get_report_row(rps: int, success_rate: float, cfg: RpsCounterCfg, gpu_stats: NvidiaSmiStats) -> ReportRow:
    return ReportRow(
        rps=rps,
        success_rate=success_rate,
        replicas=cfg.replicas,
        nprocs=cfg.nprocs,
        image_name=cfg.image_name,
        load_time_secs=cfg.load_time_secs,
        max_GPU_UTIL=gpu_stats.df[GPU_UTIL.name].max(),
        mean_GPU_UTIL=gpu_stats.df[GPU_UTIL.name].mean(),
    )


async def calculate_rps(cfg: RpsCounterCfg, report_path: str) -> Optional[int]:
    logging.info({k: v for k, v in cfg.__dict__.items() if k not in ('container_ids', 'image_name')})

    await warmup_service(cfg)

    report = pd.DataFrame(columns=ReportRow.get_field_names())
    with MaxNumberFinder(cfg.min_rps) as rps_counter:
        while rps_counter.number is not None:  # just for PyCharm, really it means `while True`
            req_stats, gpu_stats = await load_service(
                cfg.image_name, rps_counter.number, cfg.load_time_secs, cfg.timeout)
            report_row = get_report_row(rps_counter.number, req_stats.success_rate, cfg, gpu_stats)
            if req_stats.success_rate < cfg.success_rate:
                rps_counter.next(less=True)
                restart_service(cfg.container_ids, cfg.load_time_secs)
                await warmup_service(cfg)
            else:
                rps_counter.next()
            report = report.append(report_row.__dict__, ignore_index=True)
    report = report.append(report_row.__dict__, ignore_index=True)

    logging.info(f'max rps is {rps_counter.max_number}')
    report.loc[report['rps'] == rps_counter.max_number, 'is_max_rps'] = True
    report.to_csv(report_path, mode='a', header=False, index=False, float_format='%.3f')

    return rps_counter.max_number


if __name__ == '__main__':
    asyncio.run(calculate_rps(get_rps_counter_cfg(sys.argv[1]), sys.argv[2]))
