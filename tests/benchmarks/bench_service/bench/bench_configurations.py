"""Calculates the maximum rps for a service on a single physical node.

Calculations are performed when the service configuration is automatically changed.

To start, see README.md.
"""
import json
import pathlib
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Tuple

import logging

from .base import IMAGES, ReportRow, RpsCounterCfg
from .utils import run_cmd

PROJ_ROOT = pathlib.Path(__file__).parent.parent
REPORT_NAME = 'report_%s.csv'

RUN_SERVICE = f'docker-compose --env-file %s up -d --scale maas=%s maas'
STOP_SERVICE = 'docker-compose stop maas'
RM_SERVICE = 'docker-compose rm -f maas'
RUN_BENCH = 'docker-compose exec maas-bench python -m bench.rps_counter %s %s'
GET_SERVICE_IDS = 'docker ps --filter name=bench_service_maas -q'


class ScaleMethods(Enum):
    REPLICAS = 'replicas'
    PROC = 'proc'


@dataclass
class Cfg:
    path: str
    image_size: str
    image_name: str = field(init=False)
    replicas: int

    app_type: str
    nprocs: List[int]

    timeout: float
    load_time_secs: int
    success_rate: float
    min_rps: int
    scale_method: ScaleMethods

    def __post_init__(self):
        self.image_name = IMAGES[self.image_size]
        if self.app_type == 'pipeline':
            assert not self.nprocs
            assert self.scale_method == ScaleMethods.REPLICAS


def get_cfg(cfg_path: str) -> Cfg:
    with open(cfg_path) as f:
        cfg_info = json.loads(f.read())
    cfg_info['scale_method'] = ScaleMethods(cfg_info['scale_method'])
    return Cfg(cfg_path, **cfg_info)


@contextmanager
def running_service(app_type: str, replicas: int, nprocs: List[int]):
    with tempfile.NamedTemporaryFile('w', dir=PROJ_ROOT) as cfg_f:
        cfg_name = cfg_f.name.split('/')[-1]
        cfg_f.write(json.dumps({'app_type': app_type, 'nprocs': nprocs}))
        cfg_f.seek(0)

        with tempfile.NamedTemporaryFile('w', dir=PROJ_ROOT) as dc_env_f:
            dc_env_f.write(f'CFG_PATH={cfg_name}')
            dc_env_f.seek(0)

            run_cmd(RUN_SERVICE % (dc_env_f.name, replicas), shell=True)
            time.sleep(3)
            try:
                yield
            finally:
                run_cmd(STOP_SERVICE)
                run_cmd(RM_SERVICE)


def run_rps_counter(cfg: Cfg, replicas: int, nprocs: List[int], min_rps: int, report_name: str):
    container_ids = run_cmd(GET_SERVICE_IDS, stdout=subprocess.PIPE).stdout.read().decode().split()
    assert len(container_ids) == replicas, f'number of containers is {len(container_ids)} instead of {replicas}'
    rps_cfg = RpsCounterCfg(
        replicas=replicas,
        nprocs=nprocs,
        container_ids=container_ids,
        image_name=cfg.image_name,
        timeout=cfg.timeout,
        load_time_secs=cfg.load_time_secs,
        success_rate=cfg.success_rate,
        min_rps=min_rps or cfg.min_rps,
    )

    with tempfile.NamedTemporaryFile('w', dir=PROJ_ROOT) as f:
        f.write(json.dumps(rps_cfg.__dict__))
        f.seek(0)
        rps_cfg_name = f.name.split('/')[-1]
        run_cmd(RUN_BENCH % (rps_cfg_name, report_name))
    time.sleep(1)


def scaled(replicas: int, nprocs: List[int], scale_method: ScaleMethods) -> Tuple[int, List[int]]:
    # todo нужно дать возможность запускать сильно больше реплик (когда увеличивать кратно??)
    #  нужно отслеживать, что часть реплик не умерла к моменту теста (gpu oom)
    k = 1
    start_replicas, start_nprocs = replicas, nprocs
    while True:
        if scale_method == ScaleMethods.REPLICAS:
            replicas = start_replicas + k - 1
        else:
            nprocs = [n * k for n in start_nprocs]
        yield replicas, nprocs
        k += 1


def main(cfg_path: str):
    cfg = get_cfg(cfg_path)

    report_name = REPORT_NAME % cfg_path.split('/')[-1].split('.')[0]
    with open(report_name, 'w') as f:
        f.write(','.join(ReportRow.get_field_names()) + '\n')

    max_rps, failed_cnt = 0, 0

    for replicas, nprocs in scaled(cfg.replicas, cfg.nprocs, cfg.scale_method):
        with running_service(cfg.app_type, replicas, nprocs):
            run_rps_counter(cfg, replicas, nprocs, max_rps, report_name)

        max_rps_info = ReportRow.get_from_csv(
            report_name,
            lambda x: x.is_max_rps and x.replicas == replicas and x.nprocs == nprocs
        )
        if max_rps_info and max_rps_info[0].rps > max_rps:
            max_rps = max_rps_info[0].rps
        else:
            failed_cnt += 1
            if failed_cnt > 1:
                break

    logging.info(f'Absolutely max rps = {max_rps}')


if __name__ == '__main__':
    main(sys.argv[1])
