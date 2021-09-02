import json
import os
from enum import Enum
from typing import List, Union

from aiohttp import web

from .flow import get_1step_flow, get_flow
from .flow_metrics import get_metrics_exporter, get_statsd_client
from .main_aqueduct import get_flow_app
from .main_simple import get_pipeline_app


class AppTypes(Enum):
    PIPELINE = 'pipeline'
    ONE_STEP = '1step'
    THREE_STEP = '3step'


def get_app_args() -> tuple:
    cfg_path = os.environ.get('CFG_PATH')
    if not cfg_path:
        raise Exception('No CFG_PATH env')

    with open(cfg_path) as f:
        cfg = json.loads(f.read())

    app_type, nprocs = AppTypes(cfg['app_type']), cfg['nprocs']
    if app_type == AppTypes.ONE_STEP:
        return app_type, nprocs[0]
    return app_type, nprocs


def get_app(app_type: AppTypes, nprocs: Union[List[int], int] = None):
    if app_type == AppTypes.PIPELINE:
        return get_pipeline_app()

    if app_type == AppTypes.ONE_STEP:
        flow = get_1step_flow(nprocs)
        return get_flow_app(flow)

    if app_type == AppTypes.THREE_STEP:
        client = get_statsd_client()
        metrics_exporter = get_metrics_exporter(client) if client else None
        flow = get_flow(nprocs, metrics_exporter=metrics_exporter)
        return get_flow_app(flow, statsd_client=client)


if __name__ == '__main__':
    web.run_app(get_app(*get_app_args()))
