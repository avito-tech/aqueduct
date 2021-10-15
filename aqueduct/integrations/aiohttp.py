import asyncio
import sys

from aqueduct.flow import Flow
from aqueduct.logger import log

from aiohttp import web


AQUEDUCT_FLOW_NAMES = 'aqueduct_flow_names'
FLOW_NAME = 'flow'
FLOWS_OBSERVER = 'aqueduct_flows_observer'


async def observe_flows(app: web.Application, check_interval: float = 1.):
    flows = {name: app[name] for name in app[AQUEDUCT_FLOW_NAMES]}
    while True:
        for flow_name, flow in flows.items():
            if not flow.is_running:
                log.info(f'Flow {flow_name} is not running, application will be stopped')
                sys.exit(1)
        await asyncio.sleep(check_interval)


async def run_flows_observer(app):
    app[FLOWS_OBSERVER] = asyncio.create_task(observe_flows(app))


async def stop_flows_observer(app):
    app[FLOWS_OBSERVER].cancel()


async def stop_flows(app):
    flows = [app[name] for name in app[AQUEDUCT_FLOW_NAMES]]
    for flow in flows:  # type: Flow
        await flow.stop()


class AppIntegrator:
    """Adds to app flows and actions to manage flows and app itself."""
    def __init__(self, app: web.Application, exit_on_fail: bool = True):
        if AQUEDUCT_FLOW_NAMES in app:
            raise RuntimeError('AppIntegrator can be created only once. Reuse existing AppIntegrator.')
        self._app = app
        self._app[AQUEDUCT_FLOW_NAMES] = []
        if exit_on_fail:
            self._app.on_startup.append(run_flows_observer)
            self._app.on_shutdown.append(stop_flows_observer)
        self._app.on_shutdown.append(stop_flows)

    def add_flow(self, flow: Flow, flow_name: str = FLOW_NAME, with_start: bool = True):
        if flow_name in self._app:
            raise RuntimeError(f'Flow with name "{flow_name}" already exists in app')
        if with_start:
            flow.start()
        self._app[AQUEDUCT_FLOW_NAMES].append(flow_name)
        self._app[flow_name] = flow
