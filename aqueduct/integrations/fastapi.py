"""FastAPI integration.

Usage::

    from fastapi import Depends, FastAPI
    from aqueduct.integrations.fastapi import AppIntegrator, get_flow

    app = FastAPI()
    AppIntegrator(app).add_flow(build_flow())

    @app.post('/process')
    async def process(flow: Flow = Depends(get_flow())):
        ...

``add_flow`` forks the Flow step processes right away (so models are loaded before the server
accepts connections), while the Flow background tasks are bound to the server event loop
inside the application lifespan. Flows are stopped on application shutdown.
"""
import asyncio
import logging
import os
import signal
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Dict, List, Optional

from fastapi import FastAPI, Request

from aqueduct.flow import Flow
from aqueduct.integrations.utils import get_start_timeout
from aqueduct.logger import log, replace_logger


AQUEDUCT_FLOW_NAMES = 'aqueduct_flow_names'
FLOW_NAME = 'flow'


async def observe_flows(flows: Dict[str, Flow], check_interval: float = 1.):
    while True:
        for flow_name, flow in flows.items():
            if not flow.is_running:
                log.info(f'Flow {flow_name} is not running, application will be stopped')
                # kill process with SIGTERM to ensure, that stopping would not be delayed by other code
                os.kill(os.getpid(), signal.SIGTERM)
                return

        await asyncio.sleep(check_interval)


def get_flow(flow_name: str = FLOW_NAME) -> Callable[[Request], Flow]:
    """Returns a FastAPI dependency that resolves the Flow registered under ``flow_name``.

    Usage: ``flow: Flow = Depends(get_flow())``.
    """
    def _get_flow(request: Request) -> Flow:
        return getattr(request.app.state, flow_name)

    return _get_flow


class AppIntegrator:
    """Adds to app flows and actions to manage flows and app itself."""
    def __init__(
            self,
            app: FastAPI,
            exit_on_fail: bool = True,
            logger: Optional[logging.Logger] = None,
    ) -> None:
        if hasattr(app.state, AQUEDUCT_FLOW_NAMES):
            raise RuntimeError('AppIntegrator can be created only once. Reuse existing AppIntegrator.')
        self._app = app
        self._exit_on_fail = exit_on_fail
        self._started = False
        setattr(self._app.state, AQUEDUCT_FLOW_NAMES, [])
        self._app.router.lifespan_context = self._wrap_lifespan(self._app.router.lifespan_context)
        if logger is not None:
            replace_logger(logger)

    def add_flow(self, flow: Flow, flow_name: str = FLOW_NAME, with_start: bool = True):
        """Registers the flow in ``app.state`` under ``flow_name``.

        With ``with_start=True`` the Flow step processes are initialized immediately and the Flow
        is fully started on application startup. With ``with_start=False`` the Flow is left as is
        and must be started by the caller before the application starts serving requests.
        """
        if self._started:
            raise RuntimeError('Flows must be added before the application startup')
        if hasattr(self._app.state, flow_name):
            raise RuntimeError(f'Flow with name "{flow_name}" already exists in app')
        if with_start:
            flow.init_processes(timeout=get_start_timeout())
        self._flow_names.append(flow_name)
        setattr(self._app.state, flow_name, flow)

    @property
    def _flow_names(self) -> List[str]:
        return getattr(self._app.state, AQUEDUCT_FLOW_NAMES)

    @property
    def _flows(self) -> Dict[str, Flow]:
        return {name: getattr(self._app.state, name) for name in self._flow_names}

    def _wrap_lifespan(self, user_lifespan):
        @asynccontextmanager
        async def lifespan(app) -> AsyncIterator:
            self._started = True
            self._start_flows()
            try:
                async with user_lifespan(app) as state:
                    observer = asyncio.create_task(observe_flows(self._flows)) if self._exit_on_fail else None
                    try:
                        yield state
                    finally:
                        if observer is not None:
                            observer.cancel()
            finally:
                await self._stop_flows()

        return lifespan

    def _start_flows(self):
        for flow in self._flows.values():
            if flow.are_steps_initialized:
                flow.start_inited()

    async def _stop_flows(self):
        for flow in self._flows.values():
            await flow.stop()
