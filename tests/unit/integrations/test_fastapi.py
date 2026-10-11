import asyncio
import logging
import multiprocessing
import os
import signal
import time
from contextlib import asynccontextmanager
from typing import List

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from aqueduct import Flow
from aqueduct.flow import FlowState
from aqueduct.integrations.fastapi import (
    AQUEDUCT_FLOW_NAMES,
    FLOW_NAME,
    AppIntegrator,
    get_flow,
)
from aqueduct.logger import log as aqueduct_log
from tests.unit.conftest import (
    SetResultSleepHandler,
    SleepHandler,
    Task,
)


def kill_flow_worker(flow: Flow) -> None:
    context = next(iter(flow._contexts.values()))
    context.processes[0].terminate()


def wait_until(predicate, timeout_sec: float = 10., step_sec: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step_sec)
    return predicate()


async def _shutdown_flow(flow: Flow) -> None:
    if flow.are_steps_initialized:
        flow.start_inited()
    await flow.stop(graceful=False)


@pytest.fixture
def flow() -> Flow:
    flow = Flow(SetResultSleepHandler(0.001))
    yield flow
    if flow.state != FlowState.STOPPED:
        asyncio.run(_shutdown_flow(flow))


@pytest.fixture
def app_with_flow(flow: Flow) -> FastAPI:
    app = FastAPI()
    AppIntegrator(app).add_flow(flow)

    @app.post('/process')
    async def process(flow: Flow = Depends(get_flow())):
        task = Task()
        await flow.process(task)
        return {'result': task.result}

    return app


class TestAppIntegrator:
    def test_add_flow_initializes_processes_and_exposes_flow_in_state(self, flow: Flow):
        app = FastAPI()

        AppIntegrator(app).add_flow(flow)

        assert getattr(app.state, FLOW_NAME) is flow
        assert getattr(app.state, AQUEDUCT_FLOW_NAMES) == [FLOW_NAME]
        assert flow.are_steps_initialized
        assert not flow.is_running

    def test_flow_runs_within_lifespan_and_stops_on_shutdown(self, flow: Flow, app_with_flow: FastAPI):
        with TestClient(app_with_flow):
            assert flow.is_running

        assert flow.state == FlowState.STOPPED

    def test_request_is_processed_via_dependency(self, app_with_flow: FastAPI):
        with TestClient(app_with_flow) as client:
            response = client.post('/process')

        assert response.status_code == 200
        assert response.json() == {'result': 'test'}

    def test_get_flow_by_custom_name(self, flow: Flow):
        app = FastAPI()
        AppIntegrator(app).add_flow(flow, flow_name='classifier')

        @app.get('/flow_name')
        async def flow_name(request: Request, injected: Flow = Depends(get_flow('classifier'))):
            return {'same': injected is request.app.state.classifier}

        with TestClient(app) as client:
            assert client.get('/flow_name').json() == {'same': True}

    def test_user_lifespan_is_preserved(self, flow: Flow):
        events: List[str] = []

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            events.append('startup')
            yield {'answer': 42}
            events.append('shutdown')

        app = FastAPI(lifespan=lifespan)
        AppIntegrator(app).add_flow(flow)

        @app.get('/answer')
        async def answer(request: Request):
            return {'answer': request.state.answer}

        with TestClient(app) as client:
            assert events == ['startup']
            assert client.get('/answer').json() == {'answer': 42}

        assert events == ['startup', 'shutdown']

    def test_flow_is_running_during_user_startup_and_shutdown(self, flow: Flow):
        states: List[bool] = []

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            states.append(flow.is_running)
            yield
            states.append(flow.is_running)

        app = FastAPI(lifespan=lifespan)
        AppIntegrator(app).add_flow(flow)

        with TestClient(app):
            pass

        assert states == [True, True]

    def test_integrator_can_be_created_only_once(self):
        app = FastAPI()
        AppIntegrator(app)

        with pytest.raises(RuntimeError):
            AppIntegrator(app)

    def test_duplicate_flow_name_raises(self, flow: Flow):
        app = FastAPI()
        integrator = AppIntegrator(app)
        integrator.add_flow(flow)

        with pytest.raises(RuntimeError):
            integrator.add_flow(Flow(SleepHandler(0.001)), with_start=False)

    def test_add_flow_after_startup_raises(self, flow: Flow):
        app = FastAPI()
        integrator = AppIntegrator(app)
        integrator.add_flow(flow)

        with TestClient(app):
            with pytest.raises(RuntimeError):
                integrator.add_flow(Flow(SleepHandler(0.001)), flow_name='late', with_start=False)

    def test_with_start_false_leaves_flow_untouched(self, flow: Flow):
        app = FastAPI()
        AppIntegrator(app, exit_on_fail=False).add_flow(flow, with_start=False)

        assert flow.state == FlowState.STOPPED
        with TestClient(app):
            assert flow.state == FlowState.STOPPED
        assert flow.state == FlowState.STOPPED

    def test_logger_is_replaced(self):
        attrs = ('name', 'level', 'parent', 'propagate', 'handlers', 'disabled')
        saved = {attr: getattr(aqueduct_log, attr) for attr in attrs}
        custom_logger = logging.getLogger('custom_fastapi_logger')
        try:
            AppIntegrator(FastAPI(), logger=custom_logger)
            assert aqueduct_log.name == custom_logger.name
        finally:
            for attr, value in saved.items():
                setattr(aqueduct_log, attr, value)

    def test_sends_sigterm_to_itself_when_flow_dies(self, flow: Flow, app_with_flow: FastAPI, monkeypatch):
        self_signals: List[int] = []
        real_kill = os.kill

        def recording_kill(pid: int, sig: int) -> None:
            if pid == os.getpid():
                self_signals.append(sig)
                return
            real_kill(pid, sig)

        monkeypatch.setattr(os, 'kill', recording_kill)

        with TestClient(app_with_flow):
            kill_flow_worker(flow)
            assert wait_until(lambda: self_signals, timeout_sec=10)

        assert self_signals == [signal.SIGTERM]

    def test_uvicorn_server_exits_when_flow_dies(self):
        process = multiprocessing.Process(target=run_uvicorn_app_with_dying_flow)
        process.start()

        assert wait_until(lambda: not process.is_alive(), timeout_sec=30)
        # uvicorn >= 0.29 re-raises the captured SIGTERM after a graceful shutdown
        assert process.exitcode in (0, -signal.SIGTERM)


def run_uvicorn_app_with_dying_flow() -> None:
    import uvicorn

    flow = Flow(SleepHandler(0.001))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async def kill_worker_soon():
            await asyncio.sleep(1)
            kill_flow_worker(flow)

        asyncio.create_task(kill_worker_soon())
        yield

    app = FastAPI(lifespan=lifespan)
    AppIntegrator(app).add_flow(flow)

    uvicorn.run(app, host='127.0.0.1', port=0, log_level='warning')
