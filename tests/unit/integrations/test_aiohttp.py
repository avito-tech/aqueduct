import asyncio
from unittest.mock import AsyncMock

import aiohttp.web
import pytest
from aiohttp import web
from aiohttp.test_utils import setup_test_loop, TestClient

from aqueduct import Flow
from aqueduct.integrations.aiohttp import (
    AppIntegrator,
    FLOW_NAME,
)
from tests.unit.conftest import (
    SleepHandler,
    terminate_worker,
)


async def init_app(app: web.Application):
    integrator = AppIntegrator(app)
    integrator.add_flow(Flow(SleepHandler(0.001)), with_start=True)

    app.on_shutdown.insert(0, AsyncMock())
    app.on_cleanup.insert(0, AsyncMock())

    return app


@pytest.fixture
async def app_with_flow():
    app = web.Application()
    AppIntegrator(app).add_flow(Flow(SleepHandler(0.001)))
    return app


@pytest.fixture
async def app_client(aiohttp_client, app_with_flow) -> TestClient:
    yield await aiohttp_client(app_with_flow)


class TestAppIntegrator:
    def test_app_stops_correctly(self, loop):
        """Flow monitoring should not brake correct application stop.

        Real aiohttp application stops differently then aiohttp_server fixture,
        so to check real life behaviour we need to start AppRunner inside a test."""
        async def ok_exit(_):
            async def stop_task():
                await asyncio.sleep(1)
                raise KeyboardInterrupt

            asyncio.create_task(stop_task())

        app = web.Application()
        app.on_startup.append(ok_exit)

        aiohttp.web.run_app(init_app(app))

        assert app.on_shutdown[0].called is True
        assert app.on_cleanup[0].called is True

    def test_app_exists_on_flow_stop(self, loop):
        async def error_exit(_):
            async def stop_task():
                await asyncio.sleep(1)
                await terminate_worker(app[FLOW_NAME])

            asyncio.create_task(stop_task())

        app = web.Application()
        app.on_startup.append(error_exit)

        with pytest.raises(SystemExit) as info:
            aiohttp.web.run_app(init_app(app))

        assert info.value.code == 1
        assert app.on_shutdown[0].called is True
        assert app.on_cleanup[0].called is True

    async def test_unittest_teardown(self, app_with_flow: web.Application, app_client):
        """Flow monitoring should not brake unit tests.

        Here we just simulate successful test, there should be not errors on teardown."""
        pass

