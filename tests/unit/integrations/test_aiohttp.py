import sys
from unittest.mock import patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient

from aqueduct import Flow
from aqueduct.integrations.aiohttp import (
    AppIntegrator,
    FLOW_NAME,
)
from tests.unit.conftest import (
    SleepHandler,
    stop_flow,
)


@pytest.fixture
async def app_with_flow():
    app = web.Application()
    AppIntegrator(app).add_flow(Flow(SleepHandler(0.001)))
    return app


@pytest.fixture
async def app_client(aiohttp_client, app_with_flow) -> TestClient:
    yield await aiohttp_client(app_with_flow)


class TestAppIntegrator:
    async def test_add_flow_exit_on_stop(self, app_with_flow: web.Application, app_client):
        flow = app_with_flow[FLOW_NAME]
        with patch.object(sys, 'exit') as sys_exit:
            await stop_flow(flow)
            assert sys_exit.called is True
