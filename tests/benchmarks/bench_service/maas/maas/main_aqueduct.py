from typing import Optional

from aiohttp import web

from .flow import (
    Flow,
    Task,
    get_flow,
    observe_flow,
    stop_flow,
)
from .flow_metrics import (
    StatsDMetricsBuffer,
    close_statsd_client,
    connect_statsd_client,
)
from .image_storage import ImageStorage


class ClassifyView(web.View):
    @property
    def flow(self) -> Flow:
        return self.request.app['flow']

    @property
    def image_storage(self) -> ImageStorage:
        return self.request.app['image_storage']

    async def post(self):
        im_name = (await self.request.json())['image_name']
        im = await self.image_storage.get_image(im_name)
        task = Task(im)
        await self.flow.process(task)
        res = task.h_pred
        return web.json_response(data={'result': res})


async def get_flow_app(
        flow: Flow = None,
        app: web.Application = None,
        statsd_client: Optional[StatsDMetricsBuffer] = None,
) -> web.Application:
    if flow is None:
        flow = get_flow([1, 1, 1])
    if app is None:
        app = web.Application(client_max_size=0)
    app['flow'] = flow
    app['image_storage'] = ImageStorage()
    app.router.add_post('/classify', ClassifyView)

    if statsd_client:
        app['statsd_client'] = statsd_client
        app.on_startup.append(connect_statsd_client)
        app.on_cleanup.append(close_statsd_client)

    flow.start()
    app.on_startup.append(observe_flow)
    app.on_shutdown.append(stop_flow)

    return app


if __name__ == '__main__':
    web.run_app(get_flow_app())
