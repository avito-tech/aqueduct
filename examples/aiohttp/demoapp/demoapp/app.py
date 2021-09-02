from aiohttp import web

from .flow import (
    Flow,
    Task,
    get_flow,
    observe_flow,
    stop_flow,
)


class ClassifyView(web.View):
    @property
    def flow(self) -> Flow:
        return self.request.app['flow']

    async def post(self):
        im = await self.request.read()
        task = Task(im)
        await self.flow.process(task)
        return web.json_response(data={'result': task.h_pred})


def prepare_app() -> web.Application:
    app = web.Application(client_max_size=0)
    flow = get_flow()
    app['flow'] = flow
    app.router.add_post('/classify', ClassifyView)

    flow.start()
    app.on_startup.append(observe_flow)
    app.on_shutdown.append(stop_flow)

    return app


if __name__ == '__main__':
    web.run_app(prepare_app())
