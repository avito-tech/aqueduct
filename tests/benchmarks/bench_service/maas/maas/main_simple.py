from aiohttp import web

from .image_storage import ImageStorage
from .models import Pipeline, get_pipeline


class ClassifyView(web.View):
    @property
    def pipeline(self) -> Pipeline:
        return self.request.app['pipeline']

    @property
    def image_storage(self) -> ImageStorage:
        return self.request.app['image_storage']

    async def post(self):
        im_name = (await self.request.json())['image_name']
        im = await self.image_storage.get_image(im_name)
        res = self.pipeline.process(im)
        return web.json_response(data={'result': res})


async def get_pipeline_app() -> web.Application:
    app = web.Application(client_max_size=0)
    pipeline = get_pipeline()
    app['pipeline'] = pipeline
    app['image_storage'] = ImageStorage()
    app.router.add_post('/classify', ClassifyView)

    return app


if __name__ == '__main__':
    web.run_app(get_pipeline_app())
