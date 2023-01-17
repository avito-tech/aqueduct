Getting Started
===============

This is a simple example of using shared memory in Aqueduct .

.. code-block:: python

    import asyncio

    from aiohttp import web
    from aqueduct import Flow, FlowStep, BaseTaskHandler, BaseTask


    class MyModel:
        """This is an example of a CPU-bound model"""

        def process(self, image):
            """do something with image on CPU"""
            pass

    class Task(BaseTask):
        """Container for sending arguments to the model."""
        def __init__(self, image=None):
            super().__init__()
            self.image = image
            self.image_processed = None  # result will be here

    class ImageHandler(BaseTaskHandler):
        """When using Aqueduct, we need to wrap your model."""
        def __init__(self):
            self._model = None

        def on_start(self):
            """Executed in a child process, so the parent process does not consume additional memory."""
            self._model = MyModel()

        def handle(self, *tasks: Task):
            """List of tasks because it can be batching."""
            for task in tasks:
                task.image_processed = self._model.process(task.image)


    class ImageView(web.View):
        """Simple aiohttp-view handler"""

        async def post(self):
            task = Task()
            await task.share_value_with_data(
                field_name='image', 
                content=self.request.content,
                size=self.request.content_length,
            )

            await self.request.app['flow'].process(task)
            return web.json_response(data={'result': task.image_processed})


    def prepare_app() -> web.Application:
        app = web.Application()

        app['flow'] = Flow(
            FlowStep(ImageHandler()),
        )
        app.router.add_post('/image_manipulation', ImageView)

        app['flow'].start()
        return app


    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        web.run_app(prepare_app(), loop=loop)
