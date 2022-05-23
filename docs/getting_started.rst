Getting Started
===============

This is a simple example of using Aqueduct, for more advanced one see `examples <../examples/>`_.

.. code-block:: python

    from aiohttp import web
    from aqueduct import Flow, FlowStep, BaseTaskHandler, BaseTask


    class MyModel:
        """This is an example of a CPU-bound model"""

        def process(self, number):
            return sum(i * i for i in range(number))

    class Task(BaseTask):
        """Container for sending arguments to the model."""
        def __init__(self, number):
            super().__init__()
            self.number = number
            self.sum = None  # result will be here

    class SumHandler(BaseTaskHandler):
        """When using Aqueduct, we need to wrap your model."""
        def __init__(self):
            self._model = None

        def on_start(self):
            """Executed in a child process, so the parent process does not consume additional memory."""
            self._model = MyModel()

        def handle(self, *tasks: Task):
            """List of tasks because it can be batching."""
            for task in tasks:
                task.sum = self._model.process(task.number)


    class SumView(web.View):
        """Simple aiohttp-view handler"""

        async def post(self):
            number = await self.request.read()
            task = Task(int(number))
            await self.request.app['flow'].process(task)
            return web.json_response(data={'result': task.sum})


    def prepare_app() -> web.Application:
        app = web.Application()

        app['flow'] = Flow(
            FlowStep(SumHandler()),
        )
        app.router.add_post('/sum', SumView)

        app['flow'].start()
        return app


    if __name__ == '__main__':
        web.run_app(prepare_app())
