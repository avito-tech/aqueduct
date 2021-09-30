========
Aqueduct
========

Framework for performance-efficient prediction.

Contact
=======

Feel free to ask questions in telegram `t.me/avito-ml <https://t.me/avito_ml>`_

Key Features
============

- Increase RPS (Requests Per Second) for your service
- All optimisations in one library
- Uses shared memory for transfer big data between processes

Get started
===========

Simple example how to start with aqueduct using aiohttp. For better examples see `examples <examples/>`_.

.. code-block:: python

    from aiohttp import web
    from aqueduct import Flow, FlowStep, BaseTaskHandler, BaseTask
    
    
    class MyModel:
        """This is CPU bound model example."""
        
        def process(self, number):
            return sum(i * i for i in range(number))
    
    class Task(BaseTask):
        """Container to send arguments to model."""
        def __init__(self, number):
            super().__init__()
            self.number = number
            self.sum = None  # result will be here
        
    class SumHandler(BaseTaskHandler):
        """With aqueduct we need to wrap you're model."""
        def __init__(self):
            self._model = None
    
        def on_start(self):
            """Runs in child process, so memory no memory consumption in parent process."""
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
    

Batching
========

Aqueduct supports the ability to process tasks with batches. Default batch size is one.

.. code-block:: python

	import asyncio
	import time
	from typing import List

	import numpy as np

	from aqueduct.flow import Flow, FlowStep
	from aqueduct.handler import BaseTaskHandler
	from aqueduct.task import BaseTask

	# this constant needs just for example
	TASKS_BATCH_SIZE = 20


	class ArrayFieldTask(BaseTask):
		def __init__(self, array: np.array, *args, **kwargs):
			super().__init__(*args, **kwargs)
			self.array = array
			self.result = None


	class CatDetector:
		"""GPU model emulator that predicts the presence of the cat in the image."""
		IMAGE_PROCESS_TIME = 0.01
		BATCH_REDUCTION_FACTOR = 0.7
		OVERHEAD_TIME = 0.02
		BATCH_PROCESS_TIME = IMAGE_PROCESS_TIME * TASKS_BATCH_SIZE * BATCH_REDUCTION_FACTOR + OVERHEAD_TIME

		def predict(self, images: np.array) -> np.array:
			"""Always says that there is a cat in the image.

			The image is represented by a one-dimensional array.
			The model spends less time for processing batch of images due to GPU optimizations. It's emulated
			with BATCH_REDUCTION_FACTOR coefficient.
			"""
			batch_size = images.shape[0]
			if batch_size == 1:
				time.sleep(self.IMAGE_PROCESS_TIME)
			else:
				time.sleep(self.IMAGE_PROCESS_TIME * batch_size * self.BATCH_REDUCTION_FACTOR)
			return np.ones(batch_size, dtype=bool)


	class CatDetectorHandler(BaseTaskHandler):
		def handle(self, *tasks: ArrayFieldTask):
			images = np.array([task.array for task in tasks])
			predicts = CatDetector().predict(images)
			for task, predict in zip(tasks, predicts):
				task.result = predict


	def get_tasks_batch(batch_size: int = TASKS_BATCH_SIZE) -> List[BaseTask]:
		return [ArrayFieldTask(np.array([1, 2, 3])) for _ in range(batch_size)]


	async def process_tasks(flow: Flow, tasks: List[ArrayFieldTask]):
		await asyncio.gather(*(flow.process(task) for task in tasks))


	tasks_batch = get_tasks_batch()
	flow_with_batch_handler = Flow(FlowStep(CatDetectorHandler(), batch_size=TASKS_BATCH_SIZE))
	flow_with_batch_handler.start()

	# checks if no one result
	assert not any(task.result for task in tasks_batch)
	# task handling takes 0.16 secs that is less than sequential task processing with 0.22 secs
	await asyncio.wait_for(
		process_tasks(flow_with_batch_handler, tasks_batch),
		timeout=CatDetector.BATCH_PROCESS_TIME,
	)
	# checks if all results were set
	assert all(task.result for task in tasks_batch)

	await flow_with_batch_handler.stop()

	# if we have batch size more than tasks number, we can limit batch accumulation time
	# with timeout parameter for processing time optimization
	tasks_batch = get_tasks_batch()
	flow_with_batch_handler = Flow(
		FlowStep(CatDetectorHandler(), batch_size=2*TASKS_BATCH_SIZE, batch_timeout=0.01)
	)
	flow_with_batch_handler.start()

	await asyncio.wait_for(
		process_tasks(flow_with_batch_handler, tasks_batch),
		timeout=CatDetector.BATCH_PROCESS_TIME + 0.01,
	)

	await flow_with_batch_handler.stop()


Sentry
======

The implementation allows you to receive logger events from the workers and the main process.
To integrate with __Sentry__, you need to write something like this:

.. code-block:: python

	import logging
	import os

	from raven import Client
	from raven.handlers.logging import SentryHandler
	from raven.transport.http import HTTPTransport

	from aqueduct.logger import log


	if os.getenv('SENTRY_ENABLED') is True:
		dsn = os.getenv('SENTRY_DSN')
		sentry_handler = SentryHandler(client=Client(dsn=dsn, transport=HTTPTransport), level=logging.ERROR)
		log.addHandler(sentry_handler)
