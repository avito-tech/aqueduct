========
Aqueduct
========

Framework for performance-efficient prediction.

Key Benefits
============

- Increases the throughput of your machine learning-based service
- Uses shared memory for instantaneous transfer of large amounts of data between processes
- All optimizations in one library

Getting Started
===========

This is a simple example of using Aqueduct, for more advanced one see `examples <examples/>`_.

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
    

Batching
========

Aqueduct supports tasks processing in batches.
To use batching, you just need to specify the `batch_size` parameter, which indicates how many tasks can be in a batch.
You can determine the correct `batch_size` for your handler by manually measuring the performance of the handler
for different batch sizes.

.. code-block:: python

	import asyncio
	import time
	from typing import List

	import numpy as np

	from aqueduct.flow import Flow, FlowStep
	from aqueduct.handler import BaseTaskHandler
	from aqueduct.task import BaseTask

	# this constant is only needed for an example
	TASKS_BATCH_SIZE = 20


	class ArrayFieldTask(BaseTask):
		def __init__(self, array: np.array, *args, **kwargs):
			super().__init__(*args, **kwargs)
			self.array = array
			self.result = None


	class CatDetector:
		"""GPU model emulator that predicts the presence of a cat in the image."""
		IMAGE_PROCESS_TIME = 0.01
		BATCH_REDUCTION_FACTOR = 0.7
		OVERHEAD_TIME = 0.02
		BATCH_PROCESS_TIME = IMAGE_PROCESS_TIME * TASKS_BATCH_SIZE * BATCH_REDUCTION_FACTOR + OVERHEAD_TIME

		def predict(self, images: np.array) -> np.array:
			"""Always says that there is a cat in the image.

			The image is represented by a one-dimensional array.
			The model spends less time processing a batch of images due to GPU optimizations. It's emulated
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

	# checks if there are no results
	assert not any(task.result for task in tasks_batch)
	# task handling takes 0.16s which is less than sequential task processing in 0.22s
	await asyncio.wait_for(
		process_tasks(flow_with_batch_handler, tasks_batch),
		timeout=CatDetector.BATCH_PROCESS_TIME,
	)
	# checks if all results are set
	assert all(task.result for task in tasks_batch)

	await flow_with_batch_handler.stop()

	tasks_batch = get_tasks_batch()
	flow_with_batch_handler = Flow(
		FlowStep(CatDetectorHandler(), batch_size=2*TASKS_BATCH_SIZE)
	)
	flow_with_batch_handler.start()

	await asyncio.wait_for(
		process_tasks(flow_with_batch_handler, tasks_batch),
		timeout=CatDetector.BATCH_PROCESS_TIME + 0.01,
	)

	await flow_with_batch_handler.stop()


Aqueduct (by default) does not guarantee that the handler will always receive a batch of the exact size.
It may be less than the `batch_size`, but never exceed it.
This is because we are not waiting for the batch to be fully assembled.
This allows us to avoid overhead in low-load scenarios and, on the other hand,
if input requests are frequent enough, the real batch size will always be equal to the `batch_size`.
If you found your handler performs better with a specific, exact batch size,
you can use the optional `batch_timeout` parameter to limit the time of batch formation.

Sentry
======

Aqueduct allows you to receive logger events from workers and the main process.

To integrate with `Sentry <https://sentry.io>`_, you need to write something like this:

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

Contact Us
=======

Feel free to ask questions in Telegram: `t.me/avito-ml <https://t.me/avito_ml>`_
