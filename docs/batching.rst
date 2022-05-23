Batching
========

Batching is best way to improve performance of model.
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
