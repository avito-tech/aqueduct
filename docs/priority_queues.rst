Priority queues
===============

Aqueduct supports tasks with priorities.
To achieve this Aqueduct creates parallel queues and gets tasks from queues with higher priority first.

To use this feature you need

- Set how many priorities you are gonna need in ``Flow`` instance by setting up ``queue_priorities`` to ``2`` or higher
- Set task priority using task method ``set_priority`` before sending it to ``process`` function
- Task priority should be between ``0`` and ``queue_priorities - 1``. Higher value higher priority

Example
-------
.. code-block:: python

    class Task(BaseTask):
        result: Optional[int]
        def __init__(self):
            super().__init__()
            self.result = None

    class ProcessorHandler(BaseTaskHandler):
        def handle(self, *tasks: Task):
            for task in tasks:
                task.result = 42

    def get_flow() -> Flow:
        return Flow(
            FlowStep(ProcessorHandler()),
            metrics_enabled=False,
            
            # Set how many priorities you are gonna need
            queue_priorities=2,
        )

    async def main():
        flow = get_flow()
        flow.start()

        normal_task = Task()

        important_task = Task()

        # Task priority should be between 0 and queue_priorities - 1
        important_task.set_priority(1)

        normal_future = asyncio.create_task(flow.process(normal_task))
        normal_future.add_done_callback(lambda _: print('normal task finished'))

        important_future = asyncio.create_task(flow.process(important_task))
        important_future.add_done_callback(lambda _: print('important task finished'))
        await asyncio.gather(normal_future, important_future)

    if __name__ == '__main__':
        asyncio.run(main())