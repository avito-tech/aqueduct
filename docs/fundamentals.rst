Fundamentals
############

Aqueduct is a tool to run CPU-bound tasks in parallel.
It is like ``ProcessPoolExecutor`` but with `additional features <faq.rst>`_, which simplifies creating complex processing pipelines.
Aqueduct takes control of creating/managing os processes and data transfer between them.

You can create multi-step pipelines and each step can have several os processes.
Aqueduct allows you to use shared memory between all processes effortlessly.


Flow
****
``Flow`` is the main class that represents the processing pipeline.
This class manages the pipeline's life cycle.

- Creates all processing steps
- Creates queues between Flow steps
- Provides an interface to send tasks for processing and to receive results
- Collects metrics
- Monitors os processes health

Flow constructor arguments
==========================

- ``metrics_enabled``  - whether to enable metrics
- ``metrics_exporter`` - class that allows to export `metrics <metrics.rst>`_
- ``queue_size`` - the size of queues between ``FlowSteps``. If ``queue_size`` is not specified, then aqueduct calculates the queue size for each step depending on the step's batch size, but minimal 20 tasks
- ``mp_start_method`` - Start method for `process creation <https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods>`_ (``spawn``, ``fork``, ``forkserver``)

Flow main methods
=================
- ``start`` - starts pipeline. Accepts optional ``timeout`` argument. If the pipeline doesn't manage to start before timeout, then ``TimeoutError`` will be raised
- ``async stop`` - stops pipeline
- ``async process`` - sends ``Task`` for processing by pipeline. Optionally has argument ``timeout_sec`` (5 seconds by default)


FlowStep
********
Each ``Flow`` consists of one or more ``FlowStep``. ``FlowStep`` represents one processing step.

FlowStep constructor arguments
==============================

- ``handler`` - FlowStep Handler instance inherited from  ``BaseTaskHandler``. Contains your custom processing code. Usually some model inference but it can be any CPU-heavy function
- ``handle_condition`` - function predicate that returns a boolean and determines if the task should be processed by the step
- ``nprocs`` - number of os processes used for the step. By default, it is 1
- ``batch_size`` - the size of the step's batch. `More about batching <batching.rst>`_
- ``batch_timeout`` - timeout for batch to be collected


Task
****
To send data for processing and get results you need to use a ``Task``. ``Task`` should be inherited from ``BaseTask`` class.

Example
=======

.. code-block:: python

    from aqueduct import BaseTask

    class MyTask(BaseTask):
        number: int
        result: Optional[int]

        def __init__(self, number: int):
            super().__init__()
            self.number = number
            self.result = None

``Flow``'s method ``async process`` accepts ``Task`` as an argument but it doesn't return the result of processing. You have to add a field to your custom ``Task`` (for example ``result``) and use it to save the result

Shared memory fields
=====================
Aqueduct simplifies the use of shared memory between steps (os processes).
You first should create a ``Task`` class inherited from ``BaseTask``. Add field of type ``bytes``, ``bytearray`` or numpy array.
And before sending the task to the ``process`` method you should call the task's method ``share_value`` with the name of the field you want to move to shared memory

Example
-------
.. code-block:: python

    from aqueduct import BaseTask

    class MyTask(BaseTask):
        image: bytes
        result: Optional[int]

        def __init__(self, image: bytes):
            super().__init__()
            self.image = image
            self.result = None
    
    image_data = b''
    with open('image.jpg', 'br') as img_file:
        image_data = img_file.read()
    task = MyTask(image=image_data)
    task.share_value('image')
    await flow.process(task)


Handler
*******
``Handler`` is a class inherited from ``BaseTaskHandler`` and contains your custom processing code.
``Handler`` is an argument of FlowStep and contains all the logic of the step.

Handler's main methods to override
================================
- ``on_start`` - this method runs when the worker process is started. Here you put all the code for loading your models. Executed in a child process, so the parent process does not consume additional memory.
- ``handle`` - accepts several tasks ``*tasks: BaseTask`` (because batching can send multiple tasks simultaneously) and here you write all processing logic

Example
-------
.. code-block:: python

    class SumHandler(BaseTaskHandler):
        def __init__(self):
            self._model = None

        def on_start(self):
            self._model = MyModel()

        def handle(self, *tasks: Task):
            for task in tasks:
                task.result = self._model.process(task.number)

`Complete example <example.rst>`_
