Web concurrency example
#######################

If you use aqueduct in web service, you may need to scale your service with web workers concurrency.
Usually the reason to use concurrency is main process bottle neck (i.e.: main process handles http requests, manages serialization/deserialization, data validation and convertion. When you use aqueduct it also manages aqueduct Flow instance and its utility functionality)
When you enable concurrency usually your scenario is to have multiple web concurrent workers and single `aqueduct.Flow` instance.
For this scenario we suggest to use `gunicorn` with `preload app` feature and stand alone process which communicates with web workers by Unix Domain Sockets and manages single Flow instance.
It handles tasks from all the workers. Here is the scheme:


.. image:: socket_scheme.png
   :alt: Scheme
   :width: 600px
   :align: center



Implementation manual
---------------------

Concurrency example with `gunicorn` + `FastAPI`

Here are steps to add concurrency to your service with aqueduct:


1. Install gunicorn (add to your project dependencies)
 `Guide <https://docs.gunicorn.org/en/latest/install.html>`__

 As an example we use `uvicorn` workers for gunicorn so if you follow the example you should also install uvicorn

.. code-block:: bash

    pip install uvicorn

2. Refactor your app to use SocketFlow

 Let's look at basic example

 Let's say we have `flow.py`:

 .. code-block:: python

    from aqueduct.flow import Flow, FlowStep
    from aqueduct.handler import BaseTaskHandler
    from aqueduct.task import BaseTask


    class Task(BaseTask):
        def __init__(self):
            super().__init__()
            self.result = None


    class Handler(BaseTaskHandler):
        def handle(self, *tasks: Task):
            for task in tasks:
                task.result = 'done'


    def build_flow() -> Flow:
        return Flow(
            FlowStep(
                Handler(),
            ),
        )

 And `main.py`:

 .. code-block:: python

    from typing import Optional

    from aqueduct.sockets.flow import SocketFlow
    from fastapi import FastAPI
    from pydantic import BaseModel

    from flow import Handler, Task, build_flow


    mp_flow = build_flow()

    # start Flow
    # initialize flow step processes. Initialize Flow utility (metrics, result awaiting, etc.)
    app = FastAPI(on_startup=[mp_flow.start])


    class GetResultResponse(BaseModel):
        result: Optional[str]


    @app.get('/get_result')
    async def get_result() -> GetResultResponse:
        task = Task()
        print(task.result)  # ''(None)
        await mp_flow.process(task)
        print(task.result)  # 'done'
        return GetResultResponse(result=task.result)


 All you need is to use SocketFlow wrapper instead of default Flow object. New `main.py` (`flow.py` is unchanged):

 .. code-block:: python

    from typing import Optional

    from aqueduct.sockets.flow import SocketFlow
    from fastapi import FastAPI
    from pydantic import BaseModel

    from flow import Handler, Task, build_flow


    socket_flow = SocketFlow()

    # start SocketFlow wrapper
    # initialize socket connection pool to communicate with flow socket server
    app = FastAPI(on_startup=[socket_flow.start])


    class GetResultResponse(BaseModel):
        result: Optional[str]


    @app.get('/get_result')
    async def get_result() -> GetResultResponse:
        task = Task()
        print(task.result)  # ''(None)
        await socket_flow.process([task])  # wrapper processes list of tasks to send batch via socket
        print(task.result)  # 'done'
        return GetResultResponse(result=task.result)

3. Define gunicorn config with preload-app option

 The key is gunicorn initialize service objects in gunicorn main process (which is parent process for all web workers where http requests are handled).

 So we can spawn our stand alone process with Flow here and it happens only once.

 Also gunicorn spawns web worker processes with `fork` method so all the business logic objects which we initialize in preload stage are initialized only once because of Copy-on-Write mechanism.

 `About preload app <https://docs.gunicorn.org/en/stable/settings.html#preload-app>`__

 `About on_starting hook <https://docs.gunicorn.org/en/21.2.0/settings.html#on-starting>`__

 `gunicorn_config.py` file:

.. code-block:: python

    import os
    import signal

    from main import build_flow, socket_flow

    # GUNICORN CONFIG
    bind = os.getenv('SERVICE_HOST', '0.0.0.0') + ':' + os.getenv('SERVICE_PORT', '8890')
    workers = os.getenv('WEB_CONCURRENCY', 2)
    worker_class = 'uvicorn.workers.UvicornWorker'
    wsgi_app = 'main:app'
    preload_app = True

    # PRELOAD APP
    flow_socket_server_proc_ctx = None

    def on_starting(server):
        global flow_socket_server_proc_ctx

        # initialize flow steps and flow socket server processes only once.
        flow_socket_server_proc_ctx = socket_flow.preload(build_flow())


    def on_exit(server):
        if flow_socket_server_proc_ctx:
            for process in flow_socket_server_proc_ctx.processes:
                os.kill(process.pid, signal.SIGKILL)


Run example
-----------

Now you can try to run your service

Old run command:

.. code-block:: bash

    uvicorn main:app

New run command:

.. code-block:: bash

    gunicorn -c gunicorn_config.py

