Logging
=======

Default logging
^^^^^^^^^^^^^^^

Aqueduct has default preconfigured logger.

.. code-block:: python

    from aqueduct.logger import log

Default logger uses ``StreamHandler`` and writes logs to ``stderr``

Default logging level is ``DEBUG``


Adjust default logger
^^^^^^^^^^^^^^^^^^^^^

You can adjust any settings of the logger by modifying it directly.
For example let's add ``RotatingFileHandler`` to aqueduct logger and change default logging level

.. code-block:: python

    import logging

    from logging.handlers import RotatingFileHandler
    from aqueduct.logger import log

    log.setLevel(logging.INFO)

    handler = RotatingFileHandler('my_log.log', maxBytes=2000, backupCount=10)
    log.addHandler(handler)


Another example is adding `sentry handler <sentry.rst>`_ to aqueduct logger.


Use your own logger
^^^^^^^^^^^^^^^^^^^

You can replace default aqueduct logger with your own.

.. code-block:: python

    import logging

    from logging.handlers import RotatingFileHandler
    from aqueduct.logger import replace_logger

    custom_logger = logging.getLogger(__name__)
    replace_logger(custom_logger)


Aiohtt integration
^^^^^^^^^^^^^^^^^^

If you use aiohttp integration (`example <../examples/aiohttp>`_), you can pass your own logger to ``AppIntegrator``

.. code-block:: python

    import logging

    from aiohttp import web

    from aqueduct.integrations.aiohttp import AppIntegrator
    from .flow import get_flow

    custom_logger = logging.getLogger(__name__)

    def prepare_app() -> web.Application:
        app = web.Application(client_max_size=0)
        # Here we use logger parameter to setup our custom logger for aqueduct
        AppIntegrator(app).add_flow(get_flow(), logger=custom_logger)
        return app

    if __name__ == '__main__':
        web.run_app(prepare_app())
