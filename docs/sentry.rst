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