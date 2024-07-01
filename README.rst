========
Aqueduct
========

Framework to make youre prediction performance-efficient and scalable.

Key Benefits
============

- Increases the throughput of your machine learning-based service
- Uses shared memory for instantaneous transfer of large amounts of data between processes
- All optimizations in one library
- Supports multiple frameworks

Documentation
=============

- `Videos about aqueduct <docs/video.rst>`_
- Getting started
- - `Fundamentals <docs/fundamentals.rst>`_
- - `Example <docs/example.rst>`_
- `Batching <docs/batching.rst>`_
- `Logging <docs/logging.rst>`_
- `Metrics <docs/metrics.rst>`_
- `F.A.Q. <docs/faq.rst>`_
- Additional features
- - `Sentry support <docs/sentry.rst>`_

Examples
========

- `Aiohttp <examples/aiohttp/>`_
- `Flask <examples/flask/>`_

Installation
=============

Install using ``pip``:

.. code-block:: shell

    pip install aqueduct

Moreover, aqueduct has "optional extras"

- ``numpy`` - support types from numpy in shared memory
- ``aiohttp`` - extension for aiohttp support(see more in examples)

.. code-block:: shell

    pip install aqueduct[numpy,aiohttp]


Contact Us
==========

Feel free to ask questions in Telegram: `t.me/avito-ml <https://t.me/avito_ml>`_
