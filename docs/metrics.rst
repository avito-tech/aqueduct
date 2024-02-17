Metrics
=======

Description
-----------

Aqueduct has a lot of metrics to help you monitor and debug your application

Metrics collected for each ``Flow`` step

Here is all metrics collected by aqueduct:

* ``transfer_time`` - How much time task transferred between Flow steps (timing metric)
* ``task_size`` - Task size in bytes (timing metric)
* ``handle_time`` - Time spend to handle task (timing metric)
* ``batch_time`` - Time spend to collect batch of tasks (timing metric)
* ``batch_size`` - Batch size. Useful when using dynamic batching (timing metric)
* ``qsize`` - Queue size (timing metric)
* ``tasks`` - Total task count (count metric)
* ``memory_usage`` - Memory usage at each flow step (timing metric)
  

Aqueduct supports StatsD metrics format via ``ToStatsDMetricsExporter`` class

Metrics are constructed by concatenating several parts

* **constant string** - **aqueduct**
* **user specified prefix** (optional) - example: ``service``, ``app``, etc.
* **metric** - example: ``transfer_time``, ``batch_time``, etc.
* **metric name** - example: ``from_main_process_to_step1_PreprocessorHandler``, ``step1_PreprocessorHandler``, etc.

Example StatsD metrics

* ``aqueduct.transfer_time.from_step2_ModelHandler_to_main_process``
* ``aqueduct.app_custom_prefix.qsize.from_main_process_to_step1_PreprocessorHandler``
* ``aqueduct.app_custom_prefix.handle_time.step1_PreprocessorHandler``


Usage
-----

To use ``ToStatsDMetricsExporter`` you have to provide StatsD client complient with ``StatsDBuffer`` protocol

.. code-block:: python

    import statsd

    from typing import Union

    from aqueduct import Flow, FlowStep
    from aqueduct.metrics import ToStatsDMetricsExporter
    from aqueduct.metrics.export import StatsDBuffer


    class AqueductStatsd(StatsDBuffer):
        def __init__(self):
            self.statsd_client = statsd.StatsClient('localhost', 8125)

        def count(self, name: str, value: Union[float, int]):
            self.statsd_client.incr(name, value)

        def timing(self, name: str, value: Union[float, int]):
            self.statsd_client.timing(name, value)

        def gauge(self, name: str, value: Union[float, int]):
            self.statsd_client.gauge(name, value)
    
    def create_flow() -> Flow:
        aqueduct_statsd = AqueductStatsd()
        return Flow(
            FlowStep(
                SumHandler()
            ),

            # Here we pass our metrics exporter and use our implementation of StatsDBuffer
            metrics_exporter=ToStatsDMetricsExporter(
                target=aqueduct_statsd,
                prefix='app_custom_prefix'
            ),
        )
