========
F.A.Q.
========


Can I save money on hardware by using an aqueduct in my model?
--------------------------------------------------------------

Yes it is what `aqueduct` for. We have reduced hardware consumption by 200% in some cases. It means that we increased the amount of calculations by 200% without adding new hardware.


What difference between `ProcessPoolExecutor`?
----------------------------------------------

`ProcessPoolExecutor` is good and simple way to create subprocesses for cpu-bound operations.
It is best for run monolithic model in subprocess.
But when you splitting model into steps, move this steps to subprocesses and trying to make way to send parameters between
supbrocesses then you reinventing aqueduct.

You can look at `aqueduct` like it is `ProcessPoolExecutor` but with some additional features:

- compatible with asyncio
- metrics for use in production
- subprocess liveness detection
- shared memory for sending data to subprocesses
- integrated automatically creating queues for transfer data to subprocesses
- some fixes on python interpreter queues bad behavior


Why not using Kafka, RabbitMQ, Pulsar or any other messaging queue instead of interprocess queues?
--------------------------------------------------------------------------------------------------
Because of speed. We use aqueduct when we need both speed of inference and hardware economy. Using external messaging
queue making inference very slow. Also, it is bad pattern when you send big objects with messaging brokers.


Does it make sense in FlowStep, in which subscription model on gpu do nprocs > 1?
--------------------------------------------
It is technically possible to do this, but it really makes no sense.
Step with GPU is usually a heavy model and should be scaled along with the harness. As a result, such an increase will
significantly increase the consumption of memory and cores for the instance. It is better to do such scaling through
the number of service pods if you have kubernetes or in the general case through the number of instances, and not
through the number of processes per step with the model. So you will not grow the size of the pod and it will be
easier to deploy.


What happens if one of subprocesses is dead?
--------------------------------------------


Why use `aqueduct.logger` instead of ?
--------------------------------------

```
from aqueduct.logger import log as aqueduct_logger
```


Can I replace model while service is working, without stopping service?
-----------------------------------------------------------------------
