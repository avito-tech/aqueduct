# 1.10.14
- Route task to a specific worker queue according to handle_condition

# 1.10.13
- Default start timeout in aiohttp integration
- Check timeout when try to put value to queue
- Fix deadlock on exit when input queue has items

# 1.10.12
- Add a new async method share_value_with_data to the BaseTask class

# 1.10.11
- add support type bytes or bytearray for share memory in BaseTask

# 1.10.10
- Change name `on_start_timeout` to `on_start_wait`

# 1.10.9
- Add ability to set different subprocess start methods and on_start_timeout for heavyweight models

# 1.10.8
- Add metric for task size in bytes

# 1.10.7
- Add ability to set custom logger

# 1.10.6
- Add missing imports to aiohttp integration and add return from observe_flows

# 1.10.5
- fixed bad upload to pypi.org

# 1.10.4
- fixed unfinished stopping on child process death

# 1.10.3
- fixed reraise asyncio.CancelledError if the cause of exception is external actor

# 1.10.2
- fixed batch gatharing (batch was not always accumulate properly due to MP queue inconsistent behaviour)

# 1.10.1
- fixed batch time measurement

# 1.10.0
- added dynamic batching strategy (used if batch_timeout = 0)
- added `pulemet` as external dependency

# 1.9.0
- fix CPU consumption in subprocesses

# 1.8.1
- fix multiprocessing start method setting, imports order

# 1.8.0
- fix shared memory reference counting

# 1.7.1
- fix aiohttp integration (monitoring task is not affecting unit tests now)
- `Flow.run()` now waiting for all child processes to initialize (all `Handler.on_start()` methods guaranteed to be completed before `run` releases)

# 1.7.0
- tests migrated from pytest-asyncio to pytest-aiohttp
- added integration with aiohttp

# 1.6.1
- removed redundant files
- added `python_requires` parameter in `setup.py`
- added `examples` directory
- renamed `lib` directory to `aqueduct`

# 1.6.0
- added StatsDBuffer Protocol
- added metrics sending in the `maas` service in `tests/benchmarks/bench_service`
- reduced time interval for queue size metrics collection

# 1.5.0
- added benchmark tests
- moved unit tests

# 1.4.0
- added batching metrics: batch size and batch creating time
- metrics refactoring
- input and output Worker queues moved from `loop` to `__init__` method

# 1.3.0
- moved avio-specified metrics prefix from Flow to Exporter
- changed queue size metrics type from gauge to timers
- added metrics for tasks count on completed and failed
- added queue_size parameter

# 1.2.1
- fixed typing

# 1.2.0
- added multiprocess batching support

# 1.1.0
- added the ability to skip unsuitable for Worker tasks

# 1.0.0
- removed `async-timeout` requirement
- added batching mechanism to the `Worker` class
- changed `handle` method of the `BaseTaskHandler` class for tasks batch handling
- tests refactoring

# 0.9.0
- fixed segfault error
- added shared fields support in the BaseTask
- removed `to_dict` BaseTask method

# 0.8.1
- fixed segmentation fault problems due to task update

# 0.8.0
- source task now updating with result
- process() method returns True if work on task completed 

# 0.7.4
- `need_collect_task_timers` Flow property changed to cached_property
-  added the ability to disable the collecting metrics

# 0.7.3
- fixed unnecessary shared memory warnings

# 0.7.2
- fixed asleep main process due to huge count of tasks

# 0.7.1
- changed perf tests
- renamed `SharedData` method from `get_from_data` to `create_from_data`
- fixed ref counter

# 0.7.0
- added shared memory tools

# 0.6.0
- python version raised to `3.8.6`

# 0.5.3
- added child processes explicit killing

# 0.5.2
- changed Flow `start` method from async to sync
- added Flow `is_running` property

# 0.5.1
- removed event loop stopping after Flow stopping
- added StopTask forwarding to all steps

# 0.5.0
- updated multiprocessing module
- added environment checks
- revised Aqueduct errors
- removed pytorch dependency
- revised Flow stopping

# 0.4.1
- change queues to multiprocessing.Queue

# 0.4.0
- add processes liveness checking

# 0.3.1
- fixed `_task_states` clearing

# 0.3.0
- add timeout_sec for process method
- spawner moved from Torch and tuned for logger integration like Sentry
- new few  `__str__` and `__repr__` for more readable exceptions context 
- add some graceful terminating

# 0.2.4
- change queue size metrics type from counter to gauge

# 0.2.3
- removed text-detector specified code from Flow

# 0.2.2
- not None _metrics_manager field in Flow
- change metrics package structure

# 0.2.1
- add MetricsManager to encapsulate collect and export logic

# 0.2.0
- add queue size metrics
- add metrics prefixes
- add metrics export config

# 0.1.2
- add step name to Worker name
- change TaskMetrics.timers from dict to list

# 0.1.1
- change timer function from time() to monotonic()

# 0.1.0
- add timer metrics for task transfer and task handling

# 0.0.1
- first version based on shared memory via pytorch multiprocessing modules
