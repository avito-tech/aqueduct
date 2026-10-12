# Example FastAPI project using Aqueduct
The service receives an image and returns the result of image classification.

## Contents

### `data/`
Any useful data.
### `demoapp/`
This is a project itself.
### `demoapp/app.py`
Builds and runs FastAPI web application.
### `demoapp/pipeline.py`
Stores data processing pipeline.
### `demoapp/flow.py`
Contains logic for wrapping pipeline steps into Aqueduct abstracts and logic for `Flow` creation.

## Run and test

Cd to directory
```shell
cd examples/fastapi/demoapp
```

Run python with installed requirements from `requirements.txt`.
```shell
python -m demoapp.app
```
or with `uvicorn` directly
```shell
uvicorn demoapp.app:app --port 8080
```
To send request you can use `curl` utility
```shell
curl localhost:8080/classify --data-binary @data/apple.jpg
```

## How it works

`AppIntegrator(app).add_flow(flow)` forks the `Flow` step processes immediately (so the models are
loaded before the server starts accepting connections) and hooks into the application lifespan:
on startup the `Flow` is started, on shutdown it is stopped. If a `Flow` dies, the web server process
is terminated with `SIGTERM`, so a process manager (`gunicorn`, `systemd`, Kubernetes, ...) can restart it.

The `Flow` is available in handlers via `Depends(get_flow())` or as `request.app.state.flow`.

## Several web workers

Do not run this example with `uvicorn --workers N` (or `gunicorn` without `preload_app`): every worker
imports the application module and creates its own `Flow` with its own copy of the models.
To share a single `Flow` between several web workers use `SocketFlow`,
see [Web Concurrency Example](../../../docs/concurrency.rst).
