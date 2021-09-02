# Example aiohttp project using Aqueduct
The service receives an image and returns the result of image classification.

## Contents

### `data/`
Any useful data.
### `demoapp/`
This is a project itself.
### `demoapp/app.py`
Builds and runs aiohttp web application.
### `demoapp/pipeline.py`
Stores data processing pipeline.
### `demoapp/flow.py`
Contains logic for wrapping pipeline steps into Aqueduct abstracts and logic for `Flow` creation.

## Run and test
Run python with installed requirements from `requirements.txt`.
```shell
python -m demoapp.app
```
To send request you can use `curl` utility
```shell
curl localhost:8080/classify --data-binary @data/apple.jpg
```