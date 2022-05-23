import asyncio

from flask import Flask, request

from .flow import Task, get_flow

loop = asyncio.get_event_loop()

app = Flask(__name__)
flow = get_flow()
flow.start()


@app.route("/", methods=['POST'])
def hello_world():
    image = request.get_data()
    task = Task(image)
    loop.run_until_complete(flow.process(task))
    return {'result': task.h_pred}
