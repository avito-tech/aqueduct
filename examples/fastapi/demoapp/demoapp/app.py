from fastapi import (
    Depends,
    FastAPI,
    Request,
)

from aqueduct.integrations.fastapi import (
    AppIntegrator,
    get_flow,
)
from .flow import (
    Flow,
    Task,
    get_flow as build_flow,
)


def prepare_app() -> FastAPI:
    app = FastAPI()

    @app.post('/classify')
    async def classify(request: Request, flow: Flow = Depends(get_flow())):
        im = await request.body()
        task = Task(im)
        await flow.process(task)
        return {'result': task.h_pred}

    AppIntegrator(app).add_flow(build_flow())

    return app


app = prepare_app()


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=8080)
