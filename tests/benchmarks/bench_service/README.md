# Service benchmarks
It allows you to conduct benchmarking. It means that following the current instructions you can run 
`bench/bench_steps.py` and `bench/bench_configurations.py` scripts.
Tests require GPU with CUDA support. 

1. Pass aqueduct sources in current directory
    ```shell
    ln -s `pwd`/../../../aqueduct ./aqueduct
    ```
1. Install the specific version of docker-compose. It can be done by commands:
    ```shell
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" \
    -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    
    # check version, you should see 1.29.2
    docker-compose --version
    ```
1. To optimize service start you need download models files in advance
    ```shell
    mkdir maas/data/models && cd maas/data && sh get_models.sh
    ```
1. Build and run container for benchmarks
    ```shell
    docker-compose up -d maas-bench
    ```
1. Run service steps benchmark
    ```shell
    docker exec maas_bench_$USER python -m bench.bench_steps
    ```
1. Run service configurations benchmark (use the desired config)
    ```shell
    python3 -m bench.bench_configurations bench/configs/image_small_3step.json
    ```
1. Stop and remove benchmark containers
    ```shell
    docker-compose down
    ```