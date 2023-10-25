## Setup

Refere to [airflow docker compose docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more information

### Initial setup

This will create all the necessary folders, initialize airflow containers and create postgres db with project schema.

1. Make a copy of .env.sample and rename it to .env. Make any overrides you need for compose there

    ```bash
    cp .env.sample .env
    ```

2. Create airflow folders

    ```bash
    mkdir -p ./airflow/dags ./airflow/logs ./airflow/plugins ./airflow/config ./airflow/data
    ```

3. Initialize airflow

    ```bash
    docker compose up airflow-init
    ```

4. Install python 3.11 dependencies (recommended to use a virtual environment) for development

    ```bash
    pip install -r requirements.txt
    ```

### Running locally

Run docker compose

```bash
docker compose up -d
```

To clean up everything

```bash
docker compose down --volumes --remove-orphans --network
```
