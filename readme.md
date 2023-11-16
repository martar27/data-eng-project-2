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
4. Run liquibase update. Execute from project root directory

    ```bash
     ./liquibase/update.sh
    ```

5. Install python 3.11 dependencies (recommended to use a virtual environment) for development

    ```bash
    pip install -r requirements.txt
    ```

### Running locally

Run docker compose

```bash
docker compose up -d
```

To stop everything

```bash
docker compose stop
```

To clean project schema

```bash
./liquibase/dropProject.sh
```

To clean up everything

```bash
docker compose down --volumes --remove-orphans
```

## DW schema

### Fact

#### Submission
* id
* versionId (FK version)
* summaryId (FK summary)
* submitterId (FK author)
* paperId
* date
* title

### Dimensions

#### Version
* id
* number
* creationDate

#### Author
* id
* name
* email
* affiliation

#### Author to submission
* authorId (FK author)
* submissionId (FK submission)

#### Citation
* id
* title
* year
* publisher

#### Citation to submission
* citationId (FK citation)
* submissionId (FK submission)

#### Author to citation
* authorId (FK author)
* citationId (FK citation)

#### Summary
* id
* pages
* figures
* category
* abstract


### Graph

#### Paper
* id
* title

#### Author
* name
* email

#### Authored Author -> Paper 
* date

#### Submitted Author -> Paper
* date

## Pipeline

1. File watch to trigger pipeline
2. Python task to read line by line from file and deserialize to object, divide to batch of objects
3. Python task to filter out objects that are not valid
4. Python task to transform object to star schema model
5. Python task to transform object to graph db model
6. Python task to enrich from external source
7. Postgres insert task
8. Neo4j insert task
