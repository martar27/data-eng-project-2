# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.7.2

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for connections. Connection id will be the string after AIRFLOW_CONN_ in lowercase.
ENV AIRFLOW_CONN_DWH_PG='postgres://dwh_user:dwh_user@dwh_pg:5432/dwh_pg'
ENV AIRFLOW_CONN_DATA_DIR='{"conn_type": "File"}'
ENV AIRFLOW_CONN_DWH_NEO4J='neo4j://neo4j:dwh_user@dwh_neo4j:7687/neo4j'

# Create the /home/airflow/.kaggle directory. Kaggle python package needs this to exist regardless of how we show it our credentials
RUN mkdir -p /home/airflow/.kaggle
RUN echo '{"username": "dummy_username", "key": "dummy_key"}' > /home/airflow/.kaggle/kaggle.json
RUN chmod 600 /home/airflow/.kaggle/kaggle.json
