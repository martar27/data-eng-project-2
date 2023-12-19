# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.7.2

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variables for connections
ENV AIRFLOW_CONN_DWH_PG='postgres://dwh_user:dwh_user@dwh_pg:5432/dwh_pg'
ENV AIRFLOW_CONN_DATA_DIR='{"conn_type": "File"}'


ENV KAGGLE_USERNAME "dummy_username"
ENV KAGGLE_KEY "dummy_key"

# Create the /home/airflow/.kaggle directory
RUN mkdir -p /home/airflow/.kaggle

# Create an empty kaggle.json file in /home/airflow/.kaggle
# Kaggle python package needs this to exist regardless of how we show it our credentials
RUN echo '{"username": "dummy_username", "key": "dummy_key"}' > /home/airflow/.kaggle/kaggle.json
RUN chmod 600 /home/airflow/.kaggle/kaggle.json
