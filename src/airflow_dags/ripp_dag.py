from datetime import timedelta

import requests
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, \
    DataprocSubmitJobOperator, DataprocDeleteClusterOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from cachetools import TTLCache, cached


@cached(cache=TTLCache(maxsize=1024, ttl=3600 - 30))
def get_token(base_url):
    metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

    token_request_url = metadata_server_token_url + base_url
    token_request_headers = {'Metadata-Flavor': 'Google'}

    # Fetch the token
    token_response = requests.get(token_request_url, headers=token_request_headers)
    jwt = token_response.content.decode("utf-8")
    return jwt


audience = "https://us-east1-bigdata-ripp.cloudfunctions.net/ripp-scraper"

TOKEN = get_token(audience)

default_args = {
    'owner': 'Amey Kolhe',
    'depends_on_past': False,
    'email': ['kolheamey99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id="ripp-dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args
)

scraping_task = SimpleHttpOperator(
    task_id='scraping_task',
    method='GET',
    http_conn_id='google_cloud_function_http_connection',
    endpoint='ripp-scraper?scrape_date={{ ds }}',
    headers={'Authorization': f"Bearer {TOKEN}", "Content-Type": "application/json"},
    dag=dag
)

CLUSTER_NAME = "ripp-dataproc-cluster-inc"
PROJECT = "bigdata-ripp"
BUCKET = "ripp-temp-bucket"
REGION = "us-east1"
ZONE = "us-east1-c"
SERVICE_ACCOUNT = "ripp-6@bigdata-ripp.iam.gserviceaccount.com"

cluster_config = ClusterGenerator(
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT,
    storage_bucket=BUCKET,
    region=REGION,
    zone=ZONE,
    master_machine_type="n1-standard-2",
    master_disk_size=50,
    worker_machine_type="n1-standard-2",
    worker_disk_size=50,
    num_workers=3,
    service_account=SERVICE_ACCOUNT,
    image_version="1.5",
    idle_delete_ttl=300,
    init_actions_uris=[
        f"gs://goog-dataproc-initialization-actions-{REGION}/connectors/connectors.sh",
        f"gs://goog-dataproc-initialization-actions-{REGION}/python/pip-install.sh"
    ],
    metadata={
        "spark-bigquery-connector-version": "0.21.1",
        "PIP_PACKAGES": "google-cloud-bigquery==3.13.0"
    },
    properties={
        "spark:spark.jars": "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
    }
).make()

cluster_creation_task = DataprocCreateClusterOperator(
    task_id="dataproc_cluster_creation_task",
    project_id=PROJECT,
    cluster_config=cluster_config,
    region=REGION,
    cluster_name=CLUSTER_NAME
)

cluster_deletion_task = DataprocDeleteClusterOperator(
    task_id="dataproc_cluster_deletion_task",
    project_id=PROJECT,
    region=REGION,
    cluster_name=CLUSTER_NAME
)

dohmh_most_recent_grade_task = DataprocSubmitJobOperator(
    task_id="dohmh_most_recent_grade_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_most_recent_grade.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_most_recent_grade_task = DataprocSubmitJobOperator(
    task_id="dohmh_most_recent_grade_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_most_recent_grade.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_cuisine_violation_analysis_task = DataprocSubmitJobOperator(
    task_id="dohmh_cuisine_violation_analysis_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_cuisine_violation_analysis.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_violations_per_year_analysis_task = DataprocSubmitJobOperator(
    task_id="dohmh_violations_per_year_analysis_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_violations_per_year_analysis.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_seasonal_analysis_task = DataprocSubmitJobOperator(
    task_id="dohmh_seasonal_analysis_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_seasonal_analysis.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_top_violations_per_boro_analysis_task = DataprocSubmitJobOperator(
    task_id="dohmh_top_violations_per_boro_analysis_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_top_violations_per_boro_analysis.py"},
    },
    region=REGION,
    project_id=PROJECT
)

dohmh_restaurant_chain_analysis_task = DataprocSubmitJobOperator(
    task_id="dohmh_restaurant_chain_analysis_task",
    job={
        "reference": {"project_id": PROJECT},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://ripp-code-bucket/pyspark/dohmh_restaurant_chain_analysis.py"},
    },
    region=REGION,
    project_id=PROJECT
)

scraping_task >> \
cluster_creation_task >> \
dohmh_most_recent_grade_task >> \
[
    dohmh_cuisine_violation_analysis_task,
    dohmh_violations_per_year_analysis_task,
    dohmh_seasonal_analysis_task,
    dohmh_top_violations_per_boro_analysis_task,
    dohmh_restaurant_chain_analysis_task
] >> \
cluster_deletion_task
