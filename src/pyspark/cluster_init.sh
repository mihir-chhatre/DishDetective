#!/bin/bash

CLUSTER_NAME="ripp-dataproc-cluster"
PROJECT="bigdata-ripp"
BUCKET="ripp-temp-bucket"
REGION="us-east1"
ZONE="us-east1-c"
SERVICE_ACCOUNT="ripp-6@bigdata-ripp.iam.gserviceaccount.com"


gcloud beta dataproc clusters create ${CLUSTER_NAME} \
--project ${PROJECT} \
--bucket ${BUCKET} \
--temp-bucket ${BUCKET} \
--region ${REGION} \
--zone ${ZONE} \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 50 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 50 \
--service-account ${SERVICE_ACCOUNT} \
--image-version=1.5 \
--initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh,gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
--metadata spark-bigquery-connector-version=0.21.1 \
--metadata PIP_PACKAGES="google-cloud-bigquery==3.13.0" \
--properties=^#^spark:spark.jars='gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.34.0.jar'