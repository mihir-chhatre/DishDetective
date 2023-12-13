#!/bin/zsh

CLUSTER_NAME="ripp-dataproc-cluster"
REGION="us-east1"

gcloud dataproc jobs submit pyspark dohmh_most_recent_grade.py --cluster ${CLUSTER_NAME} --region ${REGION}
gcloud dataproc jobs submit pyspark dohmh_cuisine_violation_analysis.py --cluster ${CLUSTER_NAME} --region ${REGION}
gcloud dataproc jobs submit pyspark dohmh_violations_per_year_analysis.py --cluster ${CLUSTER_NAME} --region ${REGION}
gcloud dataproc jobs submit pyspark dohmh_seasonal_analysis.py --cluster ${CLUSTER_NAME} --region ${REGION}
gcloud dataproc jobs submit pyspark dohmh_top_violations_per_boro_analysis.py --cluster ${CLUSTER_NAME} --region ${REGION}
gcloud dataproc jobs submit pyspark dohmh_restaurant_chain_analysis.py --cluster ${CLUSTER_NAME} --region ${REGION}
