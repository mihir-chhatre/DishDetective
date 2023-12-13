import pyspark.sql.functions as F
from google.cloud import bigquery
from pyspark.sql import SparkSession

table_id = 'bigdata-ripp.ripp_analytics.dohmh_restaurant_chain_analysis'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Restaurant Chain Analysis') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

# Reading 'most_recent_letter_grade' from BQ
most_recent_letter_grade = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .load()

# First, calculate the total number of locations (distinct CAMIS) for each DBA
total_locations_per_dba = most_recent_letter_grade.groupBy("dba") \
    .agg(F.countDistinct("camis").alias("TotalLocations"))

# Group by DBA and VIOLATION CODE, count distinct CAMIS, DBA pairs
grouped_data = most_recent_letter_grade.groupBy("dba", "violation_code") \
    .agg(F.countDistinct("camis", "dba").alias("DistinctCount")) \
    .filter(F.col("DistinctCount") > 1)

# Join the total locations data with the grouped data
result = grouped_data.join(total_locations_per_dba, "dba")

result.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_restaurant_chain_analysis') \
    .save()
