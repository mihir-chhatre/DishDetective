from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

table_id = 'bigdata-ripp.ripp_analytics.dohmh_top_violations_per_boro'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Top Violations per Boro') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

# Reading 'most_recent_letter_grade' from BQ
most_recent_letter_grade = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .load()

windowSpec = Window.partitionBy("boro").orderBy(col("count").desc())
top_violations_per_boro = most_recent_letter_grade.groupBy("violation_code", "boro").count() \
    .withColumn("rank", rank().over(windowSpec)) \
    .filter(col("rank") <= 5)  # Top 5 violations

top_violations_per_boro.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_top_violations_per_boro') \
    .option("writeMethod", "overwrite") \
    .save()
