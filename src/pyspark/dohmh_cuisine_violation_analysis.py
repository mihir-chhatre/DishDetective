import pyspark.sql.functions as F
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

table_id = 'bigdata-ripp.ripp_analytics.dohmh_cuisine_violation_analysis'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Cuisine Violation Analysis') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

# Reading 'most_recent_letter_grade' from BQ
most_recent_letter_grade = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .load()

cuisine_violation_analysis = (
    most_recent_letter_grade.groupBy("cuisine_description", "violation_code")
    .count()
)
windowSpec = Window.partitionBy("cuisine_description").orderBy(F.desc("count"))
cuisine_violation_analysis = cuisine_violation_analysis.withColumn("rank", F.rank().over(windowSpec))

cuisine_violation_analysis.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_cuisine_violation_analysis') \
    .option("writeMethod", "overwrite") \
    .save()
