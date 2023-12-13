from google.cloud import bigquery
from pyspark.sql import SparkSession

table_id = 'bigdata-ripp.ripp_analytics.dohmh_violations_per_year'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Violations per year') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

# Reading 'most_recent_letter_grade' from BQ
most_recent_letter_grade = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .load()

violations_per_year = most_recent_letter_grade.groupBy("violation_code", "inspection_year").count()

violations_per_year.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_violations_per_year') \
    .option("writeMethod", "overwrite") \
    .save()
