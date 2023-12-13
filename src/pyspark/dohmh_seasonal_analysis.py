from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, count
from pyspark.sql.types import StringType

table_id = 'bigdata-ripp.ripp_analytics.dohmh_season_trend_violations'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Seasonal Analysis') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

# Reading 'most_recent_letter_grade' from BQ
most_recent_letter_grade = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .load()


def map_month_to_season(month):
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    elif month in [9, 10, 11]:
        return "Autumn"
    else:
        return None


map_season = udf(map_month_to_season, StringType())

season_trend_violations = most_recent_letter_grade.withColumn("season", map_season(col("inspection_month"))) \
    .groupby("violation_code", "season") \
    .agg(count(lit(1)).alias("count"))

season_trend_violations.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_season_trend_violations') \
    .save()
