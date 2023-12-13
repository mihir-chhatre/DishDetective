#!/usr/bin/env python


from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, year, month

table_id = 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade'
client = bigquery.Client()
client.delete_table(table_id, not_found_ok=True)

TEMP_BUCKET = "ripp-temp-bucket"

spark = SparkSession.builder \
    .appName('Most Recent Grade') \
    .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest.jar') \
    .config("temporaryGcsBucket", "ripp-temp-bucket") \
    .getOrCreate()

df = spark.read.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_data') \
    .load()

df = df.withColumn("inspection_date", to_date(col("inspection_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

df_filtered = df.withColumn("inspection_year", year(col("inspection_date"))) \
    .withColumn("inspection_month", month(col("inspection_date")))

df_filtered.createOrReplaceTempView("NYC_Insp_Results")

most_recent_letter_grade_df = spark.sql(
    """
            with RecentInspDate as (
                select camis
                        , max(`inspection_date`) as MostRecentInspDate
                        from NYC_Insp_Results
                where (`inspection_type` in (                                            
                            'Cycle Inspection / Re-inspection'
                        , 'Pre-permit (Operational) / Re-inspection')
                        OR (`inspection_type` in (                                                    
                            'Cycle Inspection / Initial Inspection'                                  
                        , 'Pre-permit (Operational) / Initial Inspection') 
                        AND score <= 13) or (`inspection_type` in (                                                    
                            'Pre-permit (Operational) / Reopening Inspection'
                                    ,'Cycle Inspection / Reopening Inspection') 
            )) and grade in ('A', 'B', 'C', 'P', 'Z')  --values where a grade card or grade pending card is issued
                group by camis)

            --Select all restaurant inspection data based on the most recent inspection date
            select i.*
                from NYC_Insp_Results i
                join RecentInspDate r
                        on r.camis = i.camis
                        and r.MostRecentInspDate = i.`inspection_date`
                where `inspection_type` in (                                            
            'Cycle Inspection / Re-inspection'
            , 'Pre-permit (Operational) / Re-inspection' -- re-inspections
                        , 'Pre-permit (Operational) / Reopening Inspection'
            ,'Cycle Inspection / Reopening Inspection') --re-opening inspections where grade pending is issued
                        OR (`inspection_type` in (                                                    
                            'Cycle Inspection / Initial Inspection'                                  
                        , 'Pre-permit (Operational) / Initial Inspection') 
                        AND score <= 13) --initial inspections where A grade is issued
    """
)

most_recent_letter_grade_df.write.format("bigquery") \
    .option('table', 'bigdata-ripp.ripp_analytics.dohmh_most_recent_letter_grade') \
    .save()
