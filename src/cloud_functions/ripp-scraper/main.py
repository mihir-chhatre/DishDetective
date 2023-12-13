import json
import logging
import os
import time
from datetime import datetime

import functions_framework
import requests
from google.cloud import bigquery
from google.cloud import storage

LIMIT = 50000
BUCKET_NAME = "ripp-data-bucket"
BLOB_NAME = "active/{year}/{month}/{day}/data_{offset}.csv"

storage_client = storage.Client()
bucket = storage_client.get_bucket(BUCKET_NAME)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ripp-scraper")


def get_record_count():
    url = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
    headers = {
        "X_App_Token": os.environ.get("X_App_Token")
    }
    params = {
        "$select": "count(*)"
    }

    res = requests.get(url, headers=headers, params=params)

    if res.status_code == 200:
        count = json.loads(res.text)[0]["count"]
        logger.info(f"Fetched Record Count: {count}")
        return count


def get_data(scrape_date: str):
    logger.info("Starting downloads")
    scrape_date = datetime.strptime(scrape_date, '%Y-%m-%d')

    blob_params = {
        "year": scrape_date.year,
        "month": scrape_date.month,
        "day": scrape_date.day
    }

    url = "https://data.cityofnewyork.us/resource/43nn-pn8j.csv"
    headers = {
        "X-App-Token": os.environ.get("X-App-Token")
    }

    record_count = int(get_record_count())

    params = {
        "$order": "camis",
        "$limit": LIMIT,
        "$offset": 0
    }

    for offset in range(0, record_count, LIMIT):
        params["$offset"] = offset
        blob_params["offset"] = offset

        res = requests.get(url, headers=headers, params=params)
        if res.status_code == 200:
            logger.info(f"Downloaded frame - offset: {offset}")
            data = res.text
            blob_name = BLOB_NAME.format_map(blob_params)
            blob = bucket.blob(blob_name)
            blob.upload_from_string(data)
            time.sleep(60)

    return True


def archive_data():
    logger.info("Archiving Data")
    source_bucket = storage_client.get_bucket(BUCKET_NAME)
    blob_names = [blob.name for blob in list(source_bucket.list_blobs(prefix="active"))]
    for blob_name in blob_names:
        logger.debug(f"Moving {blob_name}")
        source_blob = source_bucket.blob(blob_name)
        new_blob = source_bucket.copy_blob(source_blob, source_bucket, blob_name.replace("active", "backup"))
        source_blob.delete()


def import_data_to_bigquery():
    client = bigquery.Client()

    table_id = "bigdata-ripp.ripp_analytics.dohmh_data"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1
    )

    uri = "gs://ripp-data-bucket/active/*"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

    load_job.result()
    destination_table = client.get_table(table_id)
    logger.info("Loaded {} rows.".format(destination_table.num_rows))


@functions_framework.http
def main(request=None):
    scrape_date = str(datetime.today().date())
    if request is not None:
        scrape_date = request.args.get("scrape_date")
    logger.info(f"Scraping Function Triggered - {scrape_date}")
    archive_data()
    get_data(scrape_date)
    import_data_to_bigquery()
    return '{"status":"200", "data": "OK"}'


if __name__ == "__main__":
    main()
