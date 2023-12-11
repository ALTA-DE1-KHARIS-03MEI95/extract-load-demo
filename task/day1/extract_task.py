from google.cloud import bigquery
from google.cloud import storage
import os

def read_from_gcs_and_load_to_bigquery(bucket_name, blob_name, dataset_id, table_id):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_text()

    bigquery_client = bigquery.Client()

    schema = [
        bigquery.SchemaField('first_name', 'STRING'),
        bigquery.SchemaField('last_name', 'STRING'),
        bigquery.SchemaField('age', 'INTEGER'),
        bigquery.SchemaField('province', 'STRING')
    ]

    job_cofig = bigquery.LoadJobConfig()
    job_cofig.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_cofig.schema = schema

    uri = "gs://{}/{}".format(bucket_name, blob_name)
    load_job = bigquery_client.load_table_from_uri(uri, dataset_id + '.' + table_id, job_config=job_cofig)
    print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job Finished")

bucket_name = os.getenv('BUCKET_NAME')
blob_name = 'task.json'
dataset_id = 'task_dataset'
table_id = 'task_table'
read_from_gcs_and_load_to_bigquery(bucket_name, blob_name, dataset_id, table_id)