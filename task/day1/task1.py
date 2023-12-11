from google.cloud import storage
import os
import json


def write_to_gcs(bucket_name, blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data_string = '\n'.join(json.dumps(item) for item in data)
    blob.upload_from_string(data_string)


bucket_name = os.getenv('BUCKET_NAME')
blob_name = 'task.json'
data = [
    {"first_name": "Kharis","last_name": "Amiruddin" ,"age": 20, "province": "Central Java"},
    {"first_name": "Euis","last_name": "Geulis", "age": 22, "province": "West Java"},
    {"first_name": "Jaka","last_name": "Sembung" , "age": 27, "province": "East Java"},
    {"first_name": "Doel","last_name": "Jumidoel", "age": 25, "province": "Jakarta"}
]
write_to_gcs(bucket_name, blob_name, data)
