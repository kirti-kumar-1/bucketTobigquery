import json,os
from google.cloud import storage
storage_client = storage.Client()




def upload_file_to_bucket(bucket_name, contents, destination_blob_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(contents)