from google.cloud import storage

def upload_to_bucket(blob_name, path_to_file, bucket_name):

    storage_client = storage.Client.from_service_account_json(
        './creds/creds.json')  

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    return list(storage_client.list_buckets())

test = upload_to_bucket(blob_name='test/upload-test3',path_to_file='./files/upload-test.txt',bucket_name='bucket-quickstart_my-project-datapipeline-375319')

print(test)