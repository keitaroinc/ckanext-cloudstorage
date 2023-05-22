from google.cloud import storage


project_id = 'my-test-project'
bucket_name = "datapusher-plus"



storage_client = storage.Client.from_service_account_json(
        '/home/miloshira/Downloads/amplus-data-453d6f3dce42.json')

blobs = storage_client.list_blobs(bucket_name)
for blob in blobs:
    print(blob.name)