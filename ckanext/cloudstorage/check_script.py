from google.cloud import storage


project_id = 'my-test-project'
bucket_name = "datapusher-plus"

""" 
Runiing this script prints out the content of your google storage, created a dataset with an uploaded resource
for example a .csv file, then run this script in the terminal, if your installation is successful your .csv file will be listed.
"""

storage_client = storage.Client.from_service_account_json(
        '/home/miloshira/Downloads/amplus-data-453d6f3dce42.json')

blobs = storage_client.list_blobs(bucket_name)
for blob in blobs:
    print(blob.name)