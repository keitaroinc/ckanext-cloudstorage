# -*- coding: utf-8 -*-

from flask import Blueprint
import ckantoolkit as tk
import os
import ckanext.cloudstorage.utils as utils
from google.cloud import storage
import datetime

redirect = tk.redirect_to
config = tk.config

cloudstorage = Blueprint("cloudstorage", __name__)
s3_uploads = Blueprint("s3_uploads",__name__)


@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download")
@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download/<filename>")
def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)


def get_storage_path(upload_to):
    path = config.get('ckanext.cloudstorage.storage_path', '')
    return os.path.join(path, 'storage', 'uploads', upload_to)


def generate_download_signed_url_v4(blob_name):
    """Generates a v4 signed URL for downloading a blob.

    Note that this method requires a service account key file. You can not use
    this if you are using Application Default Credentials from Google Compute
    Engine or from the Google Cloud SDK.
    """
    # bucket_name = 'your-bucket-name'
    # blob_name = 'your-object-name'
    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)
    bucket = storage_client.bucket(bucket_name)

    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v4",
        # This URL is valid for 15 minutes
        expiration=datetime.timedelta(minutes=15),
        # Allow GET requests using this URL.
        method="GET",
    )

    print("Generated GET signed URL:")
    print(url)
    print("You can use this URL with any user agent, for example:")
    print(f"curl '{url}'")
    return url


def uploaded_file_redirect(upload_to, filename):
    '''Redirect static file requests to their location on S3.'''
    storage_path = get_storage_path(upload_to)
    filepath = os.path.join(storage_path, filename)
    bucket = config.get('ckanext.cloudstorage.container_name')

    url = generate_download_signed_url_v4(filepath)

    # url = 'https://previews.123rf.com/images/kchung/kchung1610/kchung161001354/64508202-test-written-by-hand-hand-writing-on-transparent-board-photo.jpg' 
    return redirect(url)


s3_uploads.add_url_rule(u'/uploads/<upload_to>/<filename>',
                        view_func=uploaded_file_redirect)


def get_blueprints():
    return [cloudstorage, s3_uploads]
