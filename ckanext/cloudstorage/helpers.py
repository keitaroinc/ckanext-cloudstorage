#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ckanext.cloudstorage.storage import ResourceCloudStorage
import ckan.plugins.toolkit as tk
from google.cloud import storage
import datetime

config = tk.config


def use_secure_urls():
    return all(
        [
            ResourceCloudStorage.use_secure_urls.fget(None),
            # Currently implemented just AWS version
            "S3" in ResourceCloudStorage.driver_name.fget(None),
            "host" in ResourceCloudStorage.driver_options.fget(None),
        ]
    )


def use_multipart_upload():
    return use_secure_urls()


def max_upload_size():
    return tk.config.get("ckanext.cloudstorage.max_upload_size_gb")


def generate_download_signed_url_v2(blob_name):
    """Generates a v4 signed URL for downloading a blob.

    Note that this method requires a service account key file. You can not use
    this if you are using Application Default Credentials from Google Compute
    Engine or from the Google Cloud SDK.
    """

    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)
    bucket = storage_client.bucket(bucket_name)

    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(
        version="v2",
        # This URL is valid for 365 days
        expiration=datetime.timedelta(days=365),
        # Allow GET requests using this URL.
        method="GET",
    )

    return url
