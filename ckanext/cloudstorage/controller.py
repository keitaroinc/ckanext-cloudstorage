#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ckan.lib import base
from google.cloud import storage
import ckantoolkit as toolkit

import logging
log = logging.getLogger(__name__)

redirect = toolkit.redirect_to

import ckanext.cloudstorage.utils as utils


class StorageController(base.BaseController):
    def resource_download(self, id, resource_id, filename=None):
        return utils.resource_download(id, resource_id, filename)

    def uploaded_file_redirect(self, upload_to, filename):
        """ Redirects static file requets to their location on google cloud """

        path = toolkit.config.get('ckanext.s3filestore.aws_storage_path', '')



        log.info("----------------------------------")
        log.info(path)
        log.info("----------------------------------")
        file_name = "2023-08-24-134503.211626depeche-mode.png"

        bucket_name = "datapusher-plus"
        
        client = storage.Client.from_service_account_json('/home/miloshira/Downloads/amplus-data-453d6f3dce42.json')
        log.info("we are fine here!")
        #bucket = client.get_bucket(bucket_name)
        
        blobs = client.list_blobs(bucket_name)
        
        blob_name = f"storage/upload/group/{file_name}"
        
        
        filename = "2023-08-24-134503.211626depeche-mode.png"
        path = f"storage/uploads/group/{filename}"

        #blob_name = f"storage/upload/group/{file_name}"
        
        #blob = client.bucket(bucket_name).blob(blob_name)



        for blob in blobs:
            if filename in blob.name:
                asset_signed_url = blob.generate_signed_url(
                version='v4',
                expiration=604800,
                method='GET')
                break
                #blob_for_url = blob

                #print(asset_signed_url)


        #blob = bucket.blob(blob_name)
        
        #asset_url = blob.public_url 

        #without_list = "http://localhost:5000/uploads/group/2023-05-23-103941.884380sample-resume-for-mechanical-engineer.jpg"
        #with_list = "http://localhost:5000/uploads/group/2023-05-23-103941.884380sample-resume-for-mechanical-engineer.jpg"

        # asset_signed_url = blob.generate_signed_url(
        #     version='v4',
        #     expiration=604800,
        #     method='GET'
        # )

        return redirect(f"{asset_signed_url}")