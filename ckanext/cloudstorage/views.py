# -*- coding: utf-8 -*-

from flask import Blueprint
import ckanext.cloudstorage.utils as utils
import ckantoolkit as toolkit
import ckan.plugins.toolkit as tk
from  ckan.lib import uploader
from google.cloud import storage

redirect = toolkit.redirect_to

import logging
log = logging.getLogger(__name__)

cloudstorage = Blueprint("cloudstorage", __name__)
s3_uploads = Blueprint(u's3_uploads', __name__)

bucket_name = toolkit.config.get("ckanext.cloudstorage.container_name")


@cloudstorage.route("/uploads/<upload_to>/<filename>", methods = ['POST', 'GET'])
def uploaded_file_redirect(upload_to, filename):
    '''Redirect static file requests to their location on goodle storage container.'''

    context = {'ignore_auth': True}

    user = tk.get_action('get_site_user')(context, {})
    user_name = user['name']

    
    groups = tk.get_action('group_list')(context, {'username': user_name})
    
    for group in groups:
        if 'http' not in filename: # excludes linkes, since they are already online.

            bucket_name = toolkit.config.get('ckanext.cloudstorage.container_name')
            client = storage.Client.from_service_account_json(toolkit.config.get('ckanext.cloudstorage.google_service_account_json'))
            
            blobs = client.list_blobs(bucket_name) #TODO(milosh) list_blobs is a slow iteration find a gcp blop.function() that will instantly point to the blob we need!
            for blob in blobs:    
                if filename in blob.name:
                    asset_signed_url = blob.generate_signed_url(
                    version='v4',
                    expiration=604800,
                    method='GET')

        try:
            return redirect(asset_signed_url)
        except UnboundLocalError:
            msg = log.error("----------One or more assets not uploaded to Cloud Platform, please upload it. ------------")
            return str(msg)


    organizations = tk.get_action('organization_list_for_user')(context, {'id':user_name})
    for organization in organizations:
        if 'http' not in filename:  # excludes link because we only need actual files from storage.

            client = storage.Client.from_service_account_json(toolkit.config.get('ckanext.cloudstorage.google_service_account_json'))
            
            blobs = client.list_blobs(bucket_name) #TODO(milosh) list_blobs is a slow iteration find a gcp blop.function() that will instantly point to the blob we need!
            for blob in blobs:    
                if filename in blob.name:
                    asset_signed_url = blob.generate_signed_url(
                    version='v4',
                    expiration=604800,
                    method='GET')
                    organizations.remove(organization)
                    log.info("----- this is the organizaiton list!")
                    log.info(organizations)
        try:
            return redirect(asset_signed_url)
        except UnboundLocalError:
            msg = log.error("----------One or more assets not uploaded to Cloud Platform, please upload it ------------")
            return str(msg)


@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download")
@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download/<filename>")
def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)

# def uploaded_file_redirect(upload_to, filename):
#     '''Redirect static file requests to their location on S3.'''

#     # storage_path = S3Uploader.get_storage_path(upload_to)
#     # filepath = os.path.join(storage_path, filename)
#     # base_uploader = BaseS3Uploader()

#     # try:
#     #     url = base_uploader.get_signed_url_to_key(filepath)
#     # except ClientError as ex:
#     #     if ex.response['Error']['Code'] in ['NoSuchKey', '404']:
#     #         return abort(404, _('Keys not found on S3'))
#     #     else:
#     #         raise ex
#     url = 'www.google.com'
#     return redirect(url)



s3_uploads.add_url_rule(u'/uploads/<upload_to>/<filename>',
                        view_func=uploaded_file_redirect)


def get_blueprints():
    return [cloudstorage, s3_uploads]
