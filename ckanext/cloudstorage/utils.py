# -*- coding: utf-8 -*-
from __future__ import annotations
import mimetypes

import os.path
from pathlib import Path

from google.cloud import storage

import ckan.lib.helpers as h
import ckan.plugins.toolkit as tk
from ckan import model
from ckan.lib import base, uploader
# from ckantoolkit import config
from ckan.common import config


def fix_cors(domains):
    cs = CloudStorage()

    if cs.can_use_advanced_azure:
        from azure.storage import CorsRule
        from azure.storage import blob as azure_blob

        blob_service = azure_blob.BlockBlobService(
            cs.driver_options["key"], cs.driver_options["secret"]
        )

        blob_service.set_blob_service_properties(
            cors=[CorsRule(allowed_origins=domains, allowed_methods=["GET"])]
        )
        return "Done!", True
    else:
        return (
            "The driver {driver_name} being used does not currently"
            " support updating CORS rules through"
            " cloudstorage.".format(driver_name=cs.driver_name),
            False,
        )


def migrate():
    # Setup for GCP
    storage_path = config.get('ckan.storage_path',
                            '/var/lib/ckan/default/resources')
    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)
    bucket = storage_client.bucket(bucket_name)

    # the resources file structure on the bucket on this extension is like
    # resources/{resource_id}/{filename from databse}
    # it is not coping the directory structure on CKAN storage

    resource_id_url = model.Session.execute("select id, url from resource where state = 'active' and url_type = 'upload'")
    resource_ids_and_paths = dict((x, y) for x, y in resource_id_url)
 
    resource_id = {}
    migrated_index = 0
    not_migrated_index = 0
    storage_resource = storage_path + "/resources"
 
    for dirname, dirnames, filenames in os.walk(storage_resource):
        
        for filename in filenames:
            resource_id[filename] = dirname[-7:-4] + dirname[-3:] + filename
            if resource_id[filename] in resource_ids_and_paths:
                # path of the file on localstorage
                local_path = os.path.join(dirname, filename)

                # file name of the resource from database
                resource_name = resource_ids_and_paths[resource_id[filename]]

                # resource id from database
                r_id = resource_id[filename]

                blob = bucket.blob('resources/{resource_id}/{file_name}'.
                                   format(resource_id=r_id,
                                          file_name=resource_name))
                try:
                    blob.upload_from_filename(local_path)
                    migrated_index += 1
                    print(resource_id[filename], 'is migrated to S3 bucket')

                except Exception as e:
                    print(e)
                    print(resource_id[filename], 'is not migrated ')
                    not_migrated_index += 1
                    
            else:
                print(resource_id[filename], 'missing id in database - will not be migrated')
                not_migrated_index += 1

    print('Number of total resources migrated is {}'.format(migrated_index))
    print('Number of total resources not migrated is {}'.format(not_migrated_index))
    print('Number of total resources in storage is {}'.format(len(resource_id)))


def assets_to_gcp():
    # Setup for GCP
    storage_path = config.get('ckan.storage_path',
                            '/var/lib/ckan/default/resources')
    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)
    bucket = storage_client.bucket(bucket_name)

    group_ids_and_paths = {}
    for root, dirs, files in os.walk(storage_path):
        if root[-5:] == 'group':
            for idx, group_file in enumerate(files):
                group_ids_and_paths[group_file] = os.path.join(
                    root, files[idx])

    print('{0} group assets found in the database'.format(
        len(group_ids_and_paths.keys())))
    for resource_id, file_name in group_ids_and_paths.items():
        blob = bucket.blob('storage/uploads/group/{resource_id}'.
                           format(resource_id=resource_id))
        blob.upload_from_filename(file_name)
        print('{file_name} was uploaded'.format(file_name=file_name))
        
    
def check_resources():
    # Setup for GCP

    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)

    resource_id_url = model.Session.execute("select id, url from resource where state = 'active' and url_type = 'upload'")
    resultDictionary = dict((x, y) for x, y in resource_id_url)
    print('Number of total active resources in database is {}'.format(
                                                len(resultDictionary)))
    blobs = storage_client.list_blobs(bucket_name)
    count = 0
    for blob in blobs:
        gcp_resource_id = blob.name[10:46]
        if gcp_resource_id in resultDictionary:
            count += 1
            print(blob.name, "has id in database")
        else:
            count += 1
            print(blob.name, "has no id in database and will be deleted")
            # blob.delete()
            print(blob.name, "is deleted")

    print('Number of active resources checked {}'.format(count))

    if len(resultDictionary) == count:
        print('Resource check on GCP bucket OK')
    else:
        print('There are errors in migration')


def resource_exists_check():
    # Setup for GCP
    storage_path = config.get('ckan.storage_path',
                            '/var/lib/ckan/default/resources')
    path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
    bucket_name = config.get('ckanext.cloudstorage.container_name')
    storage_client = storage.Client.from_service_account_json(path_to_json)
    bucket = storage_client.bucket(bucket_name)
    
    resource_id_url = model.Session.execute("select id, url from resource where state = 'active' and url_type = 'upload'")
    resultDictionary = dict((x, y) for x, y in resource_id_url)
    print('Number of total active resources in database is {}'.format(
                                                len(resultDictionary)))
    for resource_id in resultDictionary:

        path_to_resource = storage_path + "/resources" + "/" + resource_id[0:3] + "/" + resource_id[3:6] + "/" + resource_id[6:]
        my_file = Path(path_to_resource)   
        if my_file.is_file():
            continue
            # print("resource exists on local storage")
        else:
            print(path_to_resource)
            print("resource is missing")


def resource_download(id, resource_id, filename=None):
    context = {
        "model": model,
        "session": model.Session,
        "user": tk.c.user or tk.c.author,
        "auth_user_obj": tk.c.userobj,
    }

    try:
        resource = tk.get_action("resource_show")(context, {"id": resource_id})
    except tk.ObjectNotFound:
        return base.abort(404, tk._("Resource not found"))
    except tk.NotAuthorized:
        return base.abort(
            401, tk._("Unauthorized to read resource {0}".format(id))
        )

    # This isn't a file upload, so either redirect to the source
    # (if available) or error out.
    if resource.get("url_type") != "upload":
        url = resource.get("url")
        if not url:
            return base.abort(404, tk._("No download is available"))
        return h.redirect_to(url)

    if filename is None:
        # No filename was provided so we'll try to get one from the url.
        filename = os.path.basename(resource["url"])

    upload = uploader.get_resource_uploader(resource)

    # if the client requests with a Content-Type header (e.g. Text preview)
    # we have to add the header to the signature
    content_type = getattr(tk.request, "content_type", None)
    if not content_type:
        content_type, _enc = mimetypes.guess_type(filename)

    uploaded_url = upload.get_url_from_filename(
        resource["id"], filename, content_type=content_type
    )

    # The uploaded file is missing for some reason, such as the
    # provider being down.
    if uploaded_url is None:
        return base.abort(404, tk._("No download is available"))

    return h.redirect_to(uploaded_url)
