# -*- coding: utf-8 -*-

from flask import Blueprint
from ckanext.cloudstorage.helpers import generate_download_signed_url_v4
import ckantoolkit as tk
import os
import ckanext.cloudstorage.utils as utils

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


def uploaded_file_redirect(upload_to, filename):
    '''Redirect static file requests to their location on S3.'''
    
    storage_path = get_storage_path(upload_to)
    filepath = os.path.join(storage_path, filename)
    url = generate_download_signed_url_v4(filepath)

    return redirect(url, code=302)


s3_uploads.add_url_rule(u'/uploads/<upload_to>/<filename>',
                        view_func=uploaded_file_redirect)


def get_blueprints():
    return [cloudstorage, s3_uploads]
