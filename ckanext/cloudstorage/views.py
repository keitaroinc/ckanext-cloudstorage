# -*- coding: utf-8 -*-

from flask import Blueprint

import ckanext.cloudstorage.utils as utils
from ckanext.cloudstorage.storage import ItemCloudStorage

cloudstorage = Blueprint("cloudstorage", __name__)
s3_uploads = Blueprint("s3_uploads",__name__)


@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download")
@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download/<filename>")
def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)


def get_blueprints():
    return [cloudstorage]
