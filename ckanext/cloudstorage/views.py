# -*- coding: utf-8 -*-

from flask import Blueprint
import ckantoolkit as tk

import ckanext.cloudstorage.utils as utils
from ckanext.cloudstorage.storage import ItemCloudStorage

redirect = tk.redirect_to

cloudstorage = Blueprint("cloudstorage", __name__)
s3_uploads = Blueprint("s3_uploads",__name__)


@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download")
@cloudstorage.route("/dataset/<id>/resource/<resource_id>/download/<filename>")
def download(id, resource_id, filename=None, package_type="dataset"):
    return utils.resource_download(id, resource_id, filename)


def uploaded_file_redirect(upload_to, filename):
    '''Redirect static file requests to their location on S3.'''

    # storage_path = S3Uploader.get_storage_path(upload_to)
    # filepath = os.path.join(storage_path, filename)
    # base_uploader = BaseS3Uploader()

    # try:
    #     url = base_uploader.get_signed_url_to_key(filepath)
    # except ClientError as ex:
    #     if ex.response['Error']['Code'] in ['NoSuchKey', '404']:
    #         return abort(404, _('Keys not found on S3'))
    #     else:
    #         raise ex
    url = 'https://previews.123rf.com/images/kchung/kchung1610/kchung161001354/64508202-test-written-by-hand-hand-writing-on-transparent-board-photo.jpg' 
    return redirect(url)


s3_uploads.add_url_rule(u'/uploads/<upload_to>/<filename>',
                        view_func=uploaded_file_redirect)


def get_blueprints():
    return [cloudstorage, s3_uploads]
