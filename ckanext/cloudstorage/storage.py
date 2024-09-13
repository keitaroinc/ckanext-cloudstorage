#!/usr/bin/env python
# -*- coding: utf-8 -*-
import binascii
import cgi
import hashlib
import logging
import mimetypes
import os
from tempfile import NamedTemporaryFile
import tempfile
import traceback
from ast import literal_eval
from datetime import datetime, timedelta
from urllib.parse import urljoin

import ckan.plugins as p
import libcloud.common.types as types
from ckan import model
from ckan.lib import munge
from ckan.lib.uploader import Upload
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ObjectDoesNotExistError, Provider
from werkzeug.datastructures import FileStorage as FlaskFileStorage
from google.cloud import storage

import collections
import sys
import ckan.logic as logic

logging.basicConfig(
    level=logging.DEBUG,  # You can change this to logging.INFO for less verbosity
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# google-auth
from google.oauth2 import service_account
import six
from six.moves.urllib.parse import quote

config = p.toolkit.config

log = logging.getLogger(__name__)

ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)
AWS_UPLOAD_PART_SIZE = 5 * 1024 * 1024


CONFIG_SECURE_TTL = "ckanext.cloudstorage.secure_ttl"
DEFAULT_SECURE_TTL = 3600

# DEFINE STORAGE CHUNKSIZE
chunk_size = int(config.get('ckanext.cloudstorage.chunk_size', '2'))
storage.blob._DEFAULT_CHUNKSIZE = chunk_size * 1024 * 1024  # 2 MB default
storage.blob._MAX_MULTIPART_SIZE = chunk_size * 1024 * 1024  # 2 MB default
print("==============default chunksize============================")
print(storage.blob._DEFAULT_CHUNKSIZE)
print("==============default chunksize============================")

def config_secure_ttl():
    return p.toolkit.asint(p.toolkit.config.get(
        CONFIG_SECURE_TTL, DEFAULT_SECURE_TTL
    ))


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


def _md5sum(fobj):
    block_count = 0
    block = True
    md5string = b""
    while block:
        block = fobj.read(AWS_UPLOAD_PART_SIZE)
        if block:
            block_count += 1
            hash_obj = hashlib.md5()
            hash_obj.update(block)
            md5string = md5string + binascii.unhexlify(hash_obj.hexdigest())
        else:
            break
    fobj.seek(0, os.SEEK_SET)
    hash_obj = hashlib.md5()
    hash_obj.update(md5string)
    return hash_obj.hexdigest() + "-" + str(block_count)


class CloudStorage(object):
    def __init__(self):
        self.driver = get_driver(getattr(Provider, self.driver_name))(
            **self.driver_options
        )
        self._container = None

    def path_from_filename(self, rid, filename):
        raise NotImplementedError

    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        if self._container is None:
            self._container = self.driver.get_container(
                container_name=self.container_name
            )

        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return literal_eval(config["ckanext.cloudstorage.driver_options"])

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config["ckanext.cloudstorage.driver"]

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        return config["ckanext.cloudstorage.container_name"]

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.use_secure_urls", False)
        )

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.leave_files", False)
        )

    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if self.driver_name == "AZURE_BLOBS":
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage

                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if "S3" in self.driver_name:
            if "host" not in self.driver_options:
                # newer libcloud versions(must-use for python3)
                # requires host for secure URLs
                return False
            try:
                # Yes? Is the boto package available?
                import boto

                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                pass

        return False
    
    @property
    def can_use_advanced_gcp(self):
        """
        `True` if the `google` module is installed and ckanext-cloudstorage has
        been configured to use GCP, otherwise `False`.
        """
        # Are we even using AWS?
        if "GOOGLE_STORAGE" in self.driver_name:
            if "host" not in self.driver_options:
                # newer libcloud versions(must-use for python3)
                # requires host for secure URLs
                return False
            try:
                # Yes? Is the google package available?
                import google.cloud

                # Shut the linter up.
                assert google.cloud
                return True
            except ImportError:
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.guess_mimetype", False)
        )


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop("upload", None)
        self._clear = resource.pop("clear_upload", None)
        multipart_name = resource.pop("multipart_name", None)
        # Check to see if a file has been provided
        if (
            isinstance(upload_field_storage, (ALLOWED_UPLOAD_TYPES))
            and upload_field_storage.filename
        ):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = _get_underlying_file(upload_field_storage)
            resource["url"] = self.filename
            temp_resource = _get_underlying_file(upload_field_storage)
            temp_resource_len = len(temp_resource.read())
            resource["size"] = temp_resource_len
            temp_resource.seek(0)
            resource["url_type"] = "upload"
            resource['mimetype'] = upload_field_storage.mimetype
            # if file format should be added automaticly uncomment the line
            # resource["format"] = os.path.splitext(upload_field_storage.filename)[1][1:]
            resource["last_modified"] = datetime.utcnow()
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource["url"] = munge.munge_filename(multipart_name)
            resource["url_type"] = "upload"
            resource["last_modified"] = datetime.utcnow()
        elif self._clear and resource.get("id"):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(model.Resource).get(
                resource["id"]
            )

            self.old_filename = old_resource.url
            resource["url_type"] = ""

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join("resources", rid, munge.munge_filename(filename))

    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        if self.filename:
            if self.can_use_advanced_azure:
                from azure.storage import blob as azure_blob
                from azure.storage.blob.models import ContentSettings

                blob_service = azure_blob.BlockBlobService(
                    self.driver_options["key"], self.driver_options["secret"]
                )
                content_settings = None
                if self.guess_mimetype:
                    content_type, _ = mimetypes.guess_type(self.filename)
                    if content_type:
                        content_settings = ContentSettings(
                            content_type=content_type
                        )
                return blob_service.create_blob_from_stream(
                    container_name=self.container_name,
                    blob_name=self.path_from_filename(id, self.filename),
                    stream=self.file_upload,
                    content_settings=content_settings,
                )
            else:
                try:
                    file_upload = self.file_upload

                    # check if already uploaded
                    object_name = self.path_from_filename(id, self.filename)
                    try:
                        cloud_object = self.container.get_object(
                            object_name=object_name
                        )
                        log.debug(
                            "\t Object found, checking size %s: %s",
                            object_name,
                            cloud_object.size,
                        )
                        if os.path.isfile(self.filename):
                            file_size = os.path.getsize(self.filename)
                        else:
                            self.file_upload.seek(0, os.SEEK_END)
                            file_size = self.file_upload.tell()
                            self.file_upload.seek(0, os.SEEK_SET)
                        log.debug(
                            "\t - File size %s: %s", self.filename, file_size
                        )
                        if file_size == int(cloud_object.size):
                            log.debug(
                                "\t Size fits, checking hash %s: %s",
                                object_name,
                                cloud_object.hash,
                            )
                            hash_file = hashlib.md5(
                                self.file_upload.read()
                            ).hexdigest()
                            self.file_upload.seek(0, os.SEEK_SET)
                            log.debug(
                                "\t - File hash %s: %s",
                                self.filename,
                                hash_file,
                            )
                            # basic hash
                            if hash_file == cloud_object.hash:
                                log.debug(
                                    "\t => File found, matching hash, skipping"
                                    " upload"
                                )
                                return
                            # multipart hash
                            multi_hash_file = _md5sum(self.file_upload)
                            log.debug(
                                "\t - File multi hash %s: %s",
                                self.filename,
                                multi_hash_file,
                            )
                            if multi_hash_file == cloud_object.hash:
                                log.debug(
                                    "\t => File found, matching hash, skipping"
                                    " upload"
                                )
                                return
                        log.debug(
                            "\t Resource found in the cloud but outdated,"
                            " uploading"
                        )
                    except ObjectDoesNotExistError:
                        log.debug(
                            "\t Resource not found in the cloud, uploading"
                        )

                    # If it's temporary file, we'd better convert it
                    # into FileIO. Otherwise libcloud will iterate
                    # over lines, not over chunks and it will really
                    # slow down the process for files that consist of
                    # millions of short linew
                    # if isinstance(file_upload, tempfile.SpooledTemporaryFile):
                    #     file_upload.rollover()
                    #     try:
                    #         # extract underlying file
                    #         file_upload_iter = file_upload._file.detach()
                    #     except AttributeError:
                    #         # It's python2
                    #         file_upload_iter = file_upload._file
                    # else:
                    #     file_upload_iter = iter(file_upload)
                    # self.container.upload_object_via_stream(
                    #     iterator=file_upload_iter, object_name=object_name
                    # )
                    # log.debug(
                    #     "\t => UPLOADED %s: %s", self.filename, object_name
                    # )
                    
                    path_to_json = config.get(
                        'ckanext.cloudstorage.google_service_account_json')
                    bucket_name = config.get('ckanext.cloudstorage.container_name')
                    storage_client = storage.Client.from_service_account_json(path_to_json)
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(object_name)


                    # Log the upload attempt
                    logging.info(f"Uploading to {object_name} in bucket {bucket_name}.")
                    print("======================================================")
                    print((f"Uploading to {object_name} in bucket {bucket_name}."))
                    print("======================================================")
                    try:
                        print("=================starting upload=================")
                        blob.upload_from_file(file_upload, timeout=300)
                        print("=================finished upload====================")
                        logging.info(f"File uploaded to {blob.name}.")
                        print(f"File uploaded to {blob.name}.")
                        print("=================finished upload====================")
                    except Exception as e:
                        print("=================upload goes in exeption block==============")
                        logging.error(f"Failed to upload file {blob.name} to {bucket_name}: {e}")
                        print("===========error uploading===============")
                        print(f"Failed to upload file {blob.name} to {bucket_name}: {e}")
                        print("===========error uploading===============")
                        raise


                    log.debug(
                         "\t => UPLOADED %s: %s", self.filename, object_name
                     )
                except (ValueError, types.InvalidCredsError) as err:
                    log.error(traceback.format_exc())
                    raise err

        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            try:
                self.container.delete_object(
                    self.container.get_object(
                        self.path_from_filename(id, self.old_filename)
                    )
                )
            except ObjectDoesNotExistError:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def get_url_from_filename(self, rid, filename, content_type=None):
        path = self.path_from_filename(rid, filename)

        return self.get_url_by_path(path, content_type)


    def generate_signed_url_gcp(self, bucket_name, object_name,
                        subresource=None, expiration=604800, http_method='GET',
                        query_parameters=None, headers=None):
        
        import datetime # importing here just to make sure it doesn't break anything with the previous imports 

        if expiration > 604800: # 7 days (in seconds)
            print('Expiration Time can\'t be longer than 604800 seconds (7 days).')
            sys.exit(1)

        escaped_object_name = quote(six.ensure_binary(object_name), safe=b'/~')
        canonical_uri = '/{}'.format(escaped_object_name)

        datetime_now = datetime.datetime.now(tz=datetime.timezone.utc)
        request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
        datestamp = datetime_now.strftime('%Y%m%d')


        # Load file with the service account information from the config file (ckan.ini)
        service_account_path = config['ckanext.cloudstorage.google_service_account_json']

        google_credentials = service_account.Credentials.from_service_account_file(
            service_account_path) #providing the path to the json file with the service account information
        client_email = google_credentials.service_account_email
        credential_scope = '{}/auto/storage/goog4_request'.format(datestamp)
        credential = '{}/{}'.format(client_email, credential_scope)

        if headers is None:
            headers = dict()
        host = '{}.storage.googleapis.com'.format(bucket_name)
        headers['host'] = host

        canonical_headers = ''
        ordered_headers = collections.OrderedDict(sorted(headers.items()))
        for k, v in ordered_headers.items():
            lower_k = str(k).lower()
            strip_v = str(v).lower()
            canonical_headers += '{}:{}\n'.format(lower_k, strip_v)

        signed_headers = ''
        for k, _ in ordered_headers.items():
            lower_k = str(k).lower()
            signed_headers += '{};'.format(lower_k)
        signed_headers = signed_headers[:-1]  # remove trailing ';'

        if query_parameters is None:
            query_parameters = dict()
        query_parameters['X-Goog-Algorithm'] = 'GOOG4-RSA-SHA256'
        query_parameters['X-Goog-Credential'] = credential
        query_parameters['X-Goog-Date'] = request_timestamp
        query_parameters['X-Goog-Expires'] = expiration
        query_parameters['X-Goog-SignedHeaders'] = signed_headers
        if subresource:
            query_parameters[subresource] = ''

        canonical_query_string = ''
        ordered_query_parameters = collections.OrderedDict(
            sorted(query_parameters.items()))
        for k, v in ordered_query_parameters.items():
            encoded_k = quote(str(k), safe='')
            encoded_v = quote(str(v), safe='')
            canonical_query_string += '{}={}&'.format(encoded_k, encoded_v)
        canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

        canonical_request = '\n'.join([http_method,
                                    canonical_uri,
                                    canonical_query_string,
                                    canonical_headers,
                                    signed_headers,
                                    'UNSIGNED-PAYLOAD'])

        canonical_request_hash = hashlib.sha256(
            canonical_request.encode()).hexdigest()

        string_to_sign = '\n'.join(['GOOG4-RSA-SHA256',
                                    request_timestamp,
                                    credential_scope,
                                    canonical_request_hash])

        # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
        signature = binascii.hexlify(
            google_credentials.signer.sign(string_to_sign)
        ).decode()

        scheme_and_host = '{}://{}'.format('https', host)
        signed_url = '{}{}?{}&x-goog-signature={}'.format(
            scheme_and_host, canonical_uri, canonical_query_string, signature)

        return signed_url


    def get_url_by_path(self, path, content_type=None):
        """
        Retrieve a publically accessible URL for the given path

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param path: The resource name on cloud.
        :param content_type: Optionally a Content-Type header.

        :returns: Externally accessible URL or None.
        """
        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
        if self.can_use_advanced_azure and self.use_secure_urls:
            from azure.storage import blob as azure_blob

            blob_service = azure_blob.BlockBlobService(
                self.driver_options["key"], self.driver_options["secret"]
            )

            return blob_service.make_blob_url(
                container_name=self.container_name,
                blob_name=path,
                sas_token=blob_service.generate_blob_shared_access_signature(
                    container_name=self.container_name,
                    blob_name=path,
                    expiry=datetime.utcnow() + timedelta(seconds=config_secure_ttl()),
                    permission=azure_blob.BlobPermissions.READ,
                ),
            )
        elif self.can_use_advanced_aws and self.use_secure_urls:
            from boto.s3.connection import S3Connection

            os.environ["S3_USE_SIGV4"] = "True"
            s3_connection = S3Connection(
                self.driver_options["key"],
                self.driver_options["secret"],
                host=self.driver_options["host"],
            )

            if "region_name" in self.driver_options.keys():
                s3_connection.auth_region_name = self.driver_options[
                    "region_name"
                ]

            generate_url_params = {
                "expires_in": config_secure_ttl(),
                "method": "GET",
                "bucket": self.container_name,
                "key": path,
            }

            if content_type:
                generate_url_params["response_headers"] = {"Content-Type": content_type}
            return s3_connection.generate_url_sigv4(**generate_url_params)


        # Find the object for the given key.
        try:
            obj = self.container.get_object(path)
        except ObjectDoesNotExistError:
            return
        if obj is None:
            return

        # Not supported by all providers!
        try:
            # Since we are using GCP, we need to generate a signed URL for downloading the files we want,
            # because the initial google_storage driver (from the libcloud library) does not support the get_object_cdn_url method
            if "GOOGLE_STORAGE" in self.driver_name:
                print("Using GOOGLE_STORAGE DRIVER")

                bucket = self.container_name

                url = self.generate_signed_url_gcp(bucket, path) #bucket is the GCP bucket, path is the file name (blob)
                print("Generated GET signed URL:")
                print(url)
                return url
            
            else:
                return self.driver.get_object_cdn_url(obj)
            
        except NotImplementedError:
            if "S3" in self.driver_name:
                return urljoin(
                    "https://" + self.driver.connection.host,
                    "{container}/{path}".format(
                        container=self.container_name,
                        path=path,
                    ),
                )                    
            # This extra 'url' property isn't documented anywhere, sadly.
            # See azure_blobs.py:_xml_to_object for more.
            elif "url" in obj.extra:
                return obj.extra["url"]
            
            raise

    @property
    def package(self):
        return model.Package.get(self.resource["package_id"])


class ItemCloudStorage(Upload):
    
    def upload(self, id, max_size=10):
        storage_path = config.get('ckan.storage_path',
                            '/var/lib/ckan/default/resources')
        path_to_json = config.get(
        'ckanext.cloudstorage.google_service_account_json')
        bucket_name = config.get('ckanext.cloudstorage.container_name')
        storage_client = storage.Client.from_service_account_json(path_to_json)
        bucket = storage_client.bucket(bucket_name)
        if hasattr(self, 'upload_file'):
            self.verify_type()
            self.upload_file.seek(0, os.SEEK_SET)
            key = self.filepath
            if storage_path in key:
                key = key.replace(storage_path, '', 1)
                key = key[1:]  # remove the first "/"
            if self.filename:
                upload_file = self.upload_file
                blob = bucket.blob(key)
                file_as_bytes = upload_file.read()
                try:
                    blob.upload_from_string(file_as_bytes)
                except:
                    pass
        else:
            pass

