

class Upload(object):
    def __init__(self, object_type, old_filename=None):
        ''' Setup upload by creating a subdirectory of the storage directory
        of name object_type. old_filename is the name of the file in the url
        field last time'''

        self.storage_path = None
        self.filename = None
        self.filepath = None
        path = get_storage_path()
        if not path:
            return
        self.storage_path = os.path.join(path, 'storage',
                                         'uploads', object_type)
        # check if the storage directory is already created by
        # the user or third-party
        if os.path.isdir(self.storage_path):
            pass
        else:
            try:
                os.makedirs(self.storage_path)
            except OSError as e:
                # errno 17 is file already exists
                if e.errno != 17:
                    raise
        self.object_type = object_type
        self.old_filename = old_filename
        if old_filename:
            self.old_filepath = os.path.join(self.storage_path, old_filename)

    def update_data_dict(self, data_dict, url_field, file_field, clear_field):
        ''' Manipulate data from the data_dict.  url_field is the name of the
        field where the upload is going to be. file_field is name of the key
        where the FieldStorage is kept (i.e the field where the file data
        actually is). clear_field is the name of a boolean field which
        requests the upload to be deleted.  This needs to be called before
        it reaches any validators'''

        self.url = data_dict.get(url_field, '')
        self.clear = data_dict.pop(clear_field, None)
        self.file_field = file_field
        self.upload_field_storage = data_dict.pop(file_field, None)

        if not self.storage_path:
            return
        
        if isinstance(self.upload_field_storage, ALLOWED_UPLOAD_TYPES):
            if self.upload_field_storage.filename:
                self.filename = self.upload_field_storage.filename
                self.filename = str(datetime.datetime.utcnow()) + self.filename
                self.filename = munge.munge_filename_legacy(self.filename)
                self.filepath = os.path.join(self.storage_path, self.filename)
                data_dict[url_field] = self.filename
                self.upload_file = _get_underlying_file(
                    self.upload_field_storage)
                self.tmp_filepath = self.filepath + '~'
        # keep the file if there has been no change
        elif self.old_filename and not self.old_filename.startswith('http'):
            if not self.clear:
                data_dict[url_field] = self.old_filename
            if self.clear and self.url == self.old_filename:
                data_dict[url_field] = ''

    def upload(self, max_size=2):
        ''' Actually upload the file.
        This should happen just before a commit but after the data has
        been validated and flushed to the db. This is so we do not store
        anything unless the request is actually good.
        max_size is size in MB maximum of the file'''

        self.verify_type()
        if self.filename:
            with open(self.tmp_filepath, 'wb+') as output_file:
                try:
                    _copy_file(self.upload_file, output_file, max_size)
                except logic.ValidationError:
                    os.remove(self.tmp_filepath)
                    raise
                finally:
                    self.upload_file.close()
            os.rename(self.tmp_filepath, self.filepath)
            self.clear = True

        if (self.clear and self.old_filename
                and not self.old_filename.startswith('http')):
            try:
                os.remove(self.old_filepath)
            except OSError:
                pass

    def verify_type(self):
        if not self.filename:
            return

        mimetypes = aslist(
            config.get(
                "ckan.upload.{}.mimetypes".format(self.object_type),
                ["image/png", "image/gif", "image/jpeg"]
            )
        )

        types = aslist(
            config.get(
                "ckan.upload.{}.types".format(self.object_type),
                ["image"]
            )
        )

        if not mimetypes and not types:
            return

        actual = magic.from_buffer(self.upload_file.read(1024), mime=True)
        self.upload_file.seek(0, os.SEEK_SET)
        err = {self.file_field: [
            "Unsupported upload type: {actual}".format(actual=actual)]}

        if mimetypes and actual not in mimetypes:
            raise logic.ValidationError(err)

        type_ = actual.split("/")[0]
        if types and type_ not in types:
            raise logic.ValidationError(err)