#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path

from pylons import c
from pylons.i18n import _

from ckan import logic, model
from ckan.lib import base, uploader
import ckan.lib.helpers as h


class StorageController(base.BaseController):
    def resource_download(self, id, resource_id, filename=None):
        context = {
            'model': model,
            'session': model.Session,
            'user': c.user or c.author,
            'auth_user_obj': c.userobj
        }

        try:
            resource = logic.get_action('resource_show')(
                context,
                {
                    'id': resource_id
                }
            )
        except logic.NotFound:
            base.abort(404, _('Resource not found'))
        except logic.NotAuthorized:
            base.abort(401, _('Unauthorized to read resource {0}'.format(id)))

        # This isn't a file upload, so either redirect to the source
        # (if available) or error out.
        url = resource.get('url')
        if resource.get('url_type') != 'upload' and not url.startswith(config.get('ckan.site_url')):
            if not url:
                base.abort(404, _('No download is available'))
            h.redirect_to(url)
        if filename is None:
            # No filename was provided so we'll try to get one from the url.
            filename = os.path.basename(resource['url'])

        upload = uploader.get_resource_uploader(resource)

        # ignore resource name. Allows to download files, created by s3filestore
        prefix = upload.path_from_filename(resource['id'], 'name')
        prefix = prefix[:prefix.rindex('/')]
        similar_files = list(
            upload.driver.iterate_container_objects(upload.container, prefix)
        )
        if similar_files:
            _first = similar_files[0].name
            filename = _first[_first.rindex('/') + 1:]
        uploaded_url = upload.get_url_from_filename(resource['id'], filename)

        # The uploaded file is missing for some reason, such as the
        # provider being down.
        if uploaded_url is None:
            base.abort(404, _('No download is available'))

        h.redirect_to(uploaded_url)
