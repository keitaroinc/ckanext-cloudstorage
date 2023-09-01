from routes.mapper import SubMapper
import ckan.plugins as plugins
import ckantoolkit as toolkit

import ckanext.cloudstorage.uploader

import logging
log = logging.getLogger(__name__)


class CloudStoragePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable)
    plugins.impements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)



    # IConfigurable

    def configure(self, config):

        missing_config = "{0} is not configured. Please ammend your .ini file"

        config_options = (
            'ckanext.cloudstorage.driver',
            'ckanext.cloudstorage.container_name',
            'ckanext.cloudstorage.driver_options',
            'ckanext.cloudstorage.google_service_account_json'
        )

        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))
            
    
    # IUploader

    def get_uploader():
        pass

    # IRoutes

    def before_map():
        with SubMapper(map, controller='ckanext.cloudstorage.controller:StorageController') as m:
        
        #NOTE (milosh) Add download file intecepts too.
            log.info("----------- In map ---------------")    
            #Intercept the uploaded file links (e.g.group images)
            m.connect('uploaded_file', '/uploads/{upload_to}/{filename}', 
                      action = 'uploaded_file_redirect')
            
        return map
    

    
