# ckanext-cloudstorage

Implements support for cloud storage platforms and redirects assets calls to buckets for [CKAN].

Currently only tested and works on Google Cloud platform.

...
# Instalation

To install ckanext-cloudstorage:

Activate your CKAN virtual environment, for example:

. /usr/lib/ckan/default/bin/activate

Clone the source and install it on the virtualenv

``` git clone https://github.com/Keitaro/ckanext-cloudstorage.git 
    cd ckanext-cloudstorage 
    pip install -e . 
    pip install -r requirements.txt

```


# Setup

After installing `ckanext-cloudstorage`, add it to your list of plugins in
your `.ini`:

    ckan.plugins = stats cloudstorage


Google cloudstorage info:

``` ckanext.cloudstorage.driver = GOOGLE_STORAGE
    ckanext.cloudstorage.container_name = "bucket-name"
    ckanext.cloudstorage.driver_options = "key, secret json"
    ckanext.cloudstorage.google_service_account_json = "authorization json"
```

You can find a list of driver names [here][storage] (see the `Provider
Constant` column.)

Each driver takes its own setup options. See the [libcloud][] documentation.
These options are passed in using `driver_options`, which is a Python dict.
For most drivers, this is all you need:

    ckanext.cloudstorage.driver_options = {"key": "<your public key>", "secret": "<your secret key>"}

# Migrating From FileStorage

If you already have resources that have been uploaded and saved using CKAN's
built-in FileStorage, cloudstorage provides an easy migration command.
Simply setup cloudstorage as explained above, enable the plugin, and run the
migrate command. Provide the path to your resources on-disk (the
`ckan.storage_path` setting in your CKAN `.ini` + `/resources`), and
cloudstorage will take care of the rest. Ex:

    ckan -c <path-to-ckan.ini> cloudstorage assets-to-gcp

# FAQ

- *DataViews aren't showing my data!* - did you setup CORS rules properly on
  your hosting service? ckanext-cloudstorage can try to fix them for you automatically,
  run:

        ckan -c <path-to-ckan.ini> cloudstorage fix-cors


[libcloud]: https://libcloud.apache.org/
[ckan]: http://ckan.org/
[storage]: https://libcloud.readthedocs.io/en/latest/storage/supported_providers.html
[ckanstorage]: http://docs.ckan.org/en/latest/maintaining/filestore.html#setup-file-uploads
