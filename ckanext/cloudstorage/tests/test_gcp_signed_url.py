# -*- coding: utf-8 -*-
"""Unit tests for the GOOGLE_STORAGE download path.

Nothing here talks to the network, a bucket or the database: the libcloud
driver is a mock and the service-account credentials are a stub.
"""
import logging
import threading
import time
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest

from ckanext.cloudstorage import storage as cs_storage
from ckanext.cloudstorage.storage import ResourceCloudStorage

BUCKET = "test-bucket"
KEY_PATH = "/nonexistent/service-account.json"
CLIENT_EMAIL = "ckan@test-project.iam.gserviceaccount.com"
SEVEN_DAYS = 604800


class FakeSigner(object):
    def sign(self, message):
        return b"\x01\x02\x03\x04"


class FakeCredentials(object):
    service_account_email = CLIENT_EMAIL
    signer = FakeSigner()


@pytest.fixture
def gcp_config(monkeypatch):
    for key, value in {
        "ckanext.cloudstorage.driver": "GOOGLE_STORAGE",
        "ckanext.cloudstorage.container_name": BUCKET,
        "ckanext.cloudstorage.driver_options": "{}",
        "ckanext.cloudstorage.google_service_account_json": KEY_PATH,
    }.items():
        monkeypatch.setitem(cs_storage.config, key, value)


@pytest.fixture
def driver(monkeypatch):
    """Replace the libcloud driver class by a mock, return the instance."""
    driver_cls = mock.MagicMock(name="driver_cls")
    monkeypatch.setattr(cs_storage, "get_driver", lambda provider: driver_cls)
    return driver_cls.return_value


@pytest.fixture
def load_credentials(monkeypatch):
    """Empty the per-process cache and stub the service-account loader."""
    monkeypatch.setattr(cs_storage, "_gcp_credentials_cache", {})
    loader = mock.MagicMock(return_value=FakeCredentials())
    monkeypatch.setattr(
        cs_storage.service_account.Credentials,
        "from_service_account_file",
        loader,
    )
    return loader


@pytest.fixture
def uploader(gcp_config, driver, load_credentials):
    return ResourceCloudStorage({})


def test_gcp_url_makes_no_container_or_object_call(uploader, driver):
    url = uploader.get_url_from_filename("res-id", "data.csv")

    assert url
    driver.get_container.assert_not_called()
    driver.get_object.assert_not_called()
    driver.get_object_cdn_url.assert_not_called()
    assert uploader._container is None
    # the driver is built, but nothing at all is called on it
    assert driver.mock_calls == []


def test_gcp_url_shape(uploader):
    url = uploader.get_url_from_filename("res-id", "data.csv")

    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "{}.storage.googleapis.com".format(BUCKET)
    assert parsed.path == "/resources/res-id/data.csv"

    query = parse_qs(parsed.query)
    assert query["X-Goog-Algorithm"] == ["GOOG4-RSA-SHA256"]
    assert query["X-Goog-Expires"] == [str(SEVEN_DAYS)]
    assert query["X-Goog-SignedHeaders"] == ["host"]
    assert query["x-goog-signature"] == ["01020304"]
    assert len(query["X-Goog-Date"][0]) == len("20260101T000000Z")
    datestamp = query["X-Goog-Date"][0][:8]
    assert query["X-Goog-Credential"] == [
        "{}/{}/auto/storage/goog4_request".format(CLIENT_EMAIL, datestamp)
    ]


def test_gcp_signed_url_is_not_printed_or_logged(uploader, capsys, caplog):
    # the ckanext loggers do not always propagate to the root logger, so the
    # capturing handler is attached to the module logger itself
    logger = logging.getLogger(cs_storage.__name__)
    caplog.set_level(logging.DEBUG, logger=logger.name)
    logger.addHandler(caplog.handler)
    try:
        url = uploader.get_url_from_filename("res-id", "data.csv")
    finally:
        logger.removeHandler(caplog.handler)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert url not in caplog.text
    assert "x-goog-signature" not in caplog.text


def test_credentials_loaded_once_across_calls(
    gcp_config, driver, load_credentials
):
    for _ in range(5):
        # a new uploader per download, as resource_download does
        assert ResourceCloudStorage({}).get_url_from_filename(
            "res-id", "data.csv"
        )

    load_credentials.assert_called_once_with(KEY_PATH)


def test_credentials_loaded_once_across_threads(
    gcp_config, driver, load_credentials
):
    def slow_load(path):
        # keep every thread inside the cache-miss window: without the lock
        # each of them would load the key file
        time.sleep(0.05)
        return FakeCredentials()

    load_credentials.side_effect = slow_load

    workers = 12
    barrier = threading.Barrier(workers)
    urls = []
    errors = []

    def download():
        try:
            barrier.wait(timeout=10)
            urls.append(
                ResourceCloudStorage({}).get_url_from_filename(
                    "res-id", "data.csv"
                )
            )
        except Exception as e:  # pragma: no cover - reported below
            errors.append(e)

    threads = [threading.Thread(target=download) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert errors == []
    assert len(urls) == workers
    assert all(urls)
    load_credentials.assert_called_once_with(KEY_PATH)


def test_expiration_over_seven_days_raises_value_error(
    uploader, load_credentials
):
    with pytest.raises(ValueError):
        uploader.generate_signed_url_gcp(
            BUCKET, "resources/res-id/data.csv", expiration=SEVEN_DAYS + 1
        )

    load_credentials.assert_not_called()


def test_non_gcp_driver_still_checks_the_object(
    gcp_config, driver, load_credentials, monkeypatch
):
    monkeypatch.setitem(
        cs_storage.config, "ckanext.cloudstorage.driver", "LOCAL"
    )
    container = driver.get_container.return_value
    driver.get_object_cdn_url.return_value = "http://cdn.example/file"

    url = ResourceCloudStorage({}).get_url_from_filename("res-id", "data.csv")

    assert url == "http://cdn.example/file"
    driver.get_container.assert_called_once_with(container_name=BUCKET)
    container.get_object.assert_called_once_with("resources/res-id/data.csv")
    driver.get_object_cdn_url.assert_called_once_with(
        container.get_object.return_value
    )
    load_credentials.assert_not_called()


def test_non_gcp_driver_missing_object_returns_none(
    gcp_config, driver, load_credentials, monkeypatch
):
    monkeypatch.setitem(
        cs_storage.config, "ckanext.cloudstorage.driver", "LOCAL"
    )
    driver.get_container.return_value.get_object.side_effect = (
        cs_storage.ObjectDoesNotExistError("missing", driver, "data.csv")
    )

    url = ResourceCloudStorage({}).get_url_from_filename("res-id", "data.csv")

    assert url is None
