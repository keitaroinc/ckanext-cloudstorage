# -*- coding: utf-8 -*-

import click

import ckanext.cloudstorage.utils as utils


@click.group()
def cloudstorage():
    """CloudStorage management commands."""
    pass


@cloudstorage.command("fix-cors")
@click.argument("domains", nargs=-1)
def fix_cors(domains):
    """Update CORS rules where possible."""
    msg, ok = utils.fix_cors(domains)
    click.secho(msg, fg="green" if ok else "red")


@cloudstorage.command()
def migrate():
    """Upload local storage to the remote."""
    utils.migrate()


@cloudstorage.command("assets-to-gcp")
def assets_to_gcp():
    """Upload assets from local storage to GCP."""
    utils.assets_to_gcp()


@cloudstorage.command("check-resources")
def check_resources():
    """Check resources in storage and in GCP bucket"""
    utils.check_resources()


def get_commands():
    return [cloudstorage]
