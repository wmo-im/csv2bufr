###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import json
import logging
import os.path
import sys

import click

from csv2bufr import __version__, BUFRMessage, transform as transform_csv

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS = f"{THISDIR}{os.sep}resources{os.sep}mappings"


def cli_option_verbosity(f):
    logging_options = ["ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

    def callback(ctx, param, value):
        if value is not None:
            logging.basicConfig(stream=sys.stdout,
                                level=getattr(logging, value))
        return True

    return click.option("--verbosity", "-v",
                        type=click.Choice(logging_options),
                        help="Verbosity",
                        callback=callback)(f)


def cli_callbacks(f):
    f = cli_option_verbosity(f)
    return f


@click.group()
@click.version_option(version=__version__)
def cli():
    """csv2bufr"""
    pass


@click.group()
def data():
    """data workflows"""
    pass


@click.group()
def mappings():
    """stored mappings"""
    pass


@click.command('list')
@click.pass_context
def list_mappings(ctx):
    for mapping in os.listdir(MAPPINGS):
        msg = f"{mapping} => {MAPPINGS}{os.sep}{mapping}"
        click.echo(msg)


@click.command('create')
@click.pass_context
@click.argument("sequence", nargs=-1, type=int)
def create_mappings(ctx, sequence):
    msg = BUFRMessage(sequence)
    msg.create_template()


@click.command()
@click.pass_context
@click.argument("csv_file", type=click.File(errors="ignore"))
@click.option("--bufr-template", "mapping", required=True,
              help="Name of file or mapping template to use to map from CSV to BUFR")  # noqa
@click.option("--output-dir", "output_dir", required=True,
              help="Name of output file")
@click.option("--station-metadata", "station_metadata", required=True,
              help="WIGOS station identifier JSON file")
@cli_option_verbosity
def transform(ctx, csv_file, mapping, output_dir, station_metadata, verbosity):
    result = None
    click.echo(f"Transforming {csv_file.name} to BUFR")

    # identify mapping to use
    if not os.path.isfile(mapping):
        mappings_file = f"{MAPPINGS}{os.sep}{mapping}.json"
        if not os.path.isfile(mappings_file):
            raise click.ClickException(
                f"Invalid stored mapping ({mappings_file})")
    else:
        mappings_file = mapping

    # load mappings
    with open(mappings_file) as fh:
        mappings = json.load(fh)

    metadata = None
    with open(station_metadata) as fh:
        metadata = json.load(fh)

    try:
        result = transform_csv(csv_file.read(), metadata, mappings)
    except Exception as err:
        raise click.ClickException(err)

    click.echo("Writing data to file")
    for item in result:
        key = item['_meta']["identifier"]
        bufr_filename = f"{output_dir}{os.sep}{key}.bufr4"
        with open(bufr_filename, "wb") as fh:
            fh.write(item["bufr4"])

    click.echo("Done")


data.add_command(transform)
mappings.add_command(list_mappings)
mappings.add_command(create_mappings)

cli.add_command(data)
cli.add_command(mappings)
