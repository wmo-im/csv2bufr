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
from jsonschema import validate
import logging
import os
from pathlib import Path
from typing import Union

THISDIR = os.path.dirname(os.path.realpath(__file__))
LOGGER = logging.getLogger(__name__)
SCHEMA = f"{THISDIR}{os.sep}resources{os.sep}schema"
TEMPLATE_DIRS = [Path("./")]

_SUCCESS_ = True

if Path("/opt/csv2bufr/templates").exists():
    TEMPLATE_DIRS.append(Path("/opt/csv2bufr/templates"))

# Set user defined location first
if 'CSV2BUFR_TEMPLATES' in os.environ:
    TEMPLATE_DIRS.append(Path(os.environ['CSV2BUFR_TEMPLATES']))
else:
    LOGGER.warning(f"""CSV2BUFR_TEMPLATES is not set, default search path(s)
        will be used ({TEMPLATE_DIRS}).""")

# Dictionary to store template filename and label (if assigned)
TEMPLATES = {}


# function to load template by name
def load_template(template_name: str) -> Union[dict, None]:
    """
    Checks whether specified template exists and loads file.
    Returns none and prints a warning if no template found.

    :param template_name: The name of the template (without file extension)
                          to load.

    :returns: BUFR template as a dictionary or none in the case of template
              not found.
    """
    template = None
    msg = False
    if template_name not in TEMPLATES:
        msg = f"Requested template '{template_name}' not found." +\
                       f" Search path = {TEMPLATE_DIRS}. Please update " +\
                       "search path (e.g. 'export CSV2BUFR_TEMPLATE=...')"
    else:
        fname = TEMPLATES[template_name].get('path')
        if fname is None:
            msg = f"Error loading template {template.name}, no path found"
        else:
            with open(fname) as fh:
                template = json.load(fh)

    if msg:
        raise RuntimeError(msg)
    else:
        return template


def validate_template(mapping: dict) -> bool:
    """
    Validates dictionary containing mapping to BUFR against internal schema.
    Returns True if the dictionary passes and raises an error otherwise.

    :param mapping: dictionary containing mappings to specified BUFR
                        sequence using ecCodes key.

    :returns: `bool` of validation result
    """
    global _warnings
    # load internal file schema for mappings
    file_schema = f"{SCHEMA}{os.sep}csv2bufr-template-v2.json"
    with open(file_schema) as fh:
        schema = json.load(fh)

    # now validate
    try:
        validate(mapping, schema)
    except Exception as e:
        msg = f"Exception ({e}). Invalid BUFR template mapping file: {mapping}"
        raise RuntimeError(msg)

    return _SUCCESS_


def index_templates() -> bool:
    for dir_ in TEMPLATE_DIRS:
        for template in dir_.iterdir():
            try:
                if template.suffix == ".json":
                    # check if valid mapping file
                    with template.open() as fh:
                        tmpl = json.load(fh)
                    if 'csv2bufr-template-v2.json' not in tmpl.get("conformsTo",[]):  # noqa
                        LOGGER.warning("'csv2bufr-template-v2.json' not found in " +  # noqa
                                       f"conformsTo for file {template}, skipping")  # noqa
                        continue
                    if validate_template(tmpl) == _SUCCESS_:
                        # get label if exists else set to empty string
                        fname = str(template)
                        id = tmpl['metadata'].get("id", "")
                        if id in TEMPLATES:
                            pass
                        else:
                            TEMPLATES[id] = {
                                "label": tmpl['metadata'].get("label", ""),
                                "description": tmpl['metadata'].get("description", ""),  # noqa
                                "version": tmpl['metadata'].get("version", ""),
                                "author": tmpl['metadata'].get("author", ""),
                                "dateCreated": tmpl['metadata'].get("dateCreated", ""),  # noqa
                                "id": tmpl['metadata'].get("id", ""),
                                "path": fname
                            }

            except Exception as e:
                print(dir_)
                LOGGER.warning(f"Warning raised indexing csv2bufr templates: {e}, skipping file {template}.")  # noqa

    return _SUCCESS_


def list_templates() -> dict:
    """
    :returns: Dictionary of known templates in search path
              (CSV2BUFR_TEMPLATES). An empty dictionary is return if no
              templates are found.
    """
    return TEMPLATES


if index_templates() != _SUCCESS_:
    LOGGER.error("Error indexing csv2bufr templates, see logs")
