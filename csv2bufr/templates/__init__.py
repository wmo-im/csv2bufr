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
import os
from pathlib import Path
from typing import Union

TEMPLATE_DIRS = []

LOGGER = logging.getLogger(__name__)

# Set user defined location first
if 'CSV2BUFR_TEMPLATES' in os.environ:
    TEMPLATE_DIRS.append(Path(os.environ['CSV2BUFR_TEMPLATES']))

# Now add defaults
TEMPLATE_DIRS.append(Path(__file__).resolve().parent / 'resources')


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
    # iterate over directories and load file
    for dir_ in TEMPLATE_DIRS:
        try:
            template_file = dir_ / f"{template_name}.json"
            if template_file.is_file():
                with template_file.open() as fh:
                    template = json.load(fh)
                    break
        except Exception as e:
            LOGGER.warning(f"Error raised loading csv2bufr templates: {e}.")

    if template is None:
        LOGGER.warning(f"Requested template '{template_name}' not found." +
                       f" Search path = {TEMPLATE_DIRS}. Please update " +
                       "search path (e.g. 'export CSV2BUFR_TEMPLATE=...')"
                       )

    return template


def list_templates() -> list:
    """
    :returns: List of known templates in search path (CSV2BUFR_TEMPLATES).
              An empty list is return if no templates are found.
    """
    templates = []
    for dir_ in TEMPLATE_DIRS:
        try:
            for template in dir_.iterdir():
                if template.suffix == ".json":
                    templates.append(template.stem)
        except Exception as e:
            LOGGER.warning(f"Error raised listing csv2bufr templates: {e}." +
                           "Directory skipped.")

    return templates
