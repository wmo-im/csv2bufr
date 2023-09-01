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
import os
import pathlib

TEMPLATE_DIRS = []

# Set user defined location first
if 'CSV2BUFR_TEMPLATES' in os.environ:
    TEMPLATE_DIRS.append(os.environ['CSV2BUFR_TEMPLATES'])

# Now add defaults
TEMPLATE_DIRS.append(
    f"{os.path.dirname(os.path.realpath(__file__))}{os.sep}resources")


def load_template(template_name):
    template = None
    # iterate over directories and load file
    for dir in TEMPLATE_DIRS:
        template_file = f"{dir}{os.sep}{template_name}.json"
        if os.path.isfile(template_file):
            with open(template_file, 'r') as fh:
                template = json.load(fh)
                break
    return template


def list_templates():
    templates = []
    for dir in TEMPLATE_DIRS:
        _templates = os.listdir(dir)
        for template in _templates:
            path = pathlib.Path(template)
            if path.suffix == ".json":
                templates.append(path.stem)
    return templates
