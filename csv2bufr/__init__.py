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

__version__ = "0.1.0"

import csv
import json
from datetime import timezone, datetime
import hashlib
from io import StringIO, BytesIO
import logging
import tempfile
import os.path
from typing import Union
from jsonpath_ng.ext import parser

from eccodes import (codes_bufr_new_from_file, codes_bufr_new_from_samples,
                     codes_set_array, codes_set, codes_get_native_type,
                     codes_write, codes_release, codes_get,
                     CODES_MISSING_LONG, CODES_MISSING_DOUBLE,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_get_name)
from jsonschema import validate

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")

NULLIFY_INVALID = True  # TODO: move to env. variable

LOGGER = logging.getLogger(__name__)

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS = f"{THISDIR}{os.sep}resources{os.sep}mappings"

_LATEST_=36
_ATTRIBUTES_ = ['code','units','scale','reference','width']
_HEADERS_ = ["edition","masterTableNumber","bufrHeaderCentre",
             "bufrHeaderSubCentre","updateSequenceNumber","dataCategory",
	         "internationalDataSubCategory","dataSubCategory",
             "masterTablesVersionNumber","localTablesVersionNumber",
             "typicalYear","typicalMonth","typicalDay","typicalHour",
             "typicalMinute","typicalSecond","typicalDate","typicalTime",
             "numberOfSubsets","observedData","compressedData",
             "unexpandedDescriptors","subsetNumber"]


def parse_wigos_id(wigos_id: str) -> dict:
    """
    Split a WSI into a mapping dictionary for use in subsequent processing

    :param wigos_id: WIGOS Station Identifier (WSI)

    :returns: `dict` of WIGOS series/issuer/issuer number/local id
    """

    tokens = wigos_id.split("-")
    assert len(tokens) == 4
    result = {
        "wid_series": int(tokens[0]),
        "wid_issuer": int(tokens[1]),
        "wid_issue_number": int(tokens[2]),
        "wid_local": tokens[3]
    }
    return result


def validate_mapping_dict(mapping_dict: dict) -> bool:
    """
    Validate mapping dictionary

    :param mapping_dict: TODO: describe

    :returns: `bool` of validation result
    """

    # load internal file schema for mappings
    file_schema = f"{MAPPINGS}{os.sep}mapping_schema.json"
    with open(file_schema) as fh:
        schema = json.load(fh)

    # now validate
    try:
        validate(mapping_dict, schema)
    except Exception as e:
        message = "invalid mapping dictionary"
        LOGGER.error(message)
        raise e

    return SUCCESS


def apply_scaling(value: Union[NUMBERS], scale: Union[NUMBERS],
                  offset: Union[NUMBERS]) -> Union[NUMBERS]:
    """
    Apply simple scaling and offsets

    :param value: TODO describe
    :param scale: TODO describe
    :param offset: TODO describe

    :returns: scaled value
    """

    if isinstance(value, NUMBERS):
        if None not in [scale, offset]:
            try:
                value = value * pow(10, scale) + offset
            except Exception as e:
                LOGGER.error(e.message)
                raise e
    return value


def validate_value(key: str, value: Union[NUMBERS],
                   valid_min: Union[NUMBERS],
                   valid_max: Union[NUMBERS],
                   nullify_on_fail: bool = False) -> Union[NUMBERS]:
    """
    Check numeric values lie within specified range (if specified)

    :param key: TODO describe
    :param value: TODO describe
    :param valid_min: TODO describe
    :param valid_max: TODO describe
    :param nullify_on_fail: TODO describe

    :returns: validated value
    """

    if value is None:
        return value
    if not isinstance(value, NUMBERS):
        # TODO: add checking against code / flag table here?
        return value

    if None not in [valid_min, valid_max]:
        if value > valid_max or value < valid_min:
            e = ValueError(f"{key}: Value ({value}) out of valid range ({valid_min} - {valid_max}).")  # noqa
            if nullify_on_fail:
                message = f"{e} Element set to missing"
                LOGGER.warning(message)
                return None
            else:
                LOGGER.error(str(e))
                raise e

    return value


class bufr_message:
    def __init__(self, descriptors: list, delayed_replications: list = list(),
                 table_version:int =_LATEST_):
        # first create empty bufr message
        bufr_msg = codes_bufr_new_from_samples("BUFR4")
        # set delayed replication factors
        if len(delayed_replications) > 0:
            codes_set_array(bufr_msg, "inputDelayedDescriptorReplicationFactor",
                            delayed_replications)
        # set master table version number
        codes_set(bufr_msg, "masterTablesVersionNumber", table_version)
        # now set unexpanded descriptors
        codes_set_array(bufr_msg, "unexpandedDescriptors", descriptors)
        self.dict = dict()
        # now iterator over and add to dictionary
        iterator = codes_bufr_keys_iterator_new(bufr_msg)
        while codes_bufr_keys_iterator_next(iterator):
            key = codes_bufr_keys_iterator_get_name(iterator)
            self.dict[key] = dict()
            self.dict[key]["value"] = None
            native_type = codes_get_native_type(bufr_msg, key)
            self.dict[key]["type"] = native_type.__name__
            if key not in _HEADERS_:
                for attr in _ATTRIBUTES_:
                    try:
                        self.dict[key][attr] = \
                            codes_get(bufr_msg,f"{key}->{attr}")
                    except Exception as e:
                        raise(e)

        codes_release(bufr_msg)
        self.delayed_replications = delayed_replications
        self.bufr = None


    def set_element(self, key, value):
        if value is not None and not isinstance(value, list):
            expected_type = self.dict[key]["type"]
            if expected_type == "int" and not isinstance(value, int):
                LOGGER.warning(
                    f"int expected for {key} but received {type(value)} ({value})")  # noqa
                if isinstance(value, float):
                    value = int(round(value))
                else:
                    value = int(value)
                LOGGER.warning(f"value converted to int ({value})")
            elif expected_type == "float" and not isinstance(value, float):
                LOGGER.warning(
                    f"float expected for {key} but received {type(value)} ({value})")  # noqa
                value = float(value)
                LOGGER.warning(f"value converted to float ({value})")
            else:
                value = value
        self.dict[key]["value"] = value


    def get_element(self, key:str):
        # check if we want value or an attribute (indicated by ->)
        if "->" in key:
            tokens = key.split("->")
            result = self.dict[tokens[0]][tokens[1]]
        else:
            result = self.dict[key]["value"]
        return result


    def as_bufr(self, force=False) -> bytes:
        if (self.bufr is not None) and (force == False):
            return self.bufr
        # ===========================
        # initialise new BUFR message
        # ===========================
        bufr_msg = codes_bufr_new_from_samples("BUFR4")
        # set delayed replications, this is needed again as we only used it the
        # first time to set the keys
        if len(self.delayed_replications) > 0:
            codes_set_array(bufr_msg, "inputDelayedDescriptorReplicationFactor",
                            self.delayed_replications)
        # ============================
        # iterate over keys and encode
        # ============================
        for eccodes_key in self.dict:
            value = self.dict[eccodes_key]["value"]
            if value is not None:
                LOGGER.debug(
                    f"setting value {value} for element {eccodes_key}.")
                if isinstance(value, list):
                    try:
                        LOGGER.debug("calling codes_set_array")
                        codes_set_array(bufr_msg, eccodes_key, value)
                    except Exception as e:
                        LOGGER.error(
                            f"error calling codes_set_array({bufr_msg}, {eccodes_key}, {value}): {e}")  # noqa
                        raise e
                else:
                    try:
                        codes_set(bufr_msg, eccodes_key, value)
                    except Exception as e:
                        LOGGER.error(
                            f"error calling codes_set({bufr_msg}, {eccodes_key}, {value}): {e}")  # noqa
                        raise e
        # ==============================
        # Message now ready to be packed
        # ==============================
        try:
            codes_set(bufr_msg, "pack", True)
        except Exception as e:
            LOGGER.error(f"error calling codes_set({bufr_msg}, 'pack', True): {e}")
            raise e
        # =======================================================
        # now write to in memory file and return bytes to caller
        # =======================================================
        try:
            fh = BytesIO()
            codes_write(bufr_msg, fh)
            codes_release(bufr_msg)
            fh.seek(0)
        except Exception as e:
            LOGGER.error(f"error writing to internal BytesIO object, {e}")
            raise e
        # =============================================
        # Return BUFR message bytes
        # =============================================
        self.bufr = fh.read()
        return self.bufr


    def md5(self):
        return hashlib.md5(self.as_bufr()).hexdigest()

    def as_geojson(self, identifier: str, template: dict) -> str:
        result = self._extract(template)
        result["id"] = identifier
        result["properties"]["resultTime"] = datetime.now(timezone.utc).isoformat(timespec="seconds") #noqa
        return json.dumps(result, indent=2)

    def _extract(self, object_: Union[dict, list]) -> Union[dict, list]:
        if isinstance(object_, dict):
            # check if format or eccodes in object
            if "format" in object_:
                assert "args" in object_
                args = self._extract(object_["args"])
                if None not in args:
                    result = object_["format"].format(*args)
                else:
                    result = None
            elif "eccodes_key" in object_:
                result = self.get_element( object_["eccodes_key"] )
                if result in (CODES_MISSING_LONG, CODES_MISSING_DOUBLE):
                    result = None
            else:
                for k in object_:
                    object_[k] = self._extract(object_[k])
                result = object_
        elif isinstance(object_, list):
            for idx in range(len(object_)):
                object_[idx] = self._extract(object_[idx])
            result = object_
        else:
            result = object_
        return result

    def parse(self, data: str, metadata: dict, mappings: dict):
        # =====================
        # validate mapping dict
        # =====================
        e = validate_mapping_dict(mappings)
        if e is not SUCCESS:
            raise ValueError(f"Invalid mappings for {target_format}")
        # ==================================================
        # TODO validate metadata here
        # ==================================================
        # now extract wigos ID from metadata
        # ==================================================
        wigosID = metadata["wigosIds"][0]["wid"]
        tokens = parse_wigos_id(wigosID)
        for token in tokens:
            metadata["wigosIds"][0][token] = tokens[token]
        # ==================================================
        # now read data and parse data.
        # ==================================================
        # first convert data to StringIO object
        fh = StringIO(data)
        # now read csv data and iterate over rows
        reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        rows_read = 0
        for row in reader:
            assert rows_read < 2
            if rows_read == 0:
                col_names = row
            else:
                data = row
                # make dictionary from header row and data row
                data_dict = dict(zip(col_names, data))
                # Iterate over items to map, perform unit conversions and validate
                for section in ("header", "data"):
                    for element in mappings[section]:
                        eccodes_key = element["eccodes_key"]
                        # first identify source of data
                        value = None
                        column = None
                        jsonpath = None
                        if "value" in element:
                            value = element["value"]
                        if "csv_column" in element:
                            column = element["csv_column"]
                        if "jsonpath" in element:
                            jsonpath = element["jsonpath"]
                        # now get value from indicated source
                        if value is not None:
                            value = element["value"]
                        elif column is not None:
                            # get column name
                            # make sure column is in data_dict
                            if (column not in data_dict):
                                message = f"column '{column}' not found in data dictionary"  # noqa
                                raise ValueError(message)
                            value = data_dict[column]
                        elif jsonpath is not None:
                            query = parser.parse(jsonpath).find(metadata)
                            assert len(query) == 1
                            value = query[0].value
                        else:
                            LOGGER.debug(f"value and column both None for element {element['eccodes_key']}")  # noqa
                        # ===============================
                        # apply specified scaling to data
                        # ===============================
                        if value in MISSING:
                            value = None
                        else:
                            scale = None
                            offset = None
                            if "scale" in element:
                                scale = element["scale"]
                            if "offset" in element:
                                offset = element["offset"]
                            value = apply_scaling(value, scale, offset)
                        # ==================
                        # now validate value
                        # ==================
                        valid_min = None
                        valid_max = None
                        if "valid_min" in element:
                            valid_min = element["valid_min"]
                        if "valid_max" in element:
                            valid_max = element["valid_min"]
                        LOGGER.debug(f"validating value {value} for element {element['eccodes_key']}")  # noqa
                        value = validate_value(element["eccodes_key"], value,
                                               valid_min, valid_max,
                                               NULLIFY_INVALID)

                        LOGGER.debug(f"value {value} validated for element {element['eccodes_key']}")  # noqa
                        # ==================================================
                        # at this point we should have the eccodes key and a
                        # validated value to use, add to dict
                        # ==================================================
                        self.set_element(eccodes_key, value)
                        LOGGER.debug(f"value {value} updated for element {element['eccodes_key']}")  # noqa
            rows_read += 1


    def datetime(self):
        "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:00+00:00".format(
                                                             self.get_element("typicalYear"), #noqa
                                                             self.get_element("typicalMonth"), #noqa
                                                             self.get_element("typicalDay"), #noqa
                                                             self.get_element("typicalHour"), #noqa
                                                             self.get_element("typicalMinute")) #noqa


def transform(data: str, metadata: dict, mappings: dict, template: dict = None)\
        -> dict:
    message = bufr_message([301150, 307080], [0,0] )
    message.parse(data, metadata, mappings)
    result = dict()
    result[message.md5()] = dict()
    result[message.md5()]["bufr4"] = message.as_bufr()
    if template is not None:
        result[message.md5()]["geojson"] = message.as_geojson(message.md5(),
                                                          template)
    result[message.md5()]["_meta"] = dict()
    result[message.md5()]["_meta"]["data_date"] = message.datetime()
    result[message.md5()]["_meta"]["originating_centre"] =\
        message.get_element("bufrHeaderCentre")
    result[message.md5()]["_meta"]["data_category"] = \
        message.get_element("dataCategory")

    return result