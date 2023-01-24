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

__version__ = '0.4.1'

import csv
from datetime import timezone, datetime
import hashlib
from io import StringIO, BytesIO
import json
import logging
import os.path
from typing import Any, Iterator, Union

from eccodes import (codes_bufr_new_from_samples,
                     codes_set_array, codes_set, codes_get_native_type,
                     codes_write, codes_release, codes_get,
                     codes_bufr_keys_iterator_new,
                     codes_bufr_keys_iterator_next,
                     codes_bufr_keys_iterator_delete,
                     codes_bufr_keys_iterator_get_name, CodesInternalError)

from jsonschema import validate

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None", "")

if 'CSV2BUFR_NULLIFY_INVALID' in os.environ:
    NULLIFY_INVALID = os.environ['CSV2BUFR_NULLIFY_INVALID']
    if NULLIFY_INVALID == "True":
        NULLIFY_INVALID = True
    else:
        NULLIFY_INVALID = False
else:
    NULLIFY_INVALID = True

LOGGER = logging.getLogger(__name__)

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS = f"{THISDIR}{os.sep}resources{os.sep}mappings"

BUFR_TABLE_VERSION = 38  # default BUFR table version
# list of BUFR attributes
ATTRIBUTES = ['code', 'units', 'scale', 'reference', 'width']
# list of ecCodes keys for BUFR headers
HEADERS = ["edition", "masterTableNumber", "bufrHeaderCentre",
           "bufrHeaderSubCentre", "updateSequenceNumber", "dataCategory",
           "internationalDataSubCategory", "dataSubCategory",
           "masterTablesVersionNumber", "localTablesVersionNumber",
           "typicalYear", "typicalMonth", "typicalDay", "typicalHour",
           "typicalMinute", "typicalSecond", "typicalDate", "typicalTime",
           "numberOfSubsets", "observedData", "compressedData",
           "unexpandedDescriptors", "subsetNumber"]


# Errors
# index_, key not found
# column not found in input data
# invalid mapping, data type not recognised
# invalid mapping file
# error applying scaling to data
# validation error, value out of range
# error getting key for variable
# conversion error, non-int to int
# conversion error, non-float to float
# eccodes, codes_set_array
# eccodes, codes_set
# error creating internal BytesIO for BUFR
# error scaling data
# error validating data


# function to find position in array of requested element
def index_(key, mapping):
    idx = 0
    for item in mapping:
        if item['eccodes_key'] == key:
            return idx
        idx += 1
    LOGGER.error(f"key {key} not found in {mapping}")
    raise ValueError


def parse_value(element: str, data: dict):
    data_type = element.split(":")
    if data_type[0] == "const":
        value = data_type[1]
        if "." in value:
            value = float(value)
        else:
            value = int(value)
    elif data_type[0] == "data":
        column = data_type[1]
        if column not in data:
            LOGGER.error(f"Column {column} not found in input data: {data}")
            raise ValueError
        value = data[column]
    elif data_type[0] == "array":
        value = data_type[1]
        # determine if float or int
        if "." in value:
            func = float
        else:
            func = int
        # split into words, strip white space and convert
        words = value.split(",")
        value = list(map(lambda x: func(x.strip()), words))
    else:
        LOGGER.error(f"Data type ({data_type[0]}) not recognised in mapping: {element}")  # noqa
        raise ValueError
    return value


# function to retrieve data
def get_(key: str, mapping: dict, data: dict):
    # get position in mapping
    try:
        idx = index_(key, mapping)
        element = mapping[idx]
        value = parse_value(element['value'], data)
    except Exception as e:
        if NULLIFY_INVALID:
            LOGGER.warning(f"Error raised get value for {key}, None returned for {key}")  # noqa
            value = None
        else:
            raise e
    return value


# function to validate mapping file against JSON schema
def validate_mapping(mapping: dict) -> bool:
    """
    Validates dictionary containing mapping to BUFR against internal schema.
    Returns True if the dictionary passes and raises an error otherwise.

    :param mapping: dictionary containing mappings to specified BUFR
                        sequence using ecCodes key.

    :returns: `bool` of validation result
    """

    # load internal file schema for mappings
    file_schema = f"{MAPPINGS}{os.sep}mapping_schema.json"
    with open(file_schema) as fh:
        schema = json.load(fh)

    # now validate
    try:
        validate(mapping, schema)
    except Exception as e:
        message = f"Invalid mapping dictionary {mapping}"
        LOGGER.error(message)
        raise e

    return SUCCESS


def apply_scaling(value: Union[NUMBERS], scale: Union[NUMBERS],
                  offset: Union[NUMBERS]) -> Union[NUMBERS]:
    """
    Applies a simple scaling and offset to the input data value.
    Scaling follows the BUFR conventions, e.g.

    .. math::
        \\mbox{scaled\\_value} =
            \\mbox{value} \\times 10^{\\mbox{scale}} + \\mbox{offset}

    :param value: The input value to have scaling and offset applied to
    :param scale: The scale factor to use
    :param offset: The offset factor to use

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

    :param key: ECCODES key used when giving feedback / logging
    :param value: The value to validate
    :param valid_min: Valid minimum value
    :param valid_max: Valid maximum value
    :param nullify_on_fail: Action to take on fail, either set to None
                           (nullify_on_fail=True) or through an error (default)

    :returns: validated value
    """

    # TODO move this function to the class as part of set value

    if value is None:
        return value
    if not isinstance(value, NUMBERS):
        # TODO: add checking against code / flag table here?
        return value

    if None not in [valid_min, valid_max]:
        if value > valid_max or value < valid_min:
            e = ValueError(f"{key}: Value ({value}) out of valid range ({valid_min} - {valid_max}).")  # noqa
            if nullify_on_fail:
                LOGGER.warning(f"{e}; Element set to missing")
                return None
            else:
                LOGGER.error(str(e))
                raise e

    return value


class BUFRMessage:
    def __init__(self, descriptors: list,
                 short_delayed_replications: list = list(),
                 delayed_replications: list = list(),
                 extended_delayed_replications: list = list(),
                 table_version: int = BUFR_TABLE_VERSION) -> None:
        """
        Constructor

        :param descriptors: list of BUFR descriptors to use in this instance
                            e.g. descriptors=[301150, 307014]
        :param delayed_replications: delayed replicators to use in the sequence
                                     if not set ECCODES sets the delayed
                                     replicators to 1. Omit if unsure of value
        :param table_version: version of Master Table 0 to use, default 36

        """
        # ================================
        # first create empty bufr messages
        # ================================
        bufr_msg = codes_bufr_new_from_samples("BUFR4")
        # ===============================
        # set delayed replication factors
        # ===============================
        if len(short_delayed_replications) > 0:
            codes_set_array(bufr_msg,
                            "inputShortDelayedDescriptorReplicationFactor",
                            short_delayed_replications)

        if len(delayed_replications) > 0:
            codes_set_array(bufr_msg,
                            "inputDelayedDescriptorReplicationFactor",
                            delayed_replications)
        if len(extended_delayed_replications) > 0:
            codes_set_array(bufr_msg,
                            "inputExtendedDelayedDescriptorReplicationFactor",
                            extended_delayed_replications)

        # ===============================
        # set master table version number
        # ===============================
        codes_set(bufr_msg, "masterTablesVersionNumber", table_version)
        # now set unexpanded descriptors
        codes_set_array(bufr_msg, "unexpandedDescriptors", descriptors)
        # ================================================
        # now iterator over and add to internal dictionary
        # ================================================
        self.dict = dict()  # need a more descriptive name
        iterator = codes_bufr_keys_iterator_new(bufr_msg)
        while codes_bufr_keys_iterator_next(iterator):
            key = codes_bufr_keys_iterator_get_name(iterator)
            # place holder for data
            self.dict[key] = dict()
            self.dict[key]["value"] = None
            # add native type, used when encoding later
            native_type = codes_get_native_type(bufr_msg, key)
            self.dict[key]["type"] = native_type.__name__
            # now add attributes (excl. BUFR header elements)
            if key not in HEADERS:
                for attr in ATTRIBUTES:
                    try:
                        self.dict[key][attr] = \
                            codes_get(bufr_msg, f"{key}->{attr}")
                    except Exception as e:
                        raise e
        codes_bufr_keys_iterator_delete(iterator)
        # ============================================
        # now release the BUFR message back to eccodes
        # ============================================
        codes_release(bufr_msg)
        # ============================================
        # finally add last few items to class
        # ============================================
        self.descriptors = descriptors
        self.delayed_replications = delayed_replications  # used when encoding
        self.short_delayed_replications = \
            short_delayed_replications  # used when encoding
        self.extended_delayed_replications = \
            extended_delayed_replications  # used when encoding
        self.bufr = None  # placeholder for BUFR bytes
        # ============================================

    def create_template(self) -> None:
        template = {}
        template["inputDelayedDescriptorReplicationFactor"] = \
            self.delayed_replications

        template["inputShortDelayedDescriptorReplicationFactor"] = \
            self.short_delayed_replications
        template["inputExtendedDelayedDescriptorReplicationFactor"] = \
            self.extended_delayed_replications
        template["number_header_rows"] = 0
        template["column_names_row"] = 0

        template["header"] = []
        # create header section
        for element in HEADERS:
            value = ""
            if element == "unexpandedDescriptors":
                value = ",".join(str(x) for x in self.descriptors)
                value = f"array:{value}"
            entry = {
                "eccodes_key": element,
                "value": value
            }
            template["header"].append(entry)
        # now create data section
        template["data"] = []
        for element in self.dict:
            if element not in HEADERS:
                if self.dict[element]['type'] in ('int', 'float'):
                    # calulcate valid min and max
                    scale = self.dict[element]['scale']
                    offset = self.dict[element]['reference']
                    width = self.dict[element]['width']
                    valid_min = round((0 + offset) * pow(10, -1 * scale), scale)  # noqa
                    valid_max = round((pow(2, width) - 2 + offset) * pow(10, -1 * scale), scale)  # noqa
                    entry = {
                        "eccodes_key": element,
                        "value": "",
                        "valid_min": f"const:{valid_min}",
                        "valid_max": f"const:{valid_max}",
                        "scale": "const:0",
                        "offset": "const:0"
                    }
                else:
                    entry = {
                        "eccodes_key": element,
                        "value": "",
                    }
                template["data"].append(entry)

        print(json.dumps(template, indent=4))

    def reset(self) -> None:
        """
        Function resets BUFRMessage

        :returns: `None`
        """
        for key in self.dict:
            self.dict[key]["value"] = None
        self.bufr = None

    def set_element(self, key: str, value: object) -> None:
        """
        Function to set element in BUFR message

        :param key: the key of the element to set (using ECCODES keys)
        :param value: the value of the element

        :returns: `None`
        """

        # TODO move value validation here

        if value is not None and not isinstance(value, list):
            expected_type = self.dict[key]["type"]
            if expected_type == "int" and not isinstance(value, int):
                if isinstance(value, float):
                    value = int(round(value))
                else:
                    try:
                        value = int(value)
                    except Exception as e:
                        if NULLIFY_INVALID:
                            value = None
                            LOGGER.warning(f"{e}: Unable to convert value {value} to int for {key}, set to None")  # noqa
                        else:
                            raise e
            elif expected_type == "float" and not isinstance(value, float):
                try:
                    value = float(value)
                except Exception as e:
                    if NULLIFY_INVALID:
                        value = None
                        LOGGER.warning(f"{e}: Unable to convert value {value} to float for {key}, set to None")  # noqa
                    else:
                        raise e
            else:
                value = value
        self.dict[key]["value"] = value

    def get_element(self, key: str) -> Any:
        """
        Function to retrieve value from BUFR message

        :param key: the key of the element to set (using ECCODES keys)

        :returns: value of the element
        """

        # check if we want value or an attribute (indicated by ->)
        if "->" in key:
            tokens = key.split("->")
            result = self.dict[tokens[0]][tokens[1]]
        else:
            result = self.dict[key]["value"]
        return result

    def as_bufr(self, use_cached: bool = False) -> bytes:
        """
        Function to get BUFR message encoded into bytes. Once called the bytes
        are cached and the cached value returned unless specified otherwise.

        :param use_cached: Boolean indicating whether to use cached value

        :returns: bytes containing BUFR data
        """

        if use_cached and (self.bufr is not None):
            return self.bufr
        # ===========================
        # initialise new BUFR message
        # ===========================
        bufr_msg = codes_bufr_new_from_samples("BUFR4")
        # set delayed replications, this is needed again as we only used it the
        # first time to set the keys
        if len(self.delayed_replications) > 0:
            codes_set_array(bufr_msg,
                            "inputDelayedDescriptorReplicationFactor",
                            self.delayed_replications)
        # ============================
        # iterate over keys and encode
        # ============================
        for eccodes_key in self.dict:
            value = self.dict[eccodes_key]["value"]
            if value is not None:
                if isinstance(value, list):
                    try:
                        codes_set_array(bufr_msg, eccodes_key, value)
                    except Exception as e:
                        LOGGER.error(f"error calling codes_set_array({bufr_msg}, {eccodes_key}, {value}): {e}")  # noqa
                        raise e
                else:
                    try:
                        codes_set(bufr_msg, eccodes_key, value)
                    except Exception as e:
                        LOGGER.error(f"error calling codes_set({bufr_msg}, {eccodes_key}, {value}): {e}")  # noqa
                        raise e
        # ==============================
        # Message now ready to be packed
        # ==============================
        try:
            codes_set(bufr_msg, "pack", True)
        except CodesInternalError as e:
            LOGGER.error(f"error calling codes_set({bufr_msg}, 'pack', True): {e}")  # noqa
            LOGGER.error(json.dumps(self.dict, indent=4))
            LOGGER.error("null message returned")
            codes_release(bufr_msg)
            return self.bufr
        except Exception as e:
            LOGGER.error(f"error calling codes_set({bufr_msg}, 'pack', True): {e}") # noqa
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

    def md5(self) -> str:
        """
        Calculates and returns md5 of BUFR message

        :returns: md5 of BUFR message
        """
        bufr = self.as_bufr(use_cached=True)

        if bufr is not None:
            return hashlib.md5(bufr).hexdigest()
        else:
            return None

    def parse(self, data: dict, mappings: dict) -> None:
        """
        Function to parse observation data and station metadata, mapping to the
        specified BUFR sequence.

        :param data: dictionary of key value pairs containing the
                    data to be encoded.
        :param mappings: dictionary containing list of BUFR elements to
                        encode (specified using ECCODES key) and whether
                        to get the value from (fixed, csv or metadata)
        :returns: `None`
        """
        # ==================================================
        # Parse the data.
        # ==================================================
        for section in ("header", "data"):
            for element in mappings[section]:
                # get eccodes key
                eccodes_key = element["eccodes_key"]
                # get value
                value = get_(eccodes_key, mappings[section], data)
                # ===============================
                # apply specified scaling to data
                # ===============================
                if value in MISSING:
                    value = None
                else:
                    scale = None
                    offset = None
                    try:
                        if "scale" in element:
                            scale = parse_value(element["scale"], data)
                        if "offset" in element:
                            offset = parse_value(element["offset"], data)
                        value = apply_scaling(value, scale, offset)
                    except Exception as e:
                        LOGGER.error(f"Error scaling data: scale={scale}, offet={offset}, value={value}")  # noqa
                        LOGGER.error(f"data: {data}")
                        raise e
                # ==================
                # now validate value
                # ==================
                valid_min = None
                valid_max = None
                if "valid_min" in element:
                    valid_min = parse_value(element["valid_min"], data)  # noqa
                if "valid_max" in element:
                    valid_max = parse_value(element["valid_max"], data)  # noqa
                try:
                    value = validate_value(element["eccodes_key"], value,
                                           valid_min, valid_max,
                                           NULLIFY_INVALID)
                except Exception as e:
                    if NULLIFY_INVALID:
                        LOGGER.warning(f"Error raised whilst validating {element['eccodes_key']}, value set to None")  # noqa
                        LOGGER.warning(f"data: {data}")
                        value = None
                    else:
                        LOGGER.error(f"Error raised whilst validating {element['eccodes_key']}, value set to None")  # noqa
                        LOGGER.error(f"data: {data}")
                        raise e

                # ==================================================
                # at this point we should have the eccodes key and a
                # validated value to use, add to dict
                # ==================================================
                self.set_element(eccodes_key, value)

    def get_datetime(self) -> datetime:
        """
        Function to extract characteristic date and time from the BUFR message

        :returns: `datetime.datetime` of ISO8601 representation of the
                  characteristic date/time
        """

        return datetime(
            self.get_element("typicalYear"),
            self.get_element("typicalMonth"),
            self.get_element("typicalDay"),
            self.get_element("typicalHour"),
            self.get_element("typicalMinute")
        )


def transform(data: str, mappings: dict) -> Iterator[dict]:
    """
    This function returns an iterator to process each line in the input CSV
    string. On each iteration a dictionary is returned containing the BUFR
    encoded data. The mapping to BUFR is specified by the "mappings"
    dictionary using the ecCodes keys. For more information and a list of the
    keys see the tables at:

        https://confluence.ecmwf.int/display/ECC/WMO%3D37+element+table

    The dictionary returned by the iterator contains the following keys:

        - ["bufr4"] = data encoded into BUFR;
        - ["_meta"] = GeoJSON metadata on the data.

    The ["_meta"] element includes the following:

        - ["id"] = identifier for report (WIGOS_<WSI>_<ISO8601>);
        - ["geometry"] = GeoJSON geometry object;
        - ["properties"] = key/value pairs of properties/attributes

    The ["_meta"]["properties"] element includes the following:

        - ["md5"] = md5 checksum of BUFR encoded data;
        - ["wigos_station_identifier"] = WIGOS identifier;
        - ["datetime"] = characteristic date of data;
        - ["originating_centre"] = Originating centre (see Common code table C11);  # noqa
        - ["data_category"] = Category of data, see BUFR Table A.

    :param data: string containing csv separated data. First line should
                contain the column headers, second line the data
    :param metadata: string containing static CSV metadata to include in the BUFR  # noqa
    :param mappings: dictionary containing list of BUFR elements to
                    encode (specified using ecCodes key) and whether
                    to get the value from (fixed, csv or metadata)

    :returns: iterator
    """

    # ======================
    # validate mapping files
    # ======================
    e = validate_mapping(mappings)
    if e is not SUCCESS:
        raise ValueError("Invalid mappings")

    # Get WSI
    wsi = mappings["wigos_station_identifier"].split(":")
    if len(wsi) != 2:
        raise ValueError("Invalid wigos_station_identifier mapping specified")
    if wsi[0] == "const":
        read_wsi_from_file = False
        wsi_value = wsi[1]
        wsi_field = None
    else:
        read_wsi_from_file = True
        wsi_field = wsi[1]
        wsi_value = None

    # ==========================================================
    # Now extract descriptors and replications from mapping file
    # ==========================================================
    short_delayed_replications = mappings["inputShortDelayedDescriptorReplicationFactor"]  # noqa
    delayed_replications = mappings["inputDelayedDescriptorReplicationFactor"]
    extended_delayed_replications = mappings["inputExtendedDelayedDescriptorReplicationFactor"]   # noqa

    # get number of rows to skip
    skip = mappings["number_header_rows"]
    col_names_row = mappings["column_names_row"] - 1

    unexpanded_descriptors = get_("unexpandedDescriptors", mappings["header"], data = None)  # noqa
    table_version = get_("masterTablesVersionNumber", mappings["header"], data = None)  # noqa

    # =========================================
    # Now we need to convert string back to CSV
    # and iterate over rows
    # =========================================
    fh = StringIO(data)
    try:
        reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    except Exception as e:
        LOGGER.error(f"Error reading csv data\n{data}")
        raise e

    # read in header rows
    if skip > 0:
        rows_read = 0
        while rows_read < skip:
            row = next(reader)
            if rows_read == col_names_row:
                col_names = row

            rows_read += 1

    # initialise new BUFRMessage (and reuse later)
    try:
        message = BUFRMessage(unexpanded_descriptors,
                              short_delayed_replications,
                              delayed_replications,
                              extended_delayed_replications,
                              table_version)
    except Exception as e:
        LOGGER.error("Error initialising BUFR message")
        raise e
    # now iterate over remaining rows
    for row in reader:
        wsi = None
        result = dict()
        # check and make sure we have ascii data
        for val in row:
            if isinstance(val, str):
                if not val.isascii():
                    if NULLIFY_INVALID:
                        LOGGER.warning(f"csv read error, non ASCII data detected ({val}), skipping row")  # noqa
                        LOGGER.warning(row)
                        continue
                    else:
                        raise ValueError
        # valid data row, make dictionary
        data_dict = dict(zip(col_names, row))
        # parse and split WSI
        try:
            # extract WSI and split into required components
            if read_wsi_from_file:
                wsi = data_dict[wsi_field]
            else:
                wsi = wsi_value
            wsi_series, wsi_issuer, wsi_issue_number, wsi_local = wsi.split("-")   # noqa
            data_dict["_wsi_series"] = wsi_series
            data_dict["_wsi_issuer"] = wsi_issuer
            data_dict["_wsi_issue_number"] = wsi_issue_number
            data_dict["_wsi_local"] = wsi_local
        except Exception as e:
            LOGGER.error("Error parsing WIGOS station identifier")
            LOGGER.error(f"data:{data_dict}")
            raise ValueError(e)
        # reset BUFR message to clear data
        message.reset()
        try:
            # parse to BUFR sequence
            message.parse(data_dict, mappings)
            # encode to BUFR
            result["bufr4"] = message.as_bufr()
        except Exception as e:
            LOGGER.error(e)
            LOGGER.error("Error encoding BUFR, BUFR set to None")
            LOGGER.error(f"data:{data_dict}")
            result["bufr4"] = None

        # now identifier based on WSI and observation date as identifier
        isodate = message.get_datetime().strftime('%Y%m%dT%H%M%S')
        rmk = f"WIGOS_{wsi}_{isodate}"

        # now additional metadata elements
        result["_meta"] = {
            "id": rmk,
            "geometry": {
                "type": "Point",
                "coordinates": [
                    message.get_element('#1#longitude'),
                    message.get_element('#1#latitude')
                ]
            },
            "properties": {
                "md5": message.md5(),
                "wigos_station_identifier": wsi,
                "datetime": message.get_datetime(),
                "originating_centre": message.get_element("bufrHeaderCentre"),
                "data_category": message.get_element("dataCategory")
            }
        }

        time_ = datetime.now(timezone.utc).isoformat()
        LOGGER.info(f"{time_}|{result['_meta']}")

        # now yield result back to caller
        yield result
    fh.close()
