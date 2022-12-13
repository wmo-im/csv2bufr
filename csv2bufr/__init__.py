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

__version__ = '0.4.dev0'

from copy import deepcopy
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

NULLIFY_INVALID = True  # TODO: move to env. variable

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


# function to find position in array of requested elemennt
def index_(key, mapping):
    idx = 0
    for item in mapping:
        if item['eccodes_key'] == key:
            return idx
        idx += 1
    raise ValueError


def parse_value(element: str, data: dict, metadata: dict):
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
            raise ValueError
        value = data[column]
    elif data_type[0] == "metadata":
        column = data_type[1]
        if column not in metadata:
            raise ValueError
        value = metadata[column]
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
        LOGGER.error(f"Data type ({data_type[0]}) not recognised in mapping")
        raise ValueError
    return value


# function to retrieve data
def get_(key: str, mapping: dict, data: dict, metadata: dict):
    # get position in mapping
    idx = index_(key, mapping)
    element = mapping[idx]
    value = parse_value(element['value'], data, metadata)
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
        message = "Invalid mapping dictionary"
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
    def __init__(self, descriptors: list, delayed_replications: list = list(),
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
        if len(delayed_replications) > 0:
            codes_set_array(bufr_msg,
                            "inputDelayedDescriptorReplicationFactor",
                            delayed_replications)
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
        self.bufr = None  # placeholder for BUFR bytes
        # ============================================

    def create_template(self) -> None:
        template = {}
        template["inputDelayedDescriptorReplicationFactor"] = \
            self.delayed_replications
        template["skip"] = 0
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
                        "valid_min": valid_min,
                        "valid_max": valid_max,
                        "scale": 0,
                        "offset": 0
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

    def parse(self, data: dict, metadata: dict, mappings: dict) -> None:
        """
        Function to parse observation data and station metadata, mapping to the
        specified BUFR sequence.

        :param data: dictionary of key value pairs containing the
                    data to be encoded.
        :param metadata: dictionary containing the metadata for the station
                        from OSCAR surface
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
                value = get_(eccodes_key, mappings[section], data, metadata)
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
                    valid_min = parse_value(element["valid_min"], data, metadata)  # noqa
                if "valid_max" in element:
                    valid_max = parse_value(element["valid_max"], data, metadata)  # noqa
                try:
                    value = validate_value(element["eccodes_key"], value,
                                           valid_min, valid_max,
                                           NULLIFY_INVALID)
                except Exception as e:
                    if NULLIFY_INVALID:
                        LOGGER.warning(f"Error raised whilst validating {element['eccodes_key']}, value set to None")  # noqa
                        value = None
                    else:
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


def transform(data: str, metadata: str, mappings: dict,
              wsi: str) -> Iterator[dict]:
    """
    This function returns an iterator to process each line in the input CSV
    string. On each iteration a dictionary is returned containing the BUFR
    encoded data. The mapping to BUFR is specified by the "mappings"
    dictionary using the ecCodes keys. For more information and a list of the
    keys see the tables at:

        https://confluence.ecmwf.int/display/ECC/WMO%3D37+element+table

    The dictionary returned by the iterator contains the following keys:

        - ["bufr4"] = data encoded into BUFR;
        - ["_meta"] = metadata on the data.

    The ["_meta"] element includes the following:

        - ["identifier"] = identifier for report (WIGOS_<WSI>_<ISO8601>);
        - ["geometry"] = GeoJSON geometry object;
        - ["md5"] = md5 checksum of BUFR encoded data;
        - ["wigos_station_identifier"] = WIGOS identifier;
        - ["data_date"] = characteristic date of data;
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

    # ===================
    # Parse metadata file
    # ===================
    if isinstance(metadata, str):
        fh = StringIO(metadata)
        reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONE)
        col_names = next(reader)
        metadata_dict = {}
        for row in reader:
            single_row = dict(zip(col_names, row))
            wsi = single_row['wsi']
            metadata_dict[wsi] = deepcopy(single_row)
        fh.close()
        metadata = metadata_dict[wsi]
    elif isinstance(metadata, dict):
        if wsi in metadata:
            metadata = metadata[wsi]
        else:
            LOGGER.error(f"metadata not found for {wsi} in metadata file")
            raise ValueError
    else:
        LOGGER.error("Invalid metadata")
        raise ValueError
    # ==========================================================
    # Now extract descriptors and replications from mapping file
    # ==========================================================
    delayed_replications = mappings["inputDelayedDescriptorReplicationFactor"]

    # get number of rows to skip
    skip = mappings["skip"]

    unexpanded_descriptors = get_("unexpandedDescriptors", mappings["header"], data = None, metadata = None)  # noqa
    table_version = get_("masterTablesVersionNumber", mappings["header"], data = None, metadata = None)  # noqa

    # =========================================
    # Now we need to convert string back to CSV
    # and iterate over rows
    # =========================================
    fh = StringIO(data)
    reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)

    # read header row to get names
    col_names = next(reader)

    # skip number of rows indicated
    if skip > 0:
        rows_read = 0
        while rows_read < skip:
            next(reader)
            rows_read += 1

    # initialise new BUFRMessage (and reuse later)
    message = BUFRMessage(unexpanded_descriptors, delayed_replications,
                          table_version)

    # now iterate over remaining rows
    for row in reader:
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
        # reset BUFR message to clear data
        message.reset()
        # parse to BUFR sequence
        message.parse(data_dict, metadata, mappings)
        # encode to BUFR
        try:
            result["bufr4"] = message.as_bufr()
        except Exception as e:
            LOGGER.error("Error encoding BUFR, null returned")
            LOGGER.error(e)
            result["bufr4"] = None

        # now identifier based on WSI and observation date as identifier
        isodate = message.get_datetime().strftime('%Y%m%dT%H%M%S')
        rmk = f"WIGOS_{wsi}_{isodate}"

        # now additional metadata elements
        result["_meta"] = {
            "identifier": rmk,
            "md5": message.md5(),
            "geometry": {
                "type": "Point",
                "coordinates": [
                    message.get_element('#1#longitude'),
                    message.get_element('#1#latitude')
                ]
            },
            "wigos_station_identifier": wsi,
            "data_date": message.get_datetime(),
            "originating_centre": message.get_element("bufrHeaderCentre"),
            "data_category": message.get_element("dataCategory")
        }

        time_ = datetime.now(timezone.utc).isoformat()
        LOGGER.info(f"{time_}|{result['_meta']}")

        # now yield result back to caller
        yield result
    fh.close()
