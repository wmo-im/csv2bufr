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

__version__ = '0.2.0'

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
from jsonpath_ng.ext import parser
from jsonschema import validate

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")

NULLIFY_INVALID = True  # TODO: move to env. variable

LOGGER = logging.getLogger(__name__)

THISDIR = os.path.dirname(os.path.realpath(__file__))
MAPPINGS = f"{THISDIR}{os.sep}resources{os.sep}mappings"

BUFR_TABLE_VERSION = 36  # default BUFR table version
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

# dictionary to store jsonpath parsers, these are compiled the first time that
# they are used.
jsonpath_parsers = dict()


# custom error handlers
class MappingError(Exception):
    pass


def parse_wigos_id(wigos_id: str) -> dict:
    """
    Returns the WIGOS Identifier (wigos_id) in string representation
    as the individual components in a dictionary. The WIGOS Identifier takes
    the following form:

        <WIGOS identifier series>-<issuer>-<issue number>-<local identifier>

    See https://community.wmo.int/wigos-station-identifier for more
    information.

    :param wigos_id: WIGOS Station Identifier (WSI) as a string, e.g.
        "0-20000-0-ABCD123"

    :returns: `dict` containing components of the WIGOS identifier:
        "wid_series", "wid_issuer", "wid_issue_number" and "local_identifier"

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
    except MappingError as e:
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
                message = f"{e} Element set to missing"
                LOGGER.debug(message)
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
        template["number_header_rows"] = 1
        template["names_on_row"] = 1
        template["header"] = []
        # create header section
        for element in HEADERS:
            value = None
            if element == "unexpandedDescriptors":
                value = self.descriptors
            entry = {
                "eccodes_key": element,
                "value": value,
                "csv_column": None,
                "jsonpath": None,
                "valid_min": None,
                "valid_max": None,
                "scale": None,
                "offset": None
            }
            template["header"].append(entry)
        # now create data section
        template["data"] = []
        for element in self.dict:
            if element not in HEADERS:
                entry = {
                    "eccodes_key": element,
                    "value": None,
                    "csv_column": None,
                    "jsonpath": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                }
                template["data"].append(entry)
        # add WIGOS identifier
        template["wigos_identifier"] = {
            "value": None,
            "jsonpath": None,
            "csv_column": None
        }
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
                LOGGER.debug(f"int expected for {key} but received {type(value)} ({value})")  # noqa
                if isinstance(value, float):
                    value = int(round(value))
                else:
                    try:
                        value = int(value)
                    except Exception as e:
                        if NULLIFY_INVALID:
                            value = None
                            LOGGER.debug(f"{e}: Unable to convert value to int, set to None")  # noqa
                        else:
                            raise e
                LOGGER.debug(f"value converted to int ({value})")
            elif expected_type == "float" and not isinstance(value, float):
                LOGGER.debug(f"float expected for {key} but received {type(value)} ({value})")  # noqa
                value = float(value)
                LOGGER.debug(f"value converted to float ({value})")
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
                LOGGER.debug(f"setting value {value} for element {eccodes_key}.")  # noqa
                if isinstance(value, list):
                    try:
                        LOGGER.debug("calling codes_set_array")
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
            LOGGER.debug(f"error calling codes_set({bufr_msg}, 'pack', True): {e}")  # noqa
            LOGGER.debug(json.dumps(self.dict, indent=4))
            LOGGER.debug("null message returned")
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
        # extract wigos ID from metadata
        # Is this the right place for this?
        # ==================================================
        try:
            wigosID = metadata["wigosIds"][0]["wid"]
            tokens = parse_wigos_id(wigosID)
            for token in tokens:
                metadata["wigosIds"][0][token] = tokens[token]
        except (Exception, AssertionError):
            LOGGER.warning("WigosID not parsed automatically. wigosID element not in metadata?")  # noqa
        # ==================================================
        # now parse the data.
        # ==================================================
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
                    # make sure column is in data
                    if column not in data:
                        message = f"column '{column}' not found in data dictionary"  # noqa
                        raise ValueError(message)
                    value = data[column]
                elif jsonpath is not None:
                    if jsonpath not in jsonpath_parsers:
                        jsonpath_parsers[jsonpath] = parser.parse(jsonpath)
                    p = jsonpath_parsers[jsonpath]
                    query = p.find(metadata)
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
                    valid_max = element["valid_max"]
                LOGGER.debug(f"validating value {value} for element {element['eccodes_key']} against range")  # noqa
                try:
                    value = validate_value(element["eccodes_key"], value,
                                           valid_min, valid_max,
                                           NULLIFY_INVALID)
                    LOGGER.debug(f"value {value} validated for element {element['eccodes_key']}")  # noqa
                except Exception as e:
                    if NULLIFY_INVALID:
                        LOGGER.debug(f"Error raised whilst validating {element['eccodes_key']}, value set to None")  # noqa
                        value = None
                    else:
                        raise e

                # ==================================================
                # at this point we should have the eccodes key and a
                # validated value to use, add to dict
                # ==================================================
                self.set_element(eccodes_key, value)
                LOGGER.debug(f"value {value} updated for element {element['eccodes_key']}")  # noqa

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


def transform(data: str, metadata: dict, mappings: dict) -> Iterator[dict]:
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
        - ["wigos_id"] = WIGOS identifier;
        - ["data_date"] = characteristic date of data;
        - ["originating_centre"] = Originating centre (see Common code table C11);  # noqa
        - ["data_category"] = Category of data, see BUFR Table A.

    :param data: string containing csv separated data. First line should
                contain the column headers, second line the data
    :param metadata: dictionary containing the metadata for the station
                    from OSCAR surface
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
    # ==========================================================
    # Now extract descriptors and replications from mapping file
    # ==========================================================
    delayed_replications = mappings["inputDelayedDescriptorReplicationFactor"]
    path = "$.header[?(@.eccodes_key=='unexpandedDescriptors')]"
    unexpanded_descriptors = \
        parser.parse(path).find(mappings)[0].value["value"]

    path = "$.header[?(@.eccodes_key=='masterTablesVersionNumber')]"
    table_version = parser.parse(path).find(mappings)[0].value["value"]

    if "number_header_rows" in mappings:
        nheaders = mappings["number_header_rows"]
    else:
        nheaders = 1

    if "names_on_row" in mappings:
        names = mappings["names_on_row"]
    else:
        names = 1

    # =========================================
    # Now we need to convert string back to CSV
    # and iterate over rows
    # =========================================
    fh = StringIO(data)
    reader = csv.reader(fh, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
    # counter to keep track
    rows_read = 0
    # first read in and process header rows
    while rows_read < nheaders:
        row = next(reader)
        if rows_read == names - 1:
            col_names = row
        rows_read += 1
        continue

    # initialise new BUFRMessage (and reuse later)
    LOGGER.debug("Initializing new BUFR message")
    message = BUFRMessage(unexpanded_descriptors, delayed_replications,
                          table_version)

    # now iterate over remaining rows
    for row in reader:
        result = dict()
        LOGGER.debug(f"Processing row {rows_read}")
        # check and make sure we have ascii data
        for val in row:
            if isinstance(val, str):
                if not val.isascii():
                    if NULLIFY_INVALID:
                        LOGGER.debug(f"csv read error, non ASCII data detected ({val}), skipping row")  # noqa
                        LOGGER.debug(row)
                        continue
                    else:
                        raise ValueError
        # valid data row, make dictionary
        data_dict = dict(zip(col_names, row))
        # reset BUFR message to clear data
        message.reset()
        # parse to BUFR sequence
        LOGGER.debug("Parsing data")
        message.parse(data_dict, metadata, mappings)
        # encode to BUFR
        LOGGER.debug("Parsing data")
        try:
            result["bufr4"] = message.as_bufr()
        except Exception as e:
            LOGGER.error("Error encoding BUFR, null returned")
            LOGGER.error(e)
            result["bufr4"] = None

        # now identifier based on WSI and observation date as identifier
        wsi = metadata['wigosIds'][0]['wid'] if 'wigosIds' in metadata else "N/A"  # noqa
        isodate = message.get_datetime().strftime('%Y%m%dT%H%M%S')
        rmk = f"WIGOS_{wsi}_{isodate}"

        # now additional metadata elements
        LOGGER.debug("Adding metadata elements")
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
            "wigos_id": wsi,
            "data_date": message.get_datetime(),
            "originating_centre": message.get_element("bufrHeaderCentre"),
            "data_category": message.get_element("dataCategory")
        }

        time_ = datetime.now(timezone.utc).isoformat()
        LOGGER.info(f"{time_}|{result['_meta']}")

        # increment ticker
        rows_read += 1

        # now yield result back to caller

        yield result
