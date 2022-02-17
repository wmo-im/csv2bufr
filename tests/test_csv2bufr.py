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

import csv
from io import StringIO
import logging
import json

from eccodes import (codes_bufr_new_from_samples, codes_release)
import pytest

from csv2bufr import (validate_mapping, apply_scaling, validate_value,
                      transform, SUCCESS)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel("DEBUG")


# test data
@pytest.fixture
def mapping_dict():
    return {
        "inputDelayedDescriptorReplicationFactor": [],
        "number_header_rows": 1,
        "names_on_row": 1,
        "header": [
            {"eccodes_key": "edition", "value": 4},  # noqa
            {"eccodes_key": "masterTableNumber", "value": 0},  # noqa
            {"eccodes_key": "bufrHeaderCentre", "value": 0},  # noqa
            {"eccodes_key": "bufrHeaderSubCentre", "value": 0},  # noqa
            {"eccodes_key": "updateSequenceNumber", "value": 0},  # noqa
            {"eccodes_key": "dataCategory", "value": 0},  # noqa
            {"eccodes_key": "internationalDataSubCategory", "value": 6},  # noqa
            {"eccodes_key": "masterTablesVersionNumber", "value": 36},  # noqa
            {"eccodes_key": "numberOfSubsets", "value": 1},  # noqa
            {"eccodes_key": "observedData", "value": 1},  # noqa
            {"eccodes_key": "compressedData", "value": 0},  # noqa
            {"eccodes_key": "typicalYear", "csv_column": "year"},  # noqa
            {"eccodes_key": "typicalMonth", "csv_column": "month"},  # noqa
            {"eccodes_key": "typicalDay", "csv_column": "day"},  # noqa
            {"eccodes_key": "typicalHour", "csv_column": "hour"},  # noqa
            {"eccodes_key": "typicalMinute", "csv_column": "minute"},  # noqa
            {"eccodes_key": "unexpandedDescriptors","value": [301021, 301011, 301012, 10051, 12101]}  # noqa
        ],
        "data": [
            {"eccodes_key": "#1#year", "csv_column": "year"},  # noqa
            {"eccodes_key": "#1#month", "csv_column": "month"},  # noqa
            {"eccodes_key": "#1#day", "csv_column": "day"},  # noqa
            {"eccodes_key": "#1#hour", "csv_column": "hour"},  # noqa
            {"eccodes_key": "#1#minute", "csv_column": "minute"},  # noqa
            {"eccodes_key": "#1#latitude", "csv_column": "latitude"},  # noqa
            {"eccodes_key": "#1#longitude", "csv_column": "longitude"},  # noqa
            {"eccodes_key": "#1#pressureReducedToMeanSeaLevel", "csv_column": "pressure"},  # noqa
            {"eccodes_key": "#1#airTemperature", "csv_column": "air_temperature"}  # noqa
        ]
    }


@pytest.fixture
def data_dict():
    return {
        "air_temperature": 290.31,
        "pressure": 100130,
        "latitude": 55.154,
        "longitude": 0.0,
        "year": 2021,
        "month": 11,
        "day": 18,
        "hour": 18,
        "minute": 0
    }


@pytest.fixture
def json_template():
    return {
        "id": None,
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [{"eccodes_key": "#1#longitude"},
                            {"eccodes_key": "#1#latitude"}]
        },
        "properties": {
            "identifier": None,
            "phenomenonTime": {
                "format": "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:00+00:00",
                "args": [
                    {"eccodes_key": "#1#year"},
                    {"eccodes_key": "#1#month"},
                    {"eccodes_key": "#1#day"},
                    {"eccodes_key": "#1#hour"},
                    {"eccodes_key": "#1#minute"}
                ]},
            "resultTime": None,
            "observations": [
                {
                    "name":"#1#airTemperature",
                    "value": {
                        "eccodes_key": "#1#airTemperature"
                    },
                    "cf_standard_name": "air_temperature",
                    "units": {
                        "eccodes_key": "#1#airTemperature->units"
                    },
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                },
                {
                    "name":"#1#pressureReducedToMeanSeaLevel",
                    "value": {
                        "eccodes_key": "#1#pressureReducedToMeanSeaLevel"
                    },
                    "cf_standard_name": "pressure_at_mean_sea_level",
                    "units": {
                        "eccodes_key":
                            "#1#pressureReducedToMeanSeaLevel->units"
                    },
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                }
            ]
        },
        "_meta": {
            "units": {
                "K": "Celsius"
            }
        }
    }


@pytest.fixture
def json_result():
    return {
        "id": "WIGOS_0-1-2-ABCD_20211118T180000",
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [0.0, 55.154]
        },
        "properties": {
            "identifier": "WIGOS_0-1-2-ABCD_20211118T180000",
            "phenomenonTime": "2021-11-18T18:00:00+00:00",
            "resultTime": None,
            "observations": [
                {
                    "name":"#1#airTemperature",
                    "value": 17.160000000000025,
                    "cf_standard_name": "air_temperature",
                    "units": "Celsius",
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                },
                {
                    "name":"#1#pressureReducedToMeanSeaLevel",
                    "value": 100130.0,
                    "cf_standard_name": "pressure_at_mean_sea_level",
                    "units": "Pa",
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                }
            ]
        }
    }


@pytest.fixture
def data_to_encode():
    return {
            "edition": 4,
            "masterTableNumber": 0,
            "bufrHeaderCentre": 0,
            "bufrHeaderSubCentre": 0,
            "updateSequenceNumber": 0,
            "section1Flags": 0,
            "dataCategory": 0,
            "internationalDataSubCategory": 6,
            "masterTablesVersionNumber": 36,
            "numberOfSubsets": 1,
            "observedData": 1,
            "compressedData": 0,
            "typicalYear": 2021.0,
            "typicalMonth": 11.0,
            "typicalDay": 18.0,
            "typicalHour": 18.0,
            "typicalMinute": 0.0,
            "unexpandedDescriptors": [
                301021,
                301011,
                301012,
                10051,
                12101
            ],
            "#1#year": 2021.0,
            "#1#month": 11.0,
            "#1#day": 18.0,
            "#1#hour": 18.0,
            "#1#minute": 0.0,
            "#1#latitude": 55.154,
            "#1#longitude": 0.0,
            "#1#pressureReducedToMeanSeaLevel": 100130.0,
            "#1#airTemperature": 290.31
    }


@pytest.fixture
def station_dict():
    return {
        "metadata": {
            "last-sync": "2021-10-22"
        },
        "data": {
            "station-name": "test data"
        },
        "wigosIds": [
            {"wid": "0-1-2-ABCD"}
        ]
    }


# test to check whether eccodes is installed
def test_eccodes():
    # call to eccodes library to test if accessible
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # call to release the BUFR message
    codes_release(bufr_msg)
    assert True


# test to check validate_mapping is not broken
def test_validate_mapping_pass(mapping_dict):
    success = validate_mapping(mapping_dict)
    assert success == SUCCESS


# test to check validate_mapping fails when we expect it to
def test_validate_mapping_fail():
    # not sure on this one, do we need this test and if so have many
    # different exceptions do we want to test?
    test_data = {
        "inputDelayedDescriptorReplicationFactor": [],
        "header": [],
        "data": [
            {"eccodes_key": "abc", "value": 1, "offset": 1},  # noqa
            {"eccodes_key": "def", "csv_column": "col1", "valid-min": 0, "valid-max": 10},  # noqa
            {"eccodes_key": "ghi", "csv_column": "col2", "valid-min": 250.0, "valid-max": 350.0, "scale": 0.0, "offset":  273.15},  # noqa
            {"eccodes_key": "jkl", "csv_column": "col3", "valid-min": 90000.0, "valid-max": 120000.0, "scale": 2.0, "offset": 0.0}  # noqa
        ]
    }
    try:
        success = validate_mapping(test_data)
    except Exception:
        success = False
    assert success != SUCCESS


# test to make sure apply_scaling works as expected
def test_apply_scaling():
    scale = 1
    offset = 20.0
    test_value = 10.0
    assert 120.0 == apply_scaling(test_value, scale, offset)


# test to check that valid_value works
def test_validate_value_pass():
    input_value = 10.0
    try:
        value = validate_value("test value", input_value, 0.0, 100.0, False)
    except Exception:
        assert False
    assert value == input_value


# test to check that valid_value fails when we expect it to
def test_validate_value_fail():
    input_value = 10.0
    try:
        _ = validate_value("test value", input_value, 0.0, 9.9, False)
    except Exception:
        return
    assert False


# test to check that valid_value returns null value when we expect it to
def test_validate_value_nullify():
    input_value = 10.0
    try:
        value = validate_value("test value", input_value, 0.0, 9.9, True)
    except Exception:
        assert False
    assert value is None


# check that test transform works
def test_transform(data_dict, station_dict, mapping_dict):
    # create CSV
    output = StringIO()
    writer = csv.DictWriter(output, quoting=csv.QUOTE_NONNUMERIC,
                            fieldnames=data_dict.keys())
    writer.writeheader()
    writer.writerow(data_dict)
    data = output.getvalue()
    result = transform(data, station_dict, mapping_dict)
    for item in result:
        assert isinstance(item, dict)
        assert "_meta" in item

        item_meta_keys = ['data_category', 'data_date', 'identifier',
                          'md5', 'originating_centre', 'wigos_id']

        assert sorted(item["_meta"].keys()) == item_meta_keys

        assert item["_meta"]["md5"] == "981938dbd97be3e5adc8e7b1c6eb642c"


def test_json(data_dict, station_dict, mapping_dict, json_template,
              json_result):
    # create CSV
    output = StringIO()
    writer = csv.DictWriter(output, quoting=csv.QUOTE_NONNUMERIC,
                            fieldnames=data_dict.keys())
    writer.writeheader()
    writer.writerow(data_dict)
    data = output.getvalue()
    # transform CSV to BUFR
    result = transform(data, station_dict, mapping_dict, json_template)
    for item in result:
        geojson = json.loads(item["geojson"])
        # we need to copy result time to our expected json result
        json_result["properties"]["resultTime"] = \
            geojson["properties"]["resultTime"]
        # now compare
        assert json.dumps(geojson) == json.dumps(json_result)
