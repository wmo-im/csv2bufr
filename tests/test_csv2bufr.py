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
import hashlib
from io import StringIO
import logging

import pytest

from eccodes import codes_bufr_new_from_samples, codes_release
from csv2bufr import (validate_mapping_dict, apply_scaling, validate_value,
                      encode, transform, SUCCESS)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel("DEBUG")


# test data
@pytest.fixture
def mapping_dict():
    return {
        "inputDelayedDescriptorReplicationFactor": None,
        "sequence": [
            {"key": "edition", "value": 4, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "masterTableNumber", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "bufrHeaderCentre", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "bufrHeaderSubCentre", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "updateSequenceNumber", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "section1Flags", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "dataCategory", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "internationalDataSubCategory", "value": 6, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "dataSubCategory", "value": None, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "masterTablesVersionNumber", "value": 36, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "numberOfSubsets", "value": 1, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "observedData", "value": 1, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "compressedData", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "typicalYear", "value": None, "column": "year", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "typicalMonth", "value": None, "column": "month", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "typicalDay", "value": None, "column": "day", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "typicalHour", "value": None, "column": "hour", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "typicalMinute", "value": None, "column": "minute", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "unexpandedDescriptors", "value": [301021, 301011, 301012, 10051, 12101], "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#year", "value": None, "column": "year", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#month", "value": None, "column": "month", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#day", "value": None, "column": "day", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#hour", "value": None, "column": "hour", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#minute", "value": None, "column": "minute", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#latitude", "value": None, "column": "latitude", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#longitude", "value": None, "column": "longitude", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#pressureReducedToMeanSeaLevel", "value": None, "column": "pressure", "valid-min": None, "valid-max": None, "scale": None, "offset": None},  # noqa
            {"key": "#1#airTemperature", "value": None, "column": "air_temperature", "valid-min": None, "valid-max": None, "scale": None, "offset": None}  # noqa
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
            "observations": {
                "#1#airTemperature": {
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
                "#1#pressureReducedToMeanSeaLevel": {
                    "value": {
                        "eccodes_key": "#1#pressureReducedToMeanSeaLevel"
                    },
                    "cf_standard_name": "pressure_at_mean_sea_level",
                    "units": {
                        "eccodes_key": "#1#pressureReducedToMeanSeaLevel->units"
                    },
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                }
            }
        }
    }


@pytest.fixture
def json_result():
    return {
        "id": None,
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [0.0, 55.154]
        },
        "properties": {
            "phenomenonTime": "2021-11-18 18:00:00+00:00",
            "resultTime": None,
            "observations": {
                "#1#airTemperature": {
                    "value": 290.31,
                    "cf_standard_name": "air_temperature",
                    "units": "K",
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                },
                "#1#pressureReducedToMeanSeaLevel": {
                    "value": 100130,
                    "cf_standard_name": "pressure_at_mean_sea_level",
                    "units": "Pa",
                    "sensor_height_above_local_ground": None,
                    "sensor_height_above_mean_sea_level": None,
                    "valid_min": None,
                    "valid_max": None,
                    "scale": None,
                    "offset": None
                }
            }
        }
    }


@pytest.fixture
def station_dict():
    return {
        "metadata": {
            "last-sync": "2021-10-22"
        },
        "data": {
            "station-name": "test data"
        }
    }


# test to check whether eccodes is installed
def test_eccodes():
    # call to eccodes library to test if accessible
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # call to release the BUFR message
    codes_release(bufr_msg)
    assert True


# test to check validate_mapping_dict is not broken
def test_validate_mapping_dict_pass(mapping_dict):
    success = validate_mapping_dict(mapping_dict)
    assert success == SUCCESS


# test to check validate_mapping_dict fails when we expect it to
def test_validate_mapping_dict_fail():
    # not sure on this one, do we need this test and if so have many
    # different exceptions do we want to test?
    test_data = {
        "inputDelayedDescriptorReplicationFactor": [0, 0],
        "sequence": [
            {"key": "abc", "value": 1, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": 1},  # noqa
            {"key": "def", "value": None, "column": "col1", "valid-min": 0, "valid-max": 10, "scale": None, "offset": None},  # noqa
            {"key": "ghi", "value": None, "column": "col2", "valid-min": 250.0, "valid-max": 350.0, "scale": 0.0, "offset":  273.15},  # noqa
            {"key": "jkl", "value": None, "column": "col3", "valid-min": 90000.0, "valid-max": 120000.0, "scale": 2.0, "offset": 0.0}  # noqa
        ]
    }
    try:
        success = validate_mapping_dict(test_data)
    except Exception:
        success = False
    assert success != SUCCESS


# test to make sure apply_scaling works as expected
def test_apply_scaling():
    test_element = {"scale": 1, "offset": 20.0}
    test_value = 10.0
    assert 120.0 == apply_scaling(test_value, test_element)


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


# check that test encode works
def test_encode(mapping_dict, data_dict):
    msg = encode(mapping_dict, data_dict)
    key = hashlib.md5(msg.read()).hexdigest()
    assert key == "981938dbd97be3e5adc8e7b1c6eb642c"


# check that test transform works
def test_transform(data_dict, mapping_dict, station_dict):

    output = StringIO()

    writer = csv.DictWriter(output, quoting=csv.QUOTE_NONNUMERIC,
                            fieldnames=data_dict.keys())
    writer.writeheader()
    writer.writerow(data_dict)
    data = output.getvalue()

    result = transform(data, mapping_dict, station_dict)
    assert isinstance(result, dict)
    assert list(result.keys())[0] == '981938dbd97be3e5adc8e7b1c6eb642c'
    assert len(list(result.keys())) == 1


def test_json(data_dict, mapping_dict, station_dict, json_template,
              json_result):
    # first encode BUFR
    output = StringIO()
    writer = csv.DictWriter(output, quoting=csv.QUOTE_NONNUMERIC,
                            fieldnames=data_dict.keys())
    writer.writeheader()
    writer.writerow(data_dict)
    data = output.getvalue()
    result = transform(data, mapping_dict, station_dict)
    # write to file
    with open("test.bufr4", "wb") as fh:
        fh.write(result[0].read())

    # next  read and convert to JSON
    with open("test.bufr4", "rb") as fh:
        handle = codes_bufr_new_from_file(fh )
    json_dict = bufr_to_json(handle, json_template)

    # copy id and resulttime to expected result, these are generated at the
    # time of testing.
    json_result['id'] = json_dict['id']
    json_result['properties']['resultTime'] = \
        json_dict['properties']['resultTime']

    # now compare
    assert json_result == json_dict
