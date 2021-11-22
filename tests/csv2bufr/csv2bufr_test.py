from eccodes import codes_bufr_new_from_samples, codes_release
from csv2bufr import validate_mapping_dict, apply_scaling, validate_value, encode, transform, SUCCESS
import logging
import hashlib
import pytest

_LOGGER = logging.getLogger( __name__ )
_LOGGER.setLevel( "DEBUG" )

# test data
@pytest.fixture
def mapping_dict():
    return {
        "inputDelayedDescriptorReplicationFactor": None,
        "sequence": [
            {"key": "edition", "value": 4, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "masterTableNumber", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "bufrHeaderCentre", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "bufrHeaderSubCentre", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "updateSequenceNumber", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "section1Flags", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "dataCategory", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "internationalDataSubCategory", "value": 6, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "dataSubCategory", "value": None, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "masterTablesVersionNumber", "value": 36, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "numberOfSubsets", "value": 1, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "observedData", "value": 1, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "compressedData", "value": 0, "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "typicalYear", "value": None, "column": "year", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "typicalMonth", "value": None, "column": "month", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "typicalDay", "value": None, "column": "day", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "typicalHour", "value": None, "column": "hour", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "typicalMinute", "value": None, "column": "minute", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "unexpandedDescriptors", "value": [301021, 301011, 301012, 10051, 12101], "column": None, "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#year", "value": None, "column": "year", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#month", "value": None, "column": "month", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#day", "value": None, "column": "day", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#hour", "value": None, "column": "hour", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#minute", "value": None, "column": "minute", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#latitude", "value": None, "column": "latitude", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#longitude", "value": None, "column": "longitude", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#pressureReducedToMeanSeaLevel", "value": None, "column": "pressure", "valid-min": None, "valid-max": None, "scale": None, "offset": None},
            {"key": "#1#airTemperature", "value": None, "column": "air_temperature", "valid-min": None, "valid-max": None, "scale": None, "offset": None}
        ]
    }

@pytest.fixture
def data_dict():
    return{
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
def station_dict():
    return {
        "metadata": {
            "last-sync": "2021-10-22"
        },
        "data": {
            "station-name":"test data"
        }
    }

# test to check whether eccodes is installed
def test_eccodes():
    # call to eccodes library to test if accessible
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # call to release the BUFR message
    codes_release( bufr_msg )
    assert True

# test to check validate_mapping_dict is not broken
def test_validate_mapping_dict_pass( mapping_dict ):
    success = validate_mapping_dict( mapping_dict  )
    assert success == SUCCESS

# test to check validate_mapping_dict fails when we expect it to
def test_validate_mapping_dict_fail():
    # not sure on this one, do we need this test and if so have many different exceptions do we want to test?
    test_data = {
        "inputDelayedDescriptorReplicationFactor": [0,0],
        "sequence": [
            {"key":"abc", "value":1,    "column": None,   "valid-min":None, "valid-max":None, "scale":None, "offset":1},
            {"key":"def", "value":None, "column": "col1", "valid-min":0, "valid-max":10, "scale":None, "offset":None},
            {"key":"ghi", "value":None, "column": "col2", "valid-min":250.0, "valid-max": 350.0, "scale": 0.0, "offset": 273.15},
            {"key":"jkl", "value":None, "column": "col3", "valid-min": 90000.0, "valid-max": 120000.0, "scale": 2.0,"offset": 0.0}
        ]
    }
    try:
        success = validate_mapping_dict( test_data )
    except Exception as e:
        success = False
    assert success != SUCCESS

# test to make sure apply_scaling works as expected
def test_apply_scaling():
    test_element = {"scale":1, "offset": 20.0}
    test_value   = 10.0
    assert 120.0 == apply_scaling(  test_value, test_element )

# test to check that valid_value works
def test_validate_value_pass():
    input_value = 10.0
    try:
        value = validate_value( "test value", input_value, 0.0, 100.0, False)
    except Exception as e:
        assert False
    assert value == input_value

# test to check that valid_value fails when we expect it to
def test_validate_value_fail():
    input_value = 10.0
    try:
        value = validate_value( "test value", input_value, 0.0, 9.9, False)
    except Exception as e:
        return
    assert False

# test to check that valid_value returns null value when we expect it to
def test_validate_value_nullify():
    input_value = 10.0
    try:
        value = validate_value( "test value", input_value, 0.0, 9.9, True)
    except Exception as e:
        assert False
    assert value is None

# check that test encode works
def test_encode(mapping_dict, data_dict):
    msg = encode( mapping_dict, data_dict )
    key = hashlib.md5(msg.read()).hexdigest()
    assert key == "981938dbd97be3e5adc8e7b1c6eb642c"

# check that test transform works
def test_transform(data_dict, mapping_dict, station_dict):
    header_row = ''
    data_row = ''
    key_count = 0
    for key in data_dict:
        value = data_dict[ key ]
        if key_count > 0:
            sep = ","
        else:
            sep = ''
        if isinstance(value, str ):
            quote = '"'
        else:
            quote = ''
        header_row += sep + '"{}"'.format( key )
        data_row += sep + quote + "{}".format( value ) + quote
        key_count += 1
    data = header_row + "\n" + data_row
    result = transform( data, mapping_dict, station_dict )
    assert isinstance(result, dict)
    assert list(result.keys())[0] == '981938dbd97be3e5adc8e7b1c6eb642c'
    assert len(list(result.keys())) == 1