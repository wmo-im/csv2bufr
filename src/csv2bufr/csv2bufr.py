from eccodes import codes_bufr_new_from_samples, codes_set_array, codes_set, codes_get_native_type, codes_write, \
    codes_release
from jsonschema import validate
from io import StringIO, BytesIO
import csv
import logging
import hashlib

# some 'constants'
SUCCESS = True
NUMBERS = (float, int, complex)
MISSING = ("NA", "NaN", "NAN", "None")

NULLIFY_INVALID = True # move to env. variable
_log_level = "INFO" # change to read in from environment variable

# set format of logger
formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s","%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler()
ch.setFormatter( formatter )

# set log level
ch.setLevel( _log_level )

# now logger for this module
_LOGGER = logging.getLogger( __name__ )
_LOGGER.setLevel( _log_level )
_LOGGER.addHandler( ch )

# function to validate mapping dictionary
def validate_mapping_dict( mapping_dict ):
    file_schema = {
        "type": "object",
        "properties":{
            "inputDelayedDescriptorReplicationFactor":{"type":["array","null"]},
            "sequence":{"type":["array"]}
        }
    }
    # now validate
    try:
        validate( mapping_dict, file_schema )
    except Exception as e:
        message = "invalid mapping dictionary"
        _LOGGER.error(message)
        raise e
    # now schema for each element in the sequence array
    # ToDo make optional elements optional
    element_schema = {
        "type": "object",
        "properties":{
            "key":{"type": "string" },
            "value": {"type": ["boolean", "object", "array", "number", "string", "null"]},
            "column": {"type": ["string", "null"]},
            "valid-min": { "type": ["number", "null"] },
            "valid-max": {"type": ["number", "null"] },
            "scale":  {"type": ["number", "null"] },
            "offset": {"type": ["number", "null"]}
        }
    }

    # now iterate over elements and validate each
    for element in mapping_dict["sequence"]:
        try:
            validate( element, schema = element_schema )
        except Exception as e:
            message = "invalid element ({}) for {} in mapping file. {}".format( e.json_path, element["key"], e.message )
            _LOGGER.error( message )
            raise e
        if (element[ "scale" ] is None) != (element[ "offset" ] is None):
            message = "scale and offset should either both be present or both set to missing"
            message += " for {} in mapping file".format(element["key"])
            _LOGGER.error( message )
            e = ValueError(message)
            raise e
    return SUCCESS

# function to apply simple scaling and offsets
def apply_scaling( value, element ):
    if isinstance(value, NUMBERS):
        if element["scale"] is not None and element["offset"] is not None:
            try:
                value = value * pow(10, element["scale"]) + element["offset"]
            except Exception as e:
                _LOGGER.error( e.message )
                raise e
    return value

# check numeric values lie within specified range (if specified)
def validate_value(key, value, valid_min, valid_max, nullify_on_fail = False ):
    if value is None:
        return value
    if not isinstance( value, NUMBERS ):
        # add checking against code / flag table here?
        return( value )
    if valid_min is not None:
        if value < valid_min :
            e = ValueError( "{}: Value ({}) < valid min ({}).".format(key, value, valid_min) )
            if nullify_on_fail:
                message = str(e) + " Element set to missing"
                _LOGGER.warning( message )
                return None
            else:
                _LOGGER.error( str(e) )
                raise e
    if valid_max is not None:
        if value > valid_max :
            e = ValueError( "{}: Value ({}) > valid max ({}).".format(key, value, valid_max) )
            if nullify_on_fail:
                message = str(e) + " Element set to missing"
                _LOGGER.warning( message )
                return None
            else:
                _LOGGER.error( str(e) )
                raise e
    return value

def encode(mapping_dict, data_dict):
    """
    This is the primary function that does the conversion to BUFR

    :param mapping_dict: dictionary containing eccodes key and mapping to data dict, includes option to specify
                         valid min and max, scale and offset.
    :param data_dict: dictionary containing data values

    :return: bytesio object containing BUFR message
    """

    # initialise message to be encoded
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # set delayed replication factors if present
    if mapping_dict["inputDelayedDescriptorReplicationFactor"] is not None:
        codes_set_array( bufr_msg, "inputDelayedDescriptorReplicationFactor",
                     mapping_dict["inputDelayedDescriptorReplicationFactor"] )
    # ===================
    # Now encode the data
    # ===================
    for element in mapping_dict["sequence"]:
        key = element["key"]
        value = None
        assert value is None
        if element["value"] is not None:
            value = element["value"]
        elif element["column"] is not None:
            value = data_dict[ element["column"] ]
        else:
            # change the following to debug or leave as warning?
            _LOGGER.debug( "No value for {} but included in mapping file, value set to missing".format(key) )
        # now set
        if value is not None:
            _LOGGER.debug( "setting value {} for element {}.".format( value, key ) )
            if isinstance(value, list):
                try:
                    _LOGGER.debug( "calling codes_set_array" )
                    codes_set_array(bufr_msg, key, value )
                except Exception as e:
                    _LOGGER.error( "error calling codes_set_array({},{},{}). {}".format( bufr_msg, key, value, str(e) ))
                    raise e
            else:
                try:
                    _LOGGER.debug("calling codes_set")
                    nt = codes_get_native_type( bufr_msg, key)
                    # convert to native type, required as in Malawi data 0 encoded as "0" for some elements.
                    if (nt is int) and (not isinstance( value, int )):
                        _LOGGER.warning( "int expected for {} but received {} ({})".format( key, type(value) , value) )
                        if isinstance(value, float):
                            value = int( round( value ) )
                        else:
                            value = int( value )
                        _LOGGER.warning("value converted to int ({})".format( value ) )
                    elif (nt is float) and (not isinstance(value, float) ):
                        _LOGGER.warning("float expected for {} but received {} ({})".format(key, type(value), value))
                        value = float( value )
                        _LOGGER.warning("value converted to float ({})".format(value))
                    else:
                        value = value
                    codes_set(bufr_msg, key, value)
                except Exception as e:
                    _LOGGER.error("error calling codes_set({},{},{}). {}".format(bufr_msg, key, value, str(e)))
                    raise e
    # ==============================
    # Message now ready to be packed
    # ==============================
    try:
        codes_set( bufr_msg, "pack", True )
    except Exception as e:
        _LOGGER.error( 'error calling codes_set({},"pack",True). {}'.format( bufr_msg, str(e) ) )
        raise e
    # =======================================================
    # now write to in memory file and return object to caller
    # =======================================================
    try:
        fh = BytesIO()
        codes_write(bufr_msg, fh)
        codes_release(bufr_msg )
        fh.seek(0)
    except Exception as e:
        _LOGGER.error("error writing to internal BytesIO object, {}".format( str(e) ) )
        raise e
    # =============================================
    # Return BytesIO object containing BUFR message
    # =============================================
    return fh

def transform( data, mappings, station_metadata):
    # validate mappings
    e = validate_mapping_dict( mappings )
    if (e != SUCCESS ):
        raise ValueError( "Invalid mappings" )
    _LOGGER.debug( "mapping dictionary validated" )

    # ToDo: add in code to validate station_metadata

    # we may have multiple rows in the file, create list object to return
    # one item per message
    messages = dict()
    # now convert data to StringIO object
    fh = StringIO( data )
    # now read csv data and iterate over rows
    reader = csv.reader( fh, delimiter=',', quoting = csv.QUOTE_NONNUMERIC)
    rows_read = 0
    for row in reader:
        if rows_read == 0:
            col_names = row
        else:
            data = row
            data_dict = dict(zip(col_names, data))
            try:
                data_dict = {**data_dict, **station_metadata['data']}
            except Exception as e:
                message = "issue merging station and data dictionaries."
                _LOGGER.error( message + str(e) )
                raise e
            # Iterate over items to map, perform unit conversions and validate
            for element in mappings["sequence"]:
                value = element["value"]
                column = element["column"]
                # select between "value" and "column" fields.
                if value is not None:
                    value = element["value"]
                elif column is not None:
                    # get column name
                    # make sure column is in data_dict
                    if (column not in data_dict) :
                        message = "column '{}' not found in data dictionary".format( column )
                        raise ValueError( message )
                    value = data_dict[ column ]
                    if value in MISSING:
                        value = None
                    else:
                        value = apply_scaling( value, element )
                else:
                    _LOGGER.debug("value and column both None for element {}".format( element["key"] ) )
                # now validate value
                _LOGGER.debug( "validating value {} for element {}".format( value, element["key"] ) )
                value = validate_value(element["key"],value,element["valid-min"],element["valid-max"],NULLIFY_INVALID)
                _LOGGER.debug( "value {} validated for element {}".format( value, element["key"] ) )
                # update data dictionary
                if column is not None:
                    data_dict[ column ] = value
                _LOGGER.debug("value {} updated for element {}".format(value, element["key"]))
            # now encode the data (this one line is where the magic happens once the dictionaries have been read in)
            msg = encode(mappings, data_dict)
            key = hashlib.md5( msg.read() ).hexdigest()
            _LOGGER.debug( key )
            msg.seek(0)
            messages[key] = msg
        rows_read += 1
    _LOGGER.info( "{} rows read and converted to BUFR".format( rows_read - 1 ) )

    return messages
