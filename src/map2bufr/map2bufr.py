import logging
from eccodes import *
from jsonschema import validate
import io

SUCCESS = 0
NUMBERS = (float, int, complex)

def validate_mapping_dict( mapping_dict ):
    # first check mapping dict is a dictionary
    if not isinstance( mapping_dict, list ):
        e = TypeError( "mapping dict is not a list")
        return e
    # next check each element matches expected
    schema = {
        "type": "object",
        "properties":{
            "key":{"type": "string" },
            "value": {"type": ["boolean", "object", "array", "number", "string", "null"]},
            "column": {"type": ["string", "null"]},
            "valid-min": { "type": ["number", "null"] },
            "valid-max": {"type": ["number", "null"] }
        }
    }
    for element in mapping_dict:
        try:
            validate( element, schema = schema )
        except Exception as e:
            return e

    return SUCCESS

def validate_value(key, value, valid_min, valid_max, nullify_on_fail = False ):
    if value is None:
        return value
    if not isinstance( value, NUMBERS ):
        return( value )
    if valid_min is not None:
        if value < valid_min :
            e = ValueError( "{}: Value ({}) < valid min ({})".format(key, value, valid_min) )
            if nullify_on_fail:
                # add logging message here
                return None
            else:
                raise e
    if valid_max is not None:
        if value > valid_max :
            e = ValueError( "{}: Value ({}) > valid max ({})".format(key, value, valid_max) )
            if nullify_on_fail:
                # add logging message here
                return None
            else:
                raise e
    return value


def encode( mapping_dict , data_dict, failInvalid = True):
    """
    This is the primary function that does the conversion to BUFR

    :param mapping_dict: List containing eccodes key and mapping to data dict, includes option to specify
                         valid min and max
    :param data_dict: dictionary containing data values
    :param failInvalid: flag to indicate whether to fail on invalid values (default) or print warning message

    :return: handle to eccodes BUFR object

    To Do
        - improve this documentation
        - missing data, NaNs not currently handled very well
        - logging

    """


    # first validate inputs
    if (validate_mapping_dict( mapping_dict ) != SUCCESS ):
        print( "AAAA" ) # add error handling / logging here.

    # set up message to be encoded
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # ===================
    # Now encode the data
    # ===================
    for element in mapping_dict:
        # We are now iterating over array/list, each item
        # is a dict. We need key, column / value, min and max
        key = element["key"]
        # select between "value" and "column" fields.
        if element["value"] is not None:
            value = element["value"]
        else:
            value = data_dict[ element["column"] ]
            if value in ("NAN","NA","NaN"):
                value = None
        # validate value
        value = validate_value(key, value, element["valid-min"], element["valid-max"], failInvalid)
        # now set
        if value is not None:
            if isinstance(value, list):
                codes_set_array(bufr_msg, key, value )
            else:
                codes_set(bufr_msg, key, value)
    # ==============================
    # Message now ready to be packed
    # ==============================
    codes_set( bufr_msg, "pack", True )
    # =======================================================
    # now write to in memory file and return object to caller
    # =======================================================
    fh = io.BytesIO()
    #fh = open(outfile, 'wb')
    codes_write(bufr_msg, fh)
    codes_release(bufr_msg )
    #fh.close()
    fh.seek(0)
    # ========================
    # Return handle to message
    # ========================
    #return( bufr_msg )
    return fh