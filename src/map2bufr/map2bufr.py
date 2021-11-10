from eccodes import *

def encode( mapping_dict , data_dict):
    """
    This is the primary function that does the conversion to BUFR

    :param configuration_dict: dictionary containing section0 - section3 fields
    :param mapping_dict: dictionary containing eccodes key and mapping to data dict, including basic transformations
                        such as scaling and offsets (e.g. for converting from hPa to Pa and from degC to K
    :param data_dict: dictionary containing data values
    :return: handle to eccodes BUFR object

    To Do
        - Improve this documentation
        - missing data, NaNs not currently handled

    """
    # set up message to be encoded
    msg = codes_bufr_new_from_samples('BUFR4')
    # ===================
    # Now encode the data
    # ===================

    # This section needs to change to reflect changed json structure.

    for element in mapping_dict:
        #
        # We are now iterating over array/list, each item
        # is a dict. We need key, column / value, min and max
        key = element["key"]
        if "value" in element:
            value = element["value"]
        else:
            value = data_dict[ element["column"] ]
            # add QC code here, e.g.
            if element["min"] is not None:
                assert value >= element["min"]
            if element["max"] is not None:
                assert value <= element["max"]

        if value is not None:
            if isinstance(value, list):
                codes_set_array(msg, key, value  )
            else:
                codes_set(msg, key, value)

    # =============================
    # Message now read to be packed
    # =============================
    codes_set( msg, "pack", True )
    # ========================
    # Return handle to message
    # ========================
    return( msg )