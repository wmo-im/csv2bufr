from eccodes import *

def encode( mapping_dict , data_dict, failInvalid = True):
    """
    This is the primary function that does the conversion to BUFR

    :param mapping_dict: dictionary containing eccodes key and mapping to data dict, includes option to specify
                         valid min and max
    :param data_dict: dictionary containing data values
    :param failInvalid: flag to indicate whether to fail on invalid values (default) or print warning message

    :return: handle to eccodes BUFR object

    To Do
        - improve this documentation
        - missing data, NaNs not currently handled very well
        - logging

    """
    # set up message to be encoded
    bufr_msg = codes_bufr_new_from_samples('BUFR4')
    # ===================
    # Now encode the data
    # ===================

    # This section needs to change to reflect changed json structure.

    for element in mapping_dict:

        # validate that we have expected keys in element
        # Need to do this better
        assert "key" in element
        assert "value" in element
        assert "column" in element
        assert "valid-min" in element
        assert "valid-max" in element

        # We are now iterating over array/list, each item
        # is a dict. We need key, column / value, min and max
        key = element["key"]

        if element["value"] is not None:
            value = element["value"]
        else:
            value = data_dict[ element["column"] ]
            if value in ("NAN","NA","NaN"):
                value = None

        # check against minimum value
        if (element["valid-min"] is not None) and (value is not None):
            if value < element["valid-min"]:
                msg = "Element {} value {} outside (<) valid range (value < {} )".format(key, value,
                                                                                         element["valid-min"])
                if failInvalid:
                    raise ValueError(msg)
                else:
                    msg = "Warning:" + msg + ", value set to missing"
                    print(msg)
                    value = None
        # check against maximum value
        if (element["valid-max"] is not None) and (value is not None):
            if value > element["valid-max"]:
                msg = "Element {} value {} outside (>) valid range (value > {} )".format(key, value,
                                                                                         element["valid-max"])
                if failInvalid:
                    raise ValueError(msg)
                else:
                    msg = "Warning:" + msg + ", value set to missing"
                    print(msg)
                    value = None


        if value is not None:
            if isinstance(value, list):
                codes_set_array(bufr_msg, key, value )
            else:
                codes_set(bufr_msg, key, value)

    # =============================
    # Message now read to be packed
    # =============================
    codes_set( bufr_msg, "pack", True )
    # ========================
    # Return handle to message
    # ========================
    return( bufr_msg )