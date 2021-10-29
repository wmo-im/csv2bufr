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
    for element in mapping_dict:
        if isinstance(mapping_dict[element], list):
            codes_set_array(msg, element, mapping_dict[element]  )
        else:
            if isinstance( mapping_dict[element], str ):
                value = data_dict[ mapping_dict[element] ]
            else:
                value = mapping_dict[element]
            if value is not None:
                codes_set(msg, element, value )
    # =============================
    # Message now read to be packed
    # =============================
    codes_set( msg, "pack", True )
    # ========================
    # Return handle to message
    # ========================
    return( msg )