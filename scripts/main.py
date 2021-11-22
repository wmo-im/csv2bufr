#!/usr/bin/python3
import json
import sys
import argparse
from csv2bufr import transform
import logging

_log_level = "DEBUG"

formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s","%Y-%m-%d %H:%M:%S")
ch = logging.StreamHandler()
ch.setFormatter( formatter )
ch.setLevel( _log_level )

LOGGER = logging.getLogger( __name__ )
LOGGER.setLevel( _log_level )
LOGGER.addHandler( ch )

def main( argv ):
    # =============
    # get arguments
    # =============
    parser = argparse.ArgumentParser(description='CSV 2 BUFR')
    parser.add_argument("--config", dest="config", required=False, help="Directory containing configuration JSON files", default= "./config")
    parser.add_argument("--mapping", dest="mapping", required=True, help="JSON file mapping from CSV to BUFR")
    parser.add_argument("--input", dest="input", required=True, help="CSV file containing data to encode")
    parser.add_argument("--output", dest="output", required=True, help="Name of output file")
    parser.add_argument("--wigos-id", dest="wsi", required=True, help="WIGOS station identifier, hyphen separated. e.g. 0-20000-0-ABCDEF")
    parser.add_argument("--fail-on-invalid", dest="invalid", default=True,
                        help="Flag indicating whether to fail on invalid values. If true invalid values are set to missing")
    args = parser.parse_args()
    # now set paths from arguments
    csv_file = args.input
    station_metadata_file = "{}/{}.json".format(args.config, args.wsi)
    mappings_file = "{}/{}".format(args.config, args.mapping)
    result = None
    # ===========================
    # now the code to be executed
    # ===========================
    with open( csv_file ) as fh1, open( mappings_file ) as fh2, open(station_metadata_file) as fh3:
        try:
            result = transform( fh1.read(), mappings = json.load( fh2 ), station_metadata = json.load( fh3 ) )
        except Exception as err:
            LOGGER.error(err)
    # ======================
    # now write data to file
    # ======================
    #fh = open( "test_data.bufr", "wb")
    for item in result:
        fh = open( args.output + item + ".bufr4", "wb")
        fh.write( result[item].read() )
        fh.close()
    return 0

if __name__ == "__main__":
    main( sys.argv[1:] )
