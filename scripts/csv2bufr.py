#!/usr/bin/python3
import json
from eccodes import *
import sys
import argparse
import csv
from datetime import datetime
from map2bufr import *

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
    args = parser.parse_args()

    # ==========================================
    # Load station details for specified station
    # ==========================================
    with open( "{}/{}.json".format(args.config, args.wsi) ) as fh:
        station = json.load( fh )
    # Check age of data in file
    last_sync = datetime. strptime( station['metadata']['last-sync'], '%Y-%m-%d')
    time_delta = (datetime.now() - last_sync)
    # now do something if stale, this is just for illustration
    if time_delta.days > 7:
        print( "Stale metadata")
    # It may be better to force a sync or update by connecting to OSCAR

    # ========================
    # read in mapping template
    # ========================
    with open( "{}/{}".format(args.config, args.mapping) ) as fh:
        mapping = json.load( fh )

    # ========================
    # now load csv and process
    # ========================
    with open( args.input ) as input_data:
        reader = csv.reader( input_data , delimiter = ',', quoting = csv.QUOTE_NONNUMERIC)
        rows_read = 0
        for row in reader:
            # first row contains column names
            if rows_read == 0:
                col_names = row
            # additional rows contain data
            elif rows_read > 0: #
                data = row
                # create dictionary from data and header row / column names
                data_dict = dict( zip( col_names, data) )
                # now add / update values in dict with data not in CSV, e.g. station name or WIGOS ID
                # values specified in station file
                data_dict = { **data_dict, **station['data'] }
                # now encode the data
                msg = encode( mapping, data_dict)

                # for testing purposes set file name to write data to
                # we will wnat to write to virtual in file in future to pass blob of data to calling function
                outfile = args.output + "/" + (args.wsi.lower()) + "_{:04.0f}-{:02.0f}-{:02.0f}_{:02.0f}{:02.0f}.bufr4".format(
                    data_dict['year'],data_dict['month'],data_dict['day'],data_dict['hour'],data_dict['min'] )
                # save data to file
                fh = open( outfile, 'wb' )
                codes_write( msg, fh )
                codes_release( msg )
                fh.close()
            # only do first few rows for demo purposes
            #if rows_read > 10:
            #    break
            rows_read += 1
    return 0

if __name__ == "__main__":
    main( sys.argv[1:] )
