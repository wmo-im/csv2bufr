#!/usr/bin/python3
# create template by expanding BUFR descriptors, single replication for delayed descriptors
# convert template to mapping file by setting value and map elements, also set replications etc
# run
"""
./main.py \
   --mapping mapping-simple.json \
   --input ./data/Namitambo_TableHour.csv \
   --output ./output/ \
   --wigos-id 0-454-2-AWSNAMITAMBO
"""
import json
from eccodes import *
import sys
import argparse
import csv
from datetime import datetime
from csv2bufr import *

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

    # ========================
    # Load station details
    # ========================
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
            rows_read += 1
            # second row contains column names
            if rows_read == 2:
                col_names = row
            # rows 3 and 4 additional information but no data (e.g. units)
            elif rows_read > 4: #
                data = row
                # create dictionary from data and header row / column names
                data_dict = dict( zip( col_names, data) )
                # now add / update values in dict with data not in CSV, e.g. station name or WIGOS ID
                # values specified in station file
                data_dict = { **data_dict, **station['data'] }
                # ================================================================
                # This next section will be moved to pre-processing function
                # preprocessing to convert hPa to Pa and deg C to K
                # ================================================================
                # any parameters that need to be calculate from the data file 'could' also be calculated here
                # but it would be better for them to be included in the file. E.g. pressure reduced to MSL.
                # similarly, we can extract information from other fields such as timestamps, e.g.
                data_dict['year']  = int(data_dict['TIMESTAMP'][0:4])
                data_dict['month'] = int(data_dict['TIMESTAMP'][5:7])
                data_dict['day']   = int(data_dict['TIMESTAMP'][8:10])
                data_dict['hour']  = int(data_dict['TIMESTAMP'][11:13])
                data_dict['min']   = int(data_dict['TIMESTAMP'][14:16])
                # unit conversions
                if data_dict['AirTemp_Avg'] is not None:
                    data_dict['AirTemp_Avg'] = data_dict['AirTemp_Avg'] + 273.15
                if data_dict['BP_hPa_Avg'] is not None:
                    data_dict['BP_hPa_Avg'] = data_dict['BP_hPa_Avg']*100
                # ================================================================
                # end of code to be moved pre-processing function. this will be data / file specific
                # ================================================================
                # now encode the data
                msg = encode( mapping, data_dict)
                # for testing purposes set file name to write data to
                # we will wnat to write to virtual in file in future to pass blob of data to calling function
                outfile = args.output + "/" + (args.wsi.lower()) + "_{:04d}-{:02d}-{:02d}_{:02d}{:02d}.bufr".format(
                    data_dict['year'],data_dict['month'],data_dict['day'],data_dict['hour'],data_dict['min'] )
                # save data to file
                fh = open( outfile, 'wb' )
                codes_write( msg, fh )
                codes_release( msg )
                fh.close()
            # only do first few rows for demo purposes
            if rows_read > 10:
                break
    return 0

if __name__ == "__main__":
    main( sys.argv[1:] )
