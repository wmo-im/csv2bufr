#!/usr/bin/python3

import csv
import sys
import argparse

def main( argv ):
    # get input and output file names
    parser = argparse.ArgumentParser(description='Preprocess Malawi CSV data')
    parser.add_argument("--input",  dest="input",  required=True, help="CSV file containing data to preprocess")
    parser.add_argument("--output", dest="output", required=True, help="Name of file to write CSV data to")
    args = parser.parse_args()
    # load csv file, pre-process data and write to new file
    with open( args.output , 'w', newline='' ) as output_file:
        writer = csv.writer( output_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
        rows_written = 0
        with open( args.input, 'r') as input_file:
            reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            rows_read = 0
            # 4 header rows in input file
            # 0) "TOA5", "Namitambo", "CR300", "4720", "CR310-CELL200.Std.08.01", "CPU:CR310_Malawi_V1R5_14062021_T3.CR300", "12648", "TableHour"
            # 1) "TIMESTAMP", "RECORD", "DataloggerSerialNumber", "LoggerBatt_Min", "LoggerTemp_Max", "WSpeed_Max", "WSpeed_S_WVT", "WindDir_D1_WVT", "AirTemp_Avg", "DewPointTemp_Avg", "RH", "AirTemp_Max", "AirTemp_Min", "DewPointTemp_Max", "DewPointTemp_Min", "RH_Max", "RH_Min", "Rain_mm_Tot", "CMP6_SlrW_Avg", "CMP6_SlrMJ_Tot", "CMP6_SlrW_Max", "BP_hPa_Avg", "BP_hPa_Max", "BP_hPa_Min", "ETos", "Rso"
            # 2) "TS", "RN", "", "Volts", "DegC", "meters/second", "meters/second", "Deg", "Deg C", "Deg C", "%", "Deg C", "Deg C", "Deg C", "Deg C", "%", "%", "mm", "W/m^2", "MJ/m^2", "W/m^2", "hPa", "hPa", "hPa", "mm", "MJ/m²"
            # 3) "", "", "Smp", "Min", "Max", "Max", "WVc", "WVc", "Avg", "Avg", "Smp", "Max", "Min", "Max", "Min", "Max", "Min", "Tot", "Avg", "Tot", "Max", "Avg", "Max", "Min", "ETXs", "Rso"
            for row in reader:
                if rows_read == 0:
                    a = 1
                elif rows_read == 1:
                    col_names = row
                elif rows_read == 2:
                    a = 1
                elif rows_read == 3:
                    a = 1
                else:
                    data = row
                    data_dict = dict( zip( col_names, data) )
                    data_dict['year'] = int(data_dict['TIMESTAMP'][0:4])
                    data_dict['month'] = int(data_dict['TIMESTAMP'][5:7])
                    data_dict['day'] = int(data_dict['TIMESTAMP'][8:10])
                    data_dict['hour'] = int(data_dict['TIMESTAMP'][11:13])
                    data_dict['min'] = int(data_dict['TIMESTAMP'][14:16])
                    # unit conversions
                    if data_dict['AirTemp_Avg'] is not None:
                        data_dict['AirTemp_Avg'] = data_dict['AirTemp_Avg'] + 273.15
                    if data_dict['DewPointTemp_Avg'] is not None:
                        data_dict['DewPointTemp_Avg'] = data_dict['DewPointTemp_Avg'] + 273.15
                    if data_dict['BP_hPa_Avg'] is not None:
                        data_dict['BP_hPa_Avg'] = data_dict['BP_hPa_Avg'] * 100
                    if rows_written == 0:
                        writer.writerow( list( data_dict.keys() ) )
                    writer.writerow( data_dict.values() )
                    rows_written += 1
                rows_read += 1

    # bufr compare disable keys



    return 0

if __name__ == "__main__" :
    main( sys.argv[1:] )