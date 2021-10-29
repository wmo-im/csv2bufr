## CSV 2 BUFR
Python script to read in CSV file and to convert data to BUFR. Currently, the script converts each line in a 
CSV to a BUFR message.

### Usage
    # first run docker environment
    docker run -it -v ${pwd}:/app eccodes_v23

    # export path to python modules for this script
    export PYTHONPATH="${PYTHONPATH}:/app/src/csv2bufr/"

    # now go to app directory
    cd /app

    # run the converter
    python3 ./main.py  
       --config ./config
       --mapping mapping.json \
       --input ./data/Namitambo_TableHour.csv \
       --output ./OUTPUT/ \
       --wigos-id 0-454-2-AWSNAMITAMBO 

### Configuration

With the current version 3 configuration files are used.

- mapping.json
- station.json

The *mapping.json* file contains the expanded BUFR descriptors and contains the information to map from the BUFR element
to a column in a CSV file.

    description of file to follow ...

The *station.json* contains information specific to a station, e.g. location, height AMSL etc.

### Prerequisites

1) python3+
2) installation of eccodes, including python eccodes package 

### To Do

- Add in command line options specifying number of header rows and which row the column names are on.
- Need better handling of units? 
- Basic quality control?
