## CSV 2 BUFR
Python script to read in CSV file and to convert data to BUFR. Currently, the script converts each line in a 
CSV to a BUFR message.

### Contents
````bash
.
|-- Dockerfile
|-- README.md
|-- config
|   |-- 0-454-2-AWSNAMITAMBO.json
|   |-- mapping-simple.json
|-- data
|   |-- Namitambo_TableHour.csv
|   |-- Namitambo_preprocessed.csv
|-- output
|-- scripts
|   |-- csv2bufr.py
|   |-- expand-sequence.py
|   `-- preprocess-malawi-data.py
|-- src
|   `-- map2bufr
|       `-- map2bufr.py
````
- The *Dockerfile* file contains the commands to build a docker image containing the required libraries (eccodes) to run.
- The *README.md* file contains this README.
- The *config* directory contains example *json* files for the mapping between csv and BUFR and storing station metadata.
- The *data* directory contains sample csv data files, before and after pre-processings (see notes below).
- The *output* directory is used to write BUFR files to. this can be changes as an argument to the csv2bufr script.
- The *scripts* directory contains 3 scripts
  - *csv2bufr.py*: The main script that converts the csv file to BUFR
  - *expand-sequence.py*: A script under development to expand a BUFR sequence and to create the mapping file.
  - *preprocess-malawi.py*: A script to preprocess the Malawi CSV data and convert to SI units etc (see notes below).
- The *src* directory contains the map2bufr python module used to encode / map to BUFR.

### Usage
````bash
# first build the docker image
docker build -t eccodes_v23 .

# now start the docker a container based on the image
docker run -it -v ${pwd}:/app eccodes_v23

# export path to python modules for this script
export PYTHONPATH="${PYTHONPATH}:/app/src/map2bufr/"

# now go to app directory
cd /app

# run the converter
python3 ./scripts/csv2bufr.py \
   --config ./config \
   --mapping mapping-simple.json \
   --input ./DATA/Namitambo_preprocessed.csv \
   --output ./OUTPUT/ \
   --wigos-id 0-454-2-AWSNAMITAMBO 
````
### Configuration

With the current version 2 configuration files are used.

- {WIS}.json
- mapping-simple.json


Where {WSI} is the WIGOS station ID, e.g. 0-454-2-AWSNAMITAMBO. 
The first file provides information extracted from OSCAR Surface, e.g.:
````json
{
  "metadata": {
    "last-sync": "2021-10-22"
  },
  "data": {
    "wigos-id-series": 0,
    "wigos-id-issuer": 454,
    "wigos-id-issue-number": 2,
    "wigos-id-local": "AWSNAMITAMBO",
    "latitude": -15.84052,
    "longitude": 35.27428,
    "height-asl": 806.0,
    "station-name": "Namitambo",
    "type-of-station": 0,
    "wind-sensor": 0
  }
}
````


The keys in the file are currently arbitrary and some standardisation will be required. Note that these keys also
appear in the mapping file below. The 'last-sync' field gives the date the data were last updated and is used within
the *csv2bufr.py* script to check for stale metadata.

The second file, *mapping-simple.json*, contains the elements from the expanded BUFR sequence with data (indicated by 
the key used within ecCodes, see e.g. https://confluence.ecmwf.int/display/ECC/WMO%3D36+element+table), whether a fixed
value is used or whether the element is mapped to a column in the csv file and optional valid minimum and maximum 
values to check when converting to BUFR. Note that the name of the file is specified on the command line / call to 
*csv2bufr.py*. A truncated example is given below, see *mapping-simple.json* for full example:
````json
[
   {"key":"#1#wigosIdentifierSeries", "value":null, "column":"wigos-id-series", "valid-min":null, "valid-max":null},
   {"key":"#1#wigosIssuerOfIdentifier", "value":null, "column":"wigos-id-issuer", "valid-min":null, "valid-max":null},
   {"key":"#1#wigosIssueNumber", "value":null, "column":"wigos-id-issue-number", "valid-min":null, "valid-max":null},
   {"key":"#1#wigosLocalIdentifierCharacter", "value":null, "column":"wigos-id-local", "valid-min":null, "valid-max":null},
   {"key":"#1#stationOrSiteName", "value":null, "column":"station-name", "valid-min":null, "valid-max":null},
   {"key":"#1#stationType", "value":null, "column":"type-of-station", "valid-min":null, "valid-max":null},
   {"key":"#1#year", "value":null, "column":"year", "valid-min":null, "valid-max":null},
   {"key":"#1#month", "value":null, "column":"month", "valid-min":null, "valid-max":null},
   {"key":"#1#day", "value":null, "column":"day", "valid-min":null, "valid-max":null},
   {"key":"#1#hour", "value":null, "column":"hour", "valid-min":null, "valid-max":null},
   {"key":"#1#minute", "value":null, "column":"valid-min", "valid-min":null, "valid-max":null},
   {"key":"#1#latitude", "value":null, "column":"latitude", "valid-min":null, "valid-max":null},
   {"key":"#1#longitude", "value":null, "column":"longitude", "valid-min":null, "valid-max":null},
   {"key":"#1#heightOfStationGroundAboveMeanSeaLevel", "value":null, "column":"height-asl", "valid-min":null, "valid-max":null},
   {"key":"#1#nonCoordinatePressure", "value":null, "column":"BP_hPa_Avg", "valid-min":null, "valid-max":null},
   {"key":"#1#airTemperature", "value":null, "column":"AirTemp_Avg", "valid-min":250, "valid-max":350},
   {"key":"#1#dewpointTemperature", "value":null, "column":"DewPointTemp_Avg", "valid-min":250, "valid-max":350}
]
````
The following fields are defined:
- *key*: the key used by ecCodes to access / set the BUFR elements.
- *value*: if not null, the value to set the BUFR element to.
- *column*: the column in the CSV file to use if the *value* field is null. These data in the CSV need to be converted to units supported by BUFR, e.g. Kelvin or Pa, not degC of hPa.
- *valid-min*: the minimum valid value allowed for the element. Currently, the script throws an assertion error if a value is specified and the data to be encoded is less than (<) the specified value.
- *valid-max*: the maximum valid value allowed for the element. Currently, the script throws an assertion error if a value is specified and the data to be encoded is greater than (>) the specified value.

### Prerequisites

1) python3+
2) installation of eccodes, including python eccodes package 

Note, these are installed by the Dockerfile.

### Notes 

1) For the case of the Malawi data it has been necessary to preprocess the CSV files to ensure the units are in SI units
for the BUFR encoding and to split the timestamp field into year, month, date, hour, minute. This is performed by the
script *preprocess-malawi-data.py* in the scripts directory.
2) To help setup new data types / BUFR sequences a script is in development to write out a mapping json file given a specified unexpanded BUFR sequence. This is work in progress. 