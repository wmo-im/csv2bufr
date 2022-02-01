## csv2bufr

[![Tests](https://github.com/wmo-im/csv2bufr/workflows/tests%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/wmo-im/csv2bufr/actions/workflows/tests.yml)

csv2bufr is a Python package to transform CSV data into WMO BUFR.  Currently, csv2bufr converts each row in a 
CSV to a BUFR message. Both a python module and CLI to the module are provided.

### Installation
The csv2bufr module relies on the ECCODES software library from ECMWF. 
Prior to installing the module ECCODES needs to be installed. 
As part of the repository this is done via a Docker image, this is currently built as part of the install process but future versions will rely on a pre-built image. 

Clone the repository and enter the csv2bufr directory, working from the dev branch.  

```bash
# clone from github
git clone https://github.com/wmo-im/csv2bufr.git -b dev
cd csv2bufr
# build using docker
docker build -t cvs2bufr .
# run (note the volume is not required but has been included whilst in development)
docker run -it -v ${pwd}:/app csv2bufr
```

The software should now be installed and ready to run.

### Configuration

Two configurations files are required, the first specifying how to map from the input CSV file to BUFR.
The second contains the station level metadata from the WMO OSCAR/Surface catalogue. 
Both files take the JSON format and a description of the mapping file is given below under usage.
The second can be downloaded by the pyoscar tool (installed as part of the image build).
To download the metadata for e.g. the weather station on Bird Island, South Georgia, the following would be used:

```
pyoscar station --identifier 0-20000-0-88900 > 0-20000-0-88900.json
```

noting the redirect of the data to the json file.

### Usage (CLI)

As part of the module a command line interface is included, this takes a number of arguments:

- The name of the csv file to process.
- The template used to map from the CSV to BUFR.
- The output directory to write the BUFR data to.
- The name / path of the file containing the metadata.

An example using the CLI is show below:
```bash
# download the metadata (if not already done)
pyoscar station --identifier 0-454-2-AWSNAMITAMBO.json > ./metadata/0-454-2-AWSNAMITAMBO.json

# run the converter
csv2bufr data transform \
   ./data/input/Namitambo_SYNOP.csv \
   --bufr-template malawi_synop_bufr \
   --output-dir ./data/output \
   --station-metadata ./metadata/0-454-2-AWSNAMITAMBO.json
```
Any number of header rows can be included in the CSV file but the number needs to be specified in the template file using the *number_header_rows* field. 
The row number for the column names also needs to be specified using the *names_on_row* field. All other header rows are ignored.

Pre-configured templates mapping to BUFR will be included with the module, those available can be listed using:
```bash
# list stored mappings
csv2bufr mappings list
```
Alternatively, a new BUFR template can be providing by specifying the file name, e.g.:
```bash
# run the converter
csv2bufr data transform \
   ./data/input/Namitambo_SYNOP.csv \
   --bufr-template ./csv2bufr/resources/mappings/malawi_synop_bufr.json \  
   --output-dir ./data/output \
   --station-metadata ./metadata/0-454-2-AWSNAMITAMBO.json
```

Note the path and extension on the ``--bufr-template`` option, the template can be stored in any user defined path. 
The mappings are defined using json with the schema specified in the file *./csv2bufr/resources/mapping_schema.json*. 
For convenience this is copied below:    

```json
{
    "$id": "csv2bufr.wis2.0.node.wis",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "inputDelayedDescriptorReplicationFactor": {
            "type": "array",
            "items": {"type": "integer"}
        },
        "number_header_rows": {
            "type": "integer",
            "description": "Number of header rows in the file"
        },
        "names_on_row": {
            "type": "integer",
            "description": "Which row the column names appear on"
        },
        "header":{
            "type": "array",
            "items": {"$ref": "#/$defs/bufr_element"},
            "description": "Contents of header sections of BUFR message"
        },
        "data": {
            "type": "array",
            "items": {"$ref": "#/$defs/bufr_element"},
            "description": "mapping from CSV file (or metadata json file) to BUFR"
        },
        "wigos_identifier": {
            "type": "object",
            "description": "Field to contain WIGOS identifier (currently unused)",
            "properties": {
                "csv_column": {"type": "string"},
                "jsonpath": {"type": "string"},
                "value": {"type": "string"}
            },
            "oneOf": [
                        {"required": ["value"]},
                        {"required": ["csv_column"]},
                        {"required": ["jsonpath"]}
                    ]
        }
    },
    "required" : ["inputDelayedDescriptorReplicationFactor","header","data"],
    "$defs":{
        "bufr_element": {
            "type": "object",
            "properties": {
                "eccodes_key": {
                    "type": "string",
                    "descripition": "eccodes key used to set the value in the BUFR data"
                },
                "value": {
                    "type": [
                        "boolean", "object", "array", "number", "string", "integer"
                    ],
                    "description": "fixed value to use for all data using this mapping"
                },
                "csv_column": {
                    "type": "string",
                    "description": "column from the CSV file to map to the BUFR element indicated by eccodes_key"
                },
                "jsonpath": {
                    "type": "string",
                    "description": "json path to the element in the JSON metadata file"
                },
                "valid_min": {
                    "type": "number",
                    "description": "Minimum valid value for parameter if set"
                },
                "valid_max": {
                    "type": "number",
                    "description": "Maximum value for for the parameter if set"
                },
                "scale": {
                    "type": "number",
                    "description": "Value used to scale the data by before encoding using the same conventions as in BUFR"
                },
                "offset": {
                    "type": "number",
                    "description": "Value added to the data before encoding to BUFR following the same conventions as BUFR"
                }
            },
            "required": ["eccodes_key"],
            "allOf": [
                {
                    "oneOf": [
                        {"required": ["value"]},
                        {"required": ["csv_column"]},
                        {"required": ["jsonpath"]}
                    ]
                },
                {
                    "dependentRequired": {"scale": ["offset"]}
                },
                {
                    "dependentRequired": {"offset": ["scale"]}
                }
            ]
        }
    }
}
```

A command is also included in the CLI to generate a blank template, 
in the example below a new mapping template containing
the sequences 301150 (WIGOS Identifier) and 307091 (BUFR template for surface observations from 
one-hour period with national and WMO station identification) is generated:
```bash
# create a new mapping
csv2bufr mappings create 301150 307091 > new-mapping.json
```
Currently, this is output to stdout and redirected to the file *new-mapping.json* in the example above. 
Elements that are null or missing can be deleted, other elements can be mapped to either the CSV file by specifying the column names (``csv_column``) or to a fixed value (``value``).
The ``jsonpath`` option maps to the metadata file using jsonpath syntax to access elements in the json file..
To help quality control the data before conversion to BUFR valid minimum and maximum values can be specified using the ``valid_min`` and ``valid_max`` fields.
All data should be in S.I. units or the units expected by BUFR but a simple scaling and offset is supported.

### Usage (module)
The csv2bufr module contains a function ``transform`` to convert all rows in a file to BUFR. A example of its usage is shown below.
```python
# get iterator to iterate over lines in input
result = csv2bufr.transform(csv_file.read(), metadata, mappings, template)
# now iterate
for item in result:
    key = item["md5"]
    bufr_filename = f"{output_dir}{os.sep}{key}.bufr4"
    with open(bufr_filename, "wb") as fh:
        fh.write(item["bufr4"])
```
The ``transform`` functions takes 4 arguments.

````python
def transform(data: str, metadata: dict, mappings: dict,
              template: dict = {}) -> Iterator[dict]:
    """
    Function to drive conversion to BUFR and if specified to geojson

    :param data: string containing csv separated data. First n lines should
                contain the column headers, the followings lines the data.
    :param metadata: dictionary containing the metadata for the station
                    from OSCAR surface downloaded by wis2node.
    :param mappings: dictionary containing list of BUFR elements to
                    encode (specified using ECCODES key) and whether
                    to get the value from (fixed, csv or metadata).
    :param template: dictionary containing mapping from BUFR to geoJSON.

    :returns: `dict` of output messages
    """
````

The returned dictionary (``item`` from the above example) contains the following elements:
- ``item["md5"]`` the md5 checksum of the encoded BUFR data
- ``item["bufr4"]`` binary BUFR data
- ``item["geojson"]`` geojson string representation
- ``item["_meta"]`` dictionary containing elements used by wis2node.
- ``item["_meta"]["identifier"]`` unique identifier for result
- ``item["_meta"]["data_date"]`` characteristic date of data contained in result (from BUFR)
- ``item["_meta"]["originating_centre"]`` originating centre for data  (from BUFR)
- ``item["_meta"]["data_category"]`` data category (from BUFR)

### Examples

Generate template for basic BUFR sequence containing

- Station name (001015)
- Observation Date & Time (004001 004002 004003 004004 004005)
- Latitude (high accuracy) (005001)
- Longitude (high accuracy) (006001)
- Wind Direction (011001)
- Wind Gust (011041)
- Wind Speed (011002)
- Water Temperature (022043)
- Water Level (Tidal elevation with respect to local chart datum) (022035)

````bash
csv2bufr mappings create 001015 004001 004002 004003 004004 004005 \
    005001 006001 011001 011041 011002 022043 022038
````