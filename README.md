## csv2bufr

[![Tests](https://github.com/wmo-im/csv2bufr/workflows/tests%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/wmo-im/csv2bufr/actions/workflows/tests.yml)

csv2bufr is a Python package to transform CSV data into WMO BUFR.  Currently, csv2bufr converts each row in a 
CSV to a BUFR message.

### Usage

```bash
# first build the Docker image
docker build -t eccodes_v23 .

# now start the Docker container based on the image
docker run -it -v ${pwd}:/app eccodes_v23

# export path to Python modules for this script
export PYTHONPATH="${PYTHONPATH}:/app/src/csv2bufr/"

# now go to app directory
cd /app

# install, note once released and uploaded to PyPI this will be changed (pip install csv2bufr)
pip install .

# run the converter
csv2bufr data transform \
   ./data/input/Namitambo.SYNOP.csv \
   --mapping malawi_synop_bufr \
   --geojson-template malawi_synop_json \
   --output-dir ./data/output \
   --station-metadata ./metadata/0-454-2-AWSNAMITAMBO.json

# list stored mappings
csv2bufr mappings list
```

### Configuration

With the current version one required and one optional configuration files are used.
The first file (mandatory, specified by --mapping) contains the elements from the expanded BUFR sequence with data (indicated by 
the key used within ecCodes, see e.g. https://confluence.ecmwf.int/display/ECC/WMO%3D36+element+table). The json 
schema for the file is copied below:

```
{
    "$id": "csv2bufr.wis2.0.node.wis",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "inputDelayedDescriptorReplicationFactor": {
            "type": "array",
            "items": {"type": "integer"}
        },
        "unexpandedDescriptors": {
            "type": "array",
            "items": {"type": "integer"}
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

The second file (specified by --geojson-template) contains a template for geoJSON output. The documentation and schema for this will be added.

Additionally a json file containing the OSCAR metadata is required.

- {WSI}.json

Where {WSI} is the WIGOS station ID, e.g. 0-454-2-AWSNAMITAMBO. These are downloaded and installed
as part of the wis2node package.