.. _quickstart:

Quick start
===========

The csv2bufr Python module contains both a command line interface and an API to convert data
stored in a CSV file to the WMO BUFR data format.

Command line interface
**********************

The command line interface reads in data from a CSV file, converts it to BUFR and writes out the data to the specified directory.
Currently, a single station per file is supported with each row treated as a separate record and one BUFR file per record created.
The format of the input CSV file has a few requirements:

- A comma "," must be used as the delimiter.
- Strings must be quoted.
- Missing values must be encoded as "None".
- The final row in the file must contain data and not be a new line.
- The timestamp of the records must be separated into components, i.e. year, month, day etc must each be in a separate column.
- The date/time elements should be in Universal Coordinated Time (UTC).

The mapping from CSV to BUFR is specified in a BUFR template file (see the :ref:`BUFR template mapping page <mapping>`).
In addition to the input CSV data file and BUFR template file a json file containing the station metadata is also required.
At a minimum this file must contain the `WIGOS station identifier <https://community.wmo.int/wigos-station-identifier>`_ as per the example below:

.. code-block:: json

	{
		"wigosIds": [
				{
					"wid": "<series>-<issuer>-<issue-number>-<local-identifier>"
				}
			]
	}

Where the parameters in brackets (<>) are replaced with their respective values.

If the station has been registered within the WMO OSCAR/Surface database the metadata
file can be downloaded using the `pyoscar <https://pypi.org/project/pyoscar/>`_ Python module.
For example, to download station metadata for the station on Bird Island, South Georgia,
with the WIGOS station identifier "0-20008-0-SGI" the following would be used:

.. code-block:: bash

	pyoscar station --identifier 0-20008-0-SGI > 0-20008-0-SGI.json

This writes the output to the file 0-20008-0-SGI.json as specified by the redirect (>).

The following example transforms the data in file ``my-csv-file.csv`` to BUFR using template ``csv-to-bufr-mapping.json``
and writes the output to directory ``output-directory-path``:

.. code-block:: shell

	csv2bufr data transform <my-csv-file.csv> \
			--bufr-template <csv-to-bufr-mapping.json> \
			--station-metadata <oscar-metadata-file.json> \
			--output <output-directory-path>

The output is written to the directory specified by the ``--output`` option with the extension .bufr4.
The output filename if set using the md5 checksum of the BUFR data to ensure uniqueness, future versions
will use the WIGOS ID and timestamp of the data to set the filename.
The output BUFR files can be validated using a tool such as the `ECMWF BUFR validator <https://apps.ecmwf.int/codes/bufr/validator/>`_.

API
***

The command line interface uses the ``transform`` function from the csv2bufr module. This can be used directly, e.g.:

.. code-block:: python

	# import modules
	import json
	from csv2bufr import transform

	# load data from file
	with open("my-csv-file.csv") as fh:
		data = fh.read()

	# load mapping
	with open("csv-to-bufr-mapping.json") as fh:
		mapping = json.load(fh)

	# load metadata
	with open("oscar-metadata-file.json") as fh:
		metadata = json.load(fh)

	# call transform function
	result = transform(data, metadata, mapping)

	# iterate over items
	for item in result:
		# get id and phenomenon time to use in output filename
		wsid = item["_meta"]["wigos_id"]  # WIGOS station ID
		timestamp = item["_meta"]["data_date"]  # phenomenonTime as datetime object
		timestamp = timestamp.strftime("%Y%m%dT%H%MZ")  # convert to string
		# set filename
		output_file = f"{wsid}_{timestamp}.bufr4"
		# save to file
		with open(output_file, "wb") as fh:  # note binary write mode
			fh.write(item["bufr4"])

The ``transform`` function returns an iterator that can be used to iterate over each line in the data file.
Each item returned contains a dictionary with the following elements:

- ``item["bufr4"]`` binary BUFR data
- ``item["_meta"]`` dictionary containing metadata elements
- ``item["_meta"]["md5"]`` the md5 checksum of the encoded BUFR data
- ``item["_meta"]["identifier"]`` unique identifier for result (set to md5 checksum)
- ``item["_meta"]["wigos_id"]`` WIGOS station identifier
- ``item["_meta"]["data_date"]`` characteristic date of data contained in result (from BUFR)
- ``item["_meta"]["originating_centre"]`` originating centre for data  (from BUFR)
- ``item["_meta"]["data_category"]`` data category (from BUFR)
