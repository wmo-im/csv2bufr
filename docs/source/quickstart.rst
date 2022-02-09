.. _quickstart:

Quick start
===========

The csv2bufr Python module contains both a command line tool  and an API to convert data
stored in a CSV file to the WMO BUFR data format.

Command line interface
**********************

The command line interface reads in data from a CSV file, converts it to BUFR and writes out the data to the specified directory.
The mapping from CSV to BUFR is specified in a BUFR template mapping file (see BUFR template mapping page).
Instrumental / station metadata can also be included via a json file, with the mapping specified in the same mapping file.
For example, the following command transforms the data in file ``my-csv-file.csv`` to BUFR using template ``csv-to-bufr-mapping.json``
and writes the output to directory ``output-directory-path``:

.. code-block:: shell

	csv2bufr data transform <my-csv-file.csv> \
			--bufr-template <csv-to-bufr-mapping.json> \
			--station-metadata <oscar-metadata-file.json> \
			--output <output-directory-path>

Currently the command line interface only supports one station per csv file and a json file containing the metadata for that station needs to be specified.
This is done via the ``--station-metadata`` option and at a minimum this file needs to contain the
`WIGOS identifier <https://community.wmo.int/wigos-station-identifier>`_ for the station:

.. code-block:: json

	{
		"wigosIds": [
				{
					"wid": "<series>-<issuer>-<issue-number>-<local-identifier>"
				}
			]
	}

Where the parameters in brackets (<>) are replaced with their respective values.

The output is written to the directory specified by the ``--output`` option with the extension .bufr4.
The filename if set using the md5 checksum of the BUFR data to ensure uniqueness, future versions
will use the WIGOS ID and timestamp of the data to set the filename.
These files can be validated using a tool such as the `ECMWF BUFR validator <https://apps.ecmwf.int/codes/bufr/validator/>`_.

API
***

In addition to the command line interface the conversion can be specified