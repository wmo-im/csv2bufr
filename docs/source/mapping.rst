.. _mapping:

.. role:: redtext

BUFR template mapping
=====================

The mapping between the input csv data and the output BUFR data is specified in a JSON file.
The csv2bufr module validates the mapping file schema shown at the bottom of this page prior to attempted the transformation to BUFR.
This schema specifies 6 primary properties:

- ``inputDelayedDescriptorReplicationFactor`` - array of integers, values for the delayed descriptor replication factors to use
- ``number_header_rows`` - integer, the number of header rows in the file before the data rows
- ``names_on_row`` - integer, which row the column names appear on
- ``header`` - array of objects (see below), header section containing metadata
- ``data`` - array of object (see below) section mapping from the csv columns to the BUFR elements
- ``wigos_identifier`` - object (see below), section to contain the WIGOS station identifier

Out of these, only the ``inputDelayedDescriptorReplicationFactor``, ``header`` and ``data`` are mandatory,
with the ``unexpandedDescriptors`` described on the previous page included in the ``header`` section.
Both the ``number_header_rows`` and ``names_on_row`` default to one if not specified.

The header and data sections contain arrays of ``bufr_element`` objects mapping to either the different fields
in the header sections of the BUFR message or to the data section respectively. More information is provided below.
In both cases the field eccodes_key is used to indicate the BUFR element being mapped to rather than the 6 digit FXXYYY code.
For example, the code block below shows how the pressure reduced to mean sea level would be mapped from the column "mslp" in the csv file
to the BUFR element indicated by the eccodes key "pressureReducedToMeanSeaLevel" (FXXYYY = 010051).

.. code-block:: json

	{
		"data":[
			{"eccodes_key": "pressureReducedToMeanSeaLevel", "csv_column": "mslp"}
		]
	}

In addition to mapping to the csv columns, constant values and values from the JSON metadata file can be mapped using the "value" and "josnpath" fields.
Building on the prior example:

.. code-block:: json

	{
		"header":[
			{"eccodes_key": "dataCategory", "value": 0}
		],
		"data":[
			{"eccodes_key": "latitude", "jsonpath": "$.locations[0].latitude"},
			{"eccodes_key": "longitude", "jsonpath": "$.locations[0].latitude"},
			{"eccodes_key": "pressureReducedToMeanSeaLevel", "csv_column": "mslp"},
		]
	}

Would map: the ``dataCategory`` field in BUFR section 1 (see :ref:`bufr4`) to the constant value 0;
the ``latitude`` and ``longitude`` to the elements specified by resolving the jsonpath in the metadata file;
and the ``pressureReducedToMeanSeaLevel`` to the data from the "mslp" column in the CSV file.

The keys used for the header elements are listed on the :ref:`bufr4` page, with the mandatory keys highlighted in red.
The list of keys can also be at:

- `<https://confluence.ecmwf.int/display/ECC/BUFR+headers>`_

Similarly the keys for the different data elements can be found at:

- `<https://confluence.ecmwf.int/display/ECC/WMO%3D37+element+table>`_

inputDelayedDescriptorReplicationFactor
---------------------------------------
Due to the way that eccodes works any delayed replication factors need to be specified before encoding and included in the mapping file.
This currently limits the use of the delayed replication factors to static values for a given mapping. For example
every data file that uses a given mapping file has the same optional elements present or the same number of levels in an
atmospheric profile present.

For sequences that do not include delayed replications the :redtext:`inputDelayedDescriptorReplicationFactor`
must still be included but may be set to an empty array, e.g.

.. code-block:: json

	{
		"inputDelayedDescriptorReplicationFactor": []
	}

bufr_element
------------

Each item in the ``header`` and ``data`` arrays of the mapping template must conform with the definition of the ``bufr_element``
object specified in the schema shown below. This object contains an ``eccodes_key`` field specifying the BUFR element the data
are being mapped to as described above and up to 3 others pieces of information:

- the source of the data (``value``, ``csv_column``, ``jsonpath``)
- valid range information (``valid_min``, ``valid_max``
- simple scaling and offset parameters (``scale``, ``offset``)

Only one source can be mapped, if multiple sources are specified the validation of the mapping file by csv2bufr will fail.
The ``value`` source maps a constant value to the indicated BUFR element.
The ``csv_column`` source maps the indicated column from the CSV file to the indicated BUFR element.
The ``jsonpath`` source maps from the value found by resolving the JSON path in the metadata file to the indicated BUFR element.

The ``valid_min`` and ``valid_max`` are optional and can be used to perform a basic quality control of numeric fields.
If these fields are specified the csv2bufr module checks the value indicated extracted from the source to the indicated
valid minimum and maximum values. If outside of the range the value is set to missing.

The ``scale`` and ``offset`` fields are conditionally optional, either both can be omitted or both can included.
Including only one will result in a failed validation of the mapping file. These allow simple unit conversions to be performed,
for example from degrees Celsius to Kelvin or from hectopascals to Pascals.
The scaled values are calculated as:

.. math::

	\mbox{scaled\_value} =
		\mbox{value} \times 10^{\mbox{scale}} + \mbox{offset}

The scaled value is then used to set the indicated BUFR element. For example:

.. code-block:: json

	{
		"data":[
			{
				"eccodes_key": "pressureReducedToMeanSeaLevel",
				"csv_column": "mslp",
				"scale": 2,
				"offset": 0
			}
		]
	}

Would convert the value contained in the "mslp" column of the CSV file from hPa to Pa by multiplying by 100 and adding 0.

For each of the above elements (``value, csv_column, jsonpath, valid_min, valid_max, scale, offset``) null values must be excluded from the mapping file.

Units
-----

It should be noted that the units of the data to be encoded into BUFR should match those specified in BUFR table B
(e.g. see `<https://confluence.ecmwf.int/display/ECC/WMO%3D37+element+table>`_), i.e.
Kelvin for temperatures, Pascals for pressure etc. Simple conversions between units are possible as specified above using
the ``scale`` and ``offset`` fields. Some additional examples are given below.

.. code-block:: json

	{
		"data":[
			{
				"eccodes_key": "airTemperature",
				"csv_column": "AT-fahrenheiht",
				"scale": -0.25527,
				"offset": 459.67
			},
			{
				"eccodes_key": "airTemperature",
				"csv_column": "AT-celsius",
				"scale": 0,
				"offset": 273.15
			},
			{
				"eccodes_key": "pressure",
				"csv_column": "pressure-hPa",
				"scale": 2,
				"offset": 0
			}
		]
	}

Schema
------

.. literalinclude:: ../../csv2bufr/resources/mappings/mapping_schema.json