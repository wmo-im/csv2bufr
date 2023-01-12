.. _mapping:

.. role:: redtext

BUFR template mapping
=====================

The mapping between the input CSV data and the output BUFR data is specified in a JSON file.
The csv2bufr module validates the mapping file against the schema shown at the bottom of this page prior to attempted the transformation to BUFR.
This schema specifies 7 primary properties all of which are mandatory:

- ``inputDelayedDescriptorReplicationFactor`` - array of integers, values for the delayed descriptor replication factors to use
- ``inputShortDelayedDescriptorReplicationFactor`` - array of integers, values for the short delayed descriptor replication factors to use
- ``inputExtendedDelayedDescriptorReplicationFactor`` - array of integers, values for the extended delayed descriptor replication factors to use
- ``skip`` - integer, the number of rows to skip between the header row and the data
- ``wigos_station_identifier`` - either constant WIGOS station identifier (e.g. ``const:0-20000-0-123``) or column from csv data file containing the WSI (e.g. ``data:WSI_column``).
- ``header`` - array of objects (see below), header section containing metadata
- ``data`` - array of object (see below) section mapping from the CSV columns to the BUFR elements

The header and data sections contain arrays of ``bufr_element`` objects mapping to either the different fields
in the header sections of the BUFR message or to the data section respectively. More information is provided below.
In both cases the field ``eccodes_key`` from the ``bufr_element`` object is used to indicate the BUFR element mapped rather than the 6 digit  BUFR FXXYYY code.
The field ``value`` specifies where the data to encode comes from. This can be one of the following:

- data: this specifies that the data should come from the data file.
- const: this specifies that a constant value should be used

For example, the code block below shows how the pressure reduced to mean sea level would be mapped from the column "mslp" in the CSV file
to the BUFR element indicated by the eccodes key "pressureReducedToMeanSeaLevel" (FXXYYY = 010051).

.. code-block:: json

   {
       "data":[
           {"eccodes_key": "pressureReducedToMeanSeaLevel", "value": "data:mslp"}
       ]
   }

The code block below gives examples for both constant values and a value read from the data file:

.. code-block:: json

   {
       "header":[
           {"eccodes_key": "dataCategory", "value": "const:0"}
       ],
       "data":[
           {"eccodes_key": "latitude", "value": "const:46.2234923"},
           {"eccodes_key": "longitude", "value": "const:6.1475485"},
           {"eccodes_key": "pressureReducedToMeanSeaLevel", "value": "data:mslp"},
       ]
   }

In this example, the ``dataCategory`` field in BUFR section 1 (see :ref:`bufr4`) is mapped to the constant value 0;
the ``latitude`` and ``longitude`` to the value specified;
and the ``pressureReducedToMeanSeaLevel`` to the data from the "mslp" column in the CSV file.

The keys used for the header elements are listed on the :ref:`bufr4` page, with the mandatory keys highlighted in red.
The list of keys can also be found at:

- `<https://confluence.ecmwf.int/display/ECC/BUFR+headers>`_

Similarly the keys for the different data elements can be found at:

- `<https://confluence.ecmwf.int/display/ECC/WMO%3D37+element+table>`_

input<Short|Extended>DelayedDescriptorReplicationFactor
---------------------------------------
Due to the way that eccodes works any delayed replication factors need to be specified before encoding and included in the mapping file.
This currently limits the use of the delayed replication factors to static values for a given mapping. For example
every data file that uses a given mapping file has the same optional elements present or the same number of levels in an
atmospheric profile present.

For sequences that do not include delayed replications the :redtext:`inputDelayedDescriptorReplicationFactor` etc
must still be included but may be set to an empty array. e.g.

.. code-block:: json

   {
       "inputDelayedDescriptorReplicationFactor": [],
       "inputShortDelayedDescriptorReplicationFactor": []
       "inputExtendedDelayedDescriptorReplicationFactor": []
   }



bufr_element
------------

Each item in the ``header`` and ``data`` arrays of the mapping template must conform with the definition of the ``bufr_element``
object specified in the schema shown below. This object contains an ``eccodes_key`` field specifying the BUFR element the data
are being mapped to as described above and up to 3 others pieces of information:

- the source of the data (``value``)
- valid range information (``valid_min``, ``valid_max``)
- simple scaling and offset parameters (``scale``, ``offset``)

Only one source can be mapped, if multiple sources are specified the validation of the mapping file by csv2bufr will fail.
As noted at the start of this page. the ``value`` field maps the data to one of:

- data: this specifies that the data should come from the data file
- const: this specifies that a constant value should be used

and takes the form ``"value": "<keyword>:<column|value>"`` where ``<keyword>`` is the string ``data`` or ``const``.
``<column|value>`` can specify either the column name from the data file or it can specify a constant value to use.

The ``valid_min`` and ``valid_max`` are optional and can be used to perform a basic quality control of numeric fields.
The values to use are specified in the same way as for the ``value`` element, with the values coming from either a
constant value or from the data file.
If these fields are specified the csv2bufr module checks the value indicated extracted from the source to the indicated
valid minimum and maximum values. If outside of the range the value is set to missing.

The ``scale`` and ``offset`` fields are conditionally optional, either both can be omitted or both can included.
Including only one will result in a failed validation of the mapping file. These allow simple unit conversions to be performed,
for example from degrees Celsius to Kelvin or from hectopascals to Pascals.
Again, the values are specified in the same way as for the other fields.
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
               "value": "data:mslp",
               "scale": "const:2",
               "offset": "const:0"
           }
       ]
   }

Would convert the value contained in the "mslp" column of the CSV file from hPa to Pa by multiplying by 100 and adding 0.

For each of the above elements (``value, valid_min, valid_max, scale, offset``) null values must be excluded from the mapping file.

An individual BUFR descriptor can occur multiple times within a single BUFR message.
To allow the indexing of the descriptors within a particular message, and the inclusions of multiple descriptors or keys with  the same name, eccodes prepends an index number to the eccodes_key.
For the first occurrence the index number can be omitted but for all other cases it should be included.
The index is indicated within the eccodes_key using ``#index#eccodes_key``, an example is given below.

.. code-block:: json

   {
       "data":[
           {
               "#1#eccodes_key": "pressureReducedToMeanSeaLevel",
               "csv_column": "data:mslp",
               "scale": "const:2",
               "offset": "const:0"
           }
       ]
   }

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
               "value": "data:AT-fahrenheiht",
               "scale": "const:-0.25527",
               "offset": "const:459.67"
           },
           {
               "eccodes_key": "airTemperature",
               "value": "data:AT-celsius",
               "scale": "const:0",
               "offset": "const:273.15"
           },
           {
               "eccodes_key": "pressure",
               "value": "data:pressure-hPa",
               "scale": "const:2",
               "offset": "const:0"
           }
       ]
   }

Schema
------

.. literalinclude:: ../../csv2bufr/resources/mappings/mapping_schema.json
