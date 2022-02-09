.. _mapping:

BUFR template mapping
=====================

Schema
------
The mapping between the input csv data and the output BUFR data is specified in a JSON file.
The schema for this JSON file is shown below:

.. literalinclude:: ../../csv2bufr/resources/mappings/mapping_schema.json

Properties
----------

The JSON schema contains 6 primary properties:

- ``inputDelayedDescriptorReplicationFactor`` - values for the delayed descriptor replication factor to use
- ``number_header_rows`` - the number of header rows in the file before teh data rows
- ``names_on_row`` - which row the column names appear on
- ``header`` - header section containing metadata
- ``data`` - section mapping from the csv columns to the BUFR elements
- ``wigos_identifier`` - section to contain the WIGOS station identifier

Out of these, only the ``inputDelayedDescriptorReplicationFactor``, ``header`` and ``data`` are mandatory.
The ``number_header_rows`` and ``names_on_row`` default to one if not specified.

bufr_element
************

header
******

data
****

- json schema
- mapping, csv_column vs jsonpath
- valid_min and valid_max
- scale and offset
- creating new templates