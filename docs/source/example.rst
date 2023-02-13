.. _example:

.. role:: redtext

Examples
========

This page follows through a worked example:

#. Example data file
#. Creating a new mapping file
#. Editing the mapping file
#. Running the transformation

All the example files used are downloadable at the end of this page.

Data file (example-data.csv)
----------------------------

.. csv-table:: example-data.csv
   :file: resources/example-data.csv
   :header-rows: 1

Creating a new mapping file
---------------------------

A command line tool to create an empty BUFR mapping template has been included as part of the csv2bufr module.
This can be invoked using the ``csv2bufr mappings create <BUFR descriptors>`` command. E.g.:

.. code-block:: bash

   csv2bufr mappings create 301150 301011 301012 301021 007031 302001 --output bufr-mappings.json

generates the following file:

.. literalinclude:: resources/bufr-mappings.json

This file includes the representable range (``valid_min`` and ``valid_max``) for the different BUFR elements.
These should be set to the physical range where applicable.

Customising the mapping file (bufr-mappings-edited.json)
--------------------------------------------------------

Editing the bufr mappings file to map to the above example CSV data we have:

.. literalinclude:: resources/bufr-mappings-edited.json

Note that the sequence includes no delayed replications and so the ``inputDelayedDescriptorReplicationFactor`` etc can be left as empty arrays.
Elements that would be set to null have been removed.

Transformation
--------------

.. code-block:: bash

   csv2bufr data transform \
                    ./example-data.csv \
                    --bufr-template ./bufr-mappings-edited.json \
                    --output-dir ./

The links below can be used to download the example files:

- :download:`example-data.csv <resources/example-data.csv>`
- :download:`bufr-mappings-edited.json <resources/bufr-mappings-edited.json>`
- :download:`example output (d0464c97a88ea99f119e87629844c5dd.bufr4) <resources/d0464c97a88ea99f119e87629844c5dd.bufr4>`
