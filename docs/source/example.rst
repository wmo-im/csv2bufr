.. _example:

.. role:: redtext

Examples
========

Creating a new mapping file
---------------------------

:redtext:`NOTE: THIS PAGE IS STILL UNDER DEVELOPMENT`

A command line tool to create an empty BUFR mapping template has been included as part of the csv2bufr module.
This can be invoked using the ``csv2bufr mappings create <BUFR descriptors>`` command. E.g.:

.. code-block::

	csv2bufr mappings create 301150 302001 > bufr-mappings.json


:redtext:`we need to add location+date to the above`

generates the following file:

.. literalinclude:: resources/bufr-mappings.json

Editing to map to the example data file below we then have:

.. literalinclude:: resources/bufr-mappings-edited.json

Data file
---------

:redtext:`simple two line csv containing the parameters required above`

Metadata file
-------------

:redtext:`simple example json metadata file containing wigosID`

Transformation
--------------

:redtext:`example command and output file`