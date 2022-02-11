.. _installation:

Installation
============
Dependencies
************

The csv2bufr module relies on the `ecCodes <https://confluence.ecmwf.int/display/ECC>`_ software library to perform
the BUFR encoding. This needs to be installed prior to installing any of the Python packages, instructions can
be found on the ecCodes documentation pages: `https://confluence.ecmwf.int/display/ECC <https://confluence.ecmwf.int/display/ECC>`_.

The following Python packages are required by the csv2bufr module:

* `eccodes <https://pypi.org/project/eccodes/>`__ (NOTE: this is separate from the ecCodes library)
* `jsonschema <https://pypi.org/project/jsonschema/>`_
* `jsonpath_ng <https://pypi.org/project/jsonpath-ng/>`_

Additionally, the command line interface to csv2bufr requires:

* `click <https://pypi.org/project/click/>`_

For convenience and to download station metadata from OSCAR/Surface the pyoscar Python module is recommended.

* `pyoscar <https://pypi.org/project/pyoscar/>`_

All the above packages can be installed by running:

.. code-block:: bash

	pip install -r requirements.txt

Installation
************

The quickest way to install and run the software is via a Docker image containing all the required
libraries and Python modules:

.. code-block:: shell

	docker pull wmoim/csv2bufr

This installs a `Docker image <https://hub.docker.com/r/wmoim/csv2bufr>`_ based on Ubuntu and includes the ecCodes software library, dependencies noted above
and the csv2bufr module (including the command line interface).

Alternatively, csv2bufr can be installed from source:

.. code-block:: bash

	git clone https://github.com/wmo-im/csv2bufr.git
	cd csv2bufr
	pip install .