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

Additionally, the command line interface to csv2bufr requires:

* `click <https://pypi.org/project/click/>`_

For convenience and to download station metadata from OSCAR/Surface the pyoscar Python module is recommended.

* `pyoscar <https://pypi.org/project/pyoscar/>`_

All the above packages can be installed by running:

.. code-block:: bash

   pip install -r requirements.txt

Installation
************

Docker
------
The quickest way to install and run the software is via a Docker image containing all the required
libraries and Python modules:

.. code-block:: shell

   docker pull wmoim/csv2bufr

This installs a `Docker image <https://hub.docker.com/r/wmoim/csv2bufr>`_ based on Ubuntu and includes the ecCodes software library, dependencies noted above
and the csv2bufr module (including the command line interface).

Source
------

Alternatively, csv2bufr can be installed from source. First clone the repository and navigate to the cloned folder / directory:

.. code-block:: bash

   git clone https://github.com/wmo-im/csv2bufr.git -b dev
   cd csv2bufr

If running in a Docker environment, build the Docker image and run the container:

.. code-block:: bash

   docker build -t csv2bufr .
   docker run -it -v ${pwd}:/app csv2bufr
   cd /app

The above step can be skipped if not using Docker. Now install the module and test:

.. code-block:: bash

   python3 setup.py install
   csv2bufr --help

The following output should be shown:

.. code-block:: bash

   Usage: csv2bufr [OPTIONS] COMMAND [ARGS]...
   
     csv2bufr
   
   Options:
     --version  Show the version and exit.
     --help     Show this message and exit.
   
   Commands:
     data      data workflows
     mappings  stored mappings
   
