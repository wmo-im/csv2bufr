Overview
========

The csv2bufr Python module contains both a command line interface and an API to convert data stored in a CSV file to the WMO BUFR data format.
More information on the BUFR format can be found in the [WMO Manual on Codes, Volume I.2](https://library.wmo.int/doc_num.php?explnum_id=10722).

Install
-------

    docker pull wmoim/csv2bufr

Example usage
-------------

Transform data from file ``<my-csv-file.csv>`` to BUFR using template specified in file ``<csv-to-bufr-mapping.json>``
and with station metadata file the file ``<oscar-metadata-file.json>``. Write output to ``<output-directory-path>``.

	csv2bufr data transform <my-csv-file.csv> \
			--bufr-template <csv-to-bufr-mapping.json> \
			--station-metadata <oscar-metadata-file.json> \
			--output <output-directory-path>

Manual / documentation
----------------------

The full documentation for csv2bufr can be found at: [wmo-im.github.io/csv2bufr](https://wmo-im.github.io/csv2bufr/) including sample files.


