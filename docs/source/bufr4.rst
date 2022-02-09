.. _bufr4:

Anatomy of a BUFR4 message
--------------------------

.. role:: redtext

In order to understand the mapping between a CSV file and its BUFR encoding it is first helpful to understand
the anatomy of a BUFR message.
A BUFR message is encoded in binary and contains 6 sections as shown in the diagram below.
The ecCodes keys for the different elements are also shown as these are used to set certain elements
(highlighted in red) as part of the conversion to BUFR.
The non-highlighted elements (including those without an ecCodes key) are either set by the eccodes module based on the data,
have a default value or can be omitted / set to missing.

The information cotained in Sections 0, 1, and 3 are essentially metadata specifying:

* the version of the BUFR tables used (:redtext:`editionNumber, masterTableNumber, masterTableVersionNumber`);
* where the data have come from (:redtext:`bufrHeaderCentre, bufrHeaderSubCentre`);
* the typical time of the observation (:redtext:`typicalYear ... typicalSecond`);
* the type of data and what parameters are included (:redtext:`dataCategory, internationalDataSubCategory and unexpandedDescriptors`).

These are all specified in the BUFR template mapping file.

The :redtext:`edition number` and :redtext:`master table number` should be 4 and 0 respectively.
BUFR edition 4 is the latest version and whilst it is possible to define new BUFR tables for different application
areas only Master Table 0 (MT0) has been defined (meteorology).
The tables in MT0 are updated approximately every 6 months as part of the WMO fast track process.
The latest version can be found at https://github.com/wmo-im/BUFR4 and it is recommended to use the the most recent
version for the :redtext:`master table version number`.

The :redtext:`BUFR header centre` and :redtext:`bufr header sub centre` are specified in Common Code Tables C-11 and
C-12 respectively.
The typical time of observation (:redtext:`typicalYear ... typicalSecond`) should be determined based on the data to be
encoded.
Within the csv2bufr module and CLI ony a single observation / weather report is encoded per file and so these should
be set to those columns in the csv specifying the year, month, day etc.
More information is provided in the page on the BUFR template mapping (:redtext:`link to follow`).

The :redtext:`data category` should be set accoridng to BUFR Table A, i.e. 0 for "Surface data - land" and 1 for "Surface data - sea".
The :redtext:`international data sub category` should be set according to Common Code Table C-13.
The :redtext:`unexpanded descriptors` specifies the data to be encoded in the data section is comprised of a list of
the BUFR descriptors. These descriptors are detailed further on the next page (BUFR descriptors).

.. graphviz:: resources/bufr4_highlighted.dot

