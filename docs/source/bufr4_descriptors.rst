BUFR4 Descriptors
=================

.. role:: redtext


Introduction
------------

The data contained within the data section of a BUFR message (Section 4, byte 5 onwards - see previous page)
are specified within the :redtext:`unexpanded descriptors` from Section 3 of the BUFR message.
This element takes a list of the parameters to be included and the order of those parameters.
For example, the unexpanded descriptor list (in text, see format of descriptors below):

.. code::

	["unexpandedDescriptors"] = ["wigosID", "year", "month", "day", "hour", "minute", \
                                 "latitude", "longitude", "mean sea level pressure"]

would specify that the station identifier (wigosID) followed by the date, time, location and then mean sea level
pressure would be included in the data section. For conciseness, aliases (or sequences in BUFR terminology) exist to group commonly
reported parameters together, for example grouping the year, month and day together.
Using sequences, the above example becomes:

.. code::

	["unexpandedDescriptors"] = ["wigosID", "date", "time", "location", "mean sea level pressure"]

where

.. code::

	["date"] = ["year", "month", "day"]
	["time"] = ["hour", "minute"]
	["location"] = ["latitude", "longitude"]

These sequences form building blocks that can be used to create much longer sequences using a small number of descriptors.

Format of BUFR descriptors
--------------------------

Whilst text strings have been used above to represent the parameters to report this has been done for ease of reading
and explanation.
Within the BUFR format these are specified by 6 digit codes of the form FXXYYY, with each of F, XX and YYY having
specific meaning.

* F: Type of BUFR descriptor (0: element descriptor (BUFR Table B); 1: replication (or repetition) descriptor; 2: operator (BUFR Table C); 3: sequence (or alias) descriptor (BUFR Table D))
* XX: Sub-table within class of descriptor
* YYY: Line in sub-table.

As an example, the table below shows the first few entries from BUFR Table B 01.

For the above example, using the FXXYYY notation, the list of abbreviated (or unexpanded) descriptors becomes:

.. code::

    ["unexpandedDescriptors"] = [301150, 301011, 301012, 301023, 010051]

where:

.. code::

    [301150] = [001125, 001126, 001127, 001128] # (wigosID)
             = ["WIGOS identifer series", "WIGOS identifier issuer", "WIGOS identifier issue number", "WIGOS local identifier"]
    [301011] = [004001,  004002, 004003] # (date)
             = ["year", "month",  "day"]
    [301012] = [004004,   004005] # (time)
             = ["hour", "minute"]
    [301023] = [    005002,      006002] # (location)
             = ["latitude", "longitude"]
    [010051] = ["mean sea level pressure"]


Replication / repetition
------------------------

Within BUFR elements can be repeated using the replication descriptors (F=1 in FXXYYY).
For example, we may want to repeat temperature and humidity measurements as part of an atmospheric
profile or, alternatively, the daily minimum and maximum temperatures within a month.
In some cases we may know the number of repetitions before encoding and all data of the same type
may have the same number of repetitions. In this case th number of replications can be set before hand.
In other cases, there may be a variable number of repetitions and so the number is set at the time of encoding.

When using the replication descriptor (1XXYYY), the XX component indicates the number of following descriptors to repeat
and the YYY component the number of replications.
For example, to repeat the day of month, maximum and minimum temperatures 5 times we would use:

.. math::
	\left[\text{unexpandedDescriptors}\right] = \left[
				\underbrace{\overbrace{103005}}^{\text{repeat (1) next 3 (03) descriptors 5 (005) times}}_{FXXYYY},
				\overbrace{004003,012016,012017}^{\text{descriptors to be repeated}}
				\right]

In expanded form, or without using the replication, this would be equivalent to:

.. math::
   :nowrap:

   \begin{eqnarray}
      \left[\text{expandedDescriptors}\right] & = [& 004003, 012016, 012017, \\
      && 004003, 012016, 012017, \\
      && 004003, 012016, 012017, \\
      && 004003, 012016, 012017, \\
      && 004003, 012016, 012017]
   \end{eqnarray}

If we do not known the number of replications, or there can be a variable number of replications, for a given
sequence of descriptors we can set the YYY element (number of replications) to zero and follow the replication
descriptor with a delayed replication factor.

.. math::
	\left[\text{unexpandedDescriptors}\right] = \left[
				\underbrace{\overbrace{103000}^{\text{(repeat (1) next 3 (03) items n time)}},
				\overbrace{031001}^{\text{(delayed number (n) of replications)}}}_{\text{replication and delayed replication factor}},
				\overbrace{004003,012016,012017}^{\text{(descriptors to be repeated)}}
				\right]

This works in the same way as the regular replication except that the number of replications (n) is set at the
the time of encoding and included in data.
Often, within sequences, delayed descriptors are used to specify optional elements using the short delayed
descriptor replication factor (031000) that takes a value of either 0 or 1.

Within the csv2bufr module the number of delayed replications needs to be set within the mapping file using the
`inputDelayedDescriptorReplicationFactor` key. More information is provided on the mappings page.

Commonly used sequences
-----------------------
Listed below are some commonly used sequences:

- 307080: Sequence for representation of synoptic reports from a fixed land station suitable for SYNOP data.
- 315008: Sequence for the representation of data from moored buoys.
- 315009: Sequence for the representation of data from drifting buoys.
- :redtext:`more to follow`


Further information
-------------------
The description of the BUFR operators (F = 2 in the FXXYYY notation) is beyond the scope of this documentation.
For users wanting to define new sequences, including the use of the operators, it is recommended to
refer to Volume I.2 of the WMO Manual on Codes. However, before defining a new sequence it is recommended
to check if any of the existing sequence meet the user requirements. :redtext:`Add where to get further advice from`.
