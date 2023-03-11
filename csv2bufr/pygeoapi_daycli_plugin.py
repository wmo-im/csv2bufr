###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################
import logging
from pygeoapi.process.base import BaseProcessor

from csv2bufr import BUFRMessage

LOGGER = logging.getLogger(__name__)

class _template():
    def __init__(self):
        self.template = {
            "inputShortDelayedDescriptorReplicationFactor": [],
            "inputDelayedDescriptorReplicationFactor": [],
            "inputExtendedDelayedDescriptorReplicationFactor": [],
            "number_header_rows": 1,
            "column_names_row": 1,
            "wigos_station_identifier": "data:wigos_station_identifier",
            "header": [
                {"eccodes_key": "edition", "value": "const:4"},
                {"eccodes_key": "masterTableNumber", "value": "const:0"},
                {"eccodes_key": "bufrHeaderCentre", "value": "const:0"},
                {"eccodes_key": "bufrHeaderSubCentre", "value": "const:0"},
                {"eccodes_key": "updateSequenceNumber", "value": "const:0"},
                {"eccodes_key": "dataCategory", "value": "const:0"},
                {"eccodes_key": "internationalDataSubCategory", "value": "const:21"},
                {"eccodes_key": "masterTablesVersionNumber", "value": "const:38"},
                {"eccodes_key": "typicalYear", "value": "data:year"},
                {"eccodes_key": "typicalMonth", "value": "data:month"},
                {"eccodes_key": "typicalDay", "value": "data:day"},
                {"eccodes_key": "numberOfSubsets", "value": "const:1"},
                {"eccodes_key": "observedData", "value": "const:1"},
                {"eccodes_key": "compressedData", "value": "const:0"},
                {"eccodes_key": "unexpandedDescriptors", "value": "array:307075"}
            ],
            "data": [
                {"eccodes_key": "#1#wigosIdentifierSeries", "value": "data:wsi_series"},
                {"eccodes_key": "#1#wigosIssuerOfIdentifier", "value": "data:wsi_issuer"},
                {"eccodes_key": "#1#wigosIssueNumber", "value": "data:wsi_issue"},
                {"eccodes_key": "#1#wigosLocalIdentifierCharacter", "value": "data:wsi_local"},
                {"eccodes_key": "#1#latitude", "value": "data:lat", "valid_min": "const:-90.0", "valid_max": "const:90.0"},
                {"eccodes_key": "#1#longitude", "value": "data:lon", "valid_min": "const:-180.0", "valid_max": "const:180.0"},
                {"eccodes_key": "#1#heightOfStationGroundAboveMeanSeaLevel", "value": "data:elev"},
                {"eccodes_key": "#1#methodUsedToCalculateTheAverageDailyTemperature", "value": "const:0"},
                {"eccodes_key": "#1#year", "value": "data:year", "valid_min": "const:1800", "valid_max": "const:2100"},
                {"eccodes_key": "#1#month", "value": "data:month", "valid_min": "const:1", "valid_max": "const:12"},
                {"eccodes_key": "#1#day", "value": "data:day", "valid_min": "const:1", "valid_max": "const:31"},
                {"eccodes_key": "#1#timePeriod", "value": "const:0"},
                {"eccodes_key": "#1#hour", "value": "const:0"},
                {"eccodes_key": "#1#minute", "value": "const:0"},
                {"eccodes_key": "#1#minute", "value": "const:0"},
                {"eccodes_key": "#1#second", "value": "const:0"},
                {"eccodes_key": "#1#totalAccumulatedPrecipitation", "value": "data:total_accumulated_precipitation"},
                {"eccodes_key": "#1#totalAccumulatedPrecipitation->associatedField", "value": "data:total_accumulated_precipitation_flag"},
                {"eccodes_key": "#1#totalAccumulatedPrecipitation->associatedField->associatedFieldSignificance", "value": "const:5"},
                {"eccodes_key": "#2#timePeriod", "value": "const:0"},
                {"eccodes_key": "#2#hour", "value": "const:0"},
                {"eccodes_key": "#2#minute", "value": "const:0"},
                {"eccodes_key": "#2#second", "value": "const:0"},
                {"eccodes_key": "#1#depthOfFreshSnow", "value": "data:fresh_snow_depth"},
                {"eccodes_key": "#1#depthOfFreshSnow->associatedField", "value": "data:fresh_snow_depth_flag"},
                {"eccodes_key": "#1#depthOfFreshSnow->associatedField->associatedFieldSignificance", "value": "const:5"},
                {"eccodes_key": "#3#timePeriod", "value": "const:0"},
                {"eccodes_key": "#3#hour", "value": "const:0"},
                {"eccodes_key": "#3#minute", "value": "const:0"},
                {"eccodes_key": "#3#second", "value": "const:0"},
                {"eccodes_key": "#1#totalSnowDepth", "value": "data:total_snow_depth"},
                {"eccodes_key": "#1#totalSnowDepth->associatedField", "value": "data:total_snow_depth_flag"},
                {"eccodes_key": "#1#totalSnowDepth->associatedField->associatedFieldSignificance", "value": "const:5"},
                {"eccodes_key": "#1#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "const:0"},
                {"eccodes_key": "#4#timePeriod", "value": "const:0"},
                {"eccodes_key": "#4#hour", "value": "const:0"},
                {"eccodes_key": "#4#minute", "value": "const:0"},
                {"eccodes_key": "#4#second", "value": "const:0"},
                {"eccodes_key": "#1#firstOrderStatistics", "value": "const:2"},
                {"eccodes_key": "#1#airTemperature", "value": "data:maximum_temperature", "scale": "const:0", "offset": "const:273.15"},
                {"eccodes_key": "#1#airTemperature->associatedField", "value": "data:maximum_temperature_flag"},
                {"eccodes_key": "#1#airTemperature->associatedField->associatedFieldSignificance", "value": "const:5"},
                {"eccodes_key": "#5#timePeriod", "value": "const:0"},
                {"eccodes_key": "#5#hour", "value": "const:0"},
                {"eccodes_key": "#5#minute", "value": "const:0"},
                {"eccodes_key": "#5#second", "value": "const:0"},
                {"eccodes_key": "#2#firstOrderStatistics", "value": "const:3"},
                {"eccodes_key": "#2#airTemperature", "value": "data:minimum_temperature", "scale": "const:0", "offset": "const:273.15"},
                {"eccodes_key": "#2#airTemperature->associatedField", "value": "data:minimum_temperature_flag"},
                {"eccodes_key": "#2#airTemperature->associatedField->associatedFieldSignificance", "value": "const:5"},
                {"eccodes_key": "#6#timePeriod", "value": "const:0"},
                {"eccodes_key": "#6#hour", "value": "const:0"},
                {"eccodes_key": "#6#minute", "value": "const:0"},
                {"eccodes_key": "#6#second", "value": "const:0"},
                {"eccodes_key": "#3#firstOrderStatistics", "value": "const:4"},
                {"eccodes_key": "#3#airTemperature", "value": "data:mean_temperature", "scale": "const:0", "offset": "const:273.15"},
                {"eccodes_key": "#3#airTemperature->associatedField", "value": "data:mean_temperature_flag"},
                {"eccodes_key": "#3#airTemperature->associatedField->associatedFieldSignificance", "value": "const:5"}
            ]
        }

    def set_element(self, section, key, value):
        idx = self.get_idx(self.template[section], key)
        self.template[section][idx]['value'] = value

    def get_idx(self,elements, key):
        idx = 0
        position = None
        for element in elements:
            if key == element['eccodes_key']:
                position = idx
                break
            idx += 1
        return position

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "daycli-encoder",
    "title": {"en": "daycli-encoder"},
    "description": {"en": "Process to convert DAYCLI data to BUFR"},  # noqa
    "keywords": [],
    "links": [{
        "type": "text/html",
        "rel": "about",
        "title": "information",
        "href": "https://example.org/process",
        "hreflang": "en-US",
    }],
    "inputs": {},
    "outputs": {},
    "example": {
        "inputs": {},
        "output": {"messages": []}
    }
}

def element(elements, key, value):
    idx = 0
    position = None
    for element in elements:
        if key == element['eccodes_key']:
            position = idx
            break
        idx += 1
    return position

# need to fix this to index elements, e.g. bufrHeaderCentre by index

def update_template(template, metadata):
    wsi = f"const:{metadata['station_identification']['wigos_identifier']}"
    wsi_series, wsi_issuer, wsi_issue, wsi_local = wsi.split("-")
    template.set_element('header', 'bufrHeaderCentre', f"const:{metadata['data_identification']['originating_centre']}")
    template.set_element('header', 'bufrHeaderSubCentre', f"const:{metadata['data_identification']['originating_subcentre']}")
    template.set_element('data','wsi_series', f"const:{wsi_series}")
    template.set_element('data','wsi_issuer', f"const:{wsi_issuer}")
    template.set_element('data','wsi_issue', f"const:{wsi_issue}")
    template.set_element('data','wsi_local', f"const:{wsi_local}")
    template.set_element('data','#1#latitude', f"const:{metadata['station_location']['latitude']}")
    template.set_element('data','#1#longitude', f"const:{metadata['station_location']['longitude']}")
    template.set_element('data','#1#heightOfStationGroundAboveMeanSeaLevel', f"const:{metadata['station_location']['station_height_above_sea_level']}")
    template.set_element('data','#1#methodUsedToCalculateTheAverageDailyTemperature', f"const:{metadata['observing_practices']['method_of_calculating_mean_temperature']}")
    template.set_element('data','#1#timePeriod', f"const:{metadata['observing_practices']['total_precipitation_day_offset']}")
    hour, minute, second = metadata['observing_practices']['total_precipitation_start_time'].split(":")
    template.set_element('data','#1#hour', f"const:{hour}")
    template.set_element('data','#1#minute', f"const:{minute}")
    template.set_element('data','#1#second', f"const:{second}")
    template.set_element('data','#2#timePeriod', f"const:{metadata['observing_practices']['fresh_snow_depth_day_offset']}")
    hour, minute, second = metadata['observing_practices']['fresh_snow_depth_start_time'].split(":")
    template.set_element('data','#2#hour', f"const:{hour}")
    template.set_element('data','#2#minute', f"const:{minute}")
    template.set_element('data','#2#second', f"const:{second}")
    template.set_element('data','#3#timePeriod', f"const:{metadata['observing_practices']['total_snow_depth_day_offset']}")
    hour, minute, second = metadata['observing_practices']['total_snow_depth_start_time'].split(":")
    template.set_element('data','#3#hour', f"const:{hour}")
    template.set_element('data','#3#minute', f"const:{minute}")
    template.set_element('data','#3#second', f"const:{second}")
    template.set_element('data','#1#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', f"const:{metadata['station_location']['thermometer_height_above_local_ground']}")
    template.set_element('data','#4#timePeriod', f"const:{metadata['observing_practices']['maximum_temperature_day_offset']}")
    hour, minute, second = metadata['observing_practices']['maximum_temperature_start_time'].split(":")
    template.set_element('data','#4#houre', f"const:{hour}")
    template.set_element('data','#4#minute', f"const:{minute}")
    template.set_element('data','#4#second', f"const:{second}")
    template.set_element('data','#5#timePeriod', f"const:{metadata['observing_practices']['minimum_temperature_day_offset']}")
    hour, minute, second = metadata['observing_practices']['minimum_temperature_start_time'].split(":")
    template.set_element('data','#5#hour', f"const:{hour}")
    template.set_element('data','#5#minute', f"const:{minute}")
    template.set_element('data','#5#second', f"const:{second}")
    template.set_element('data','#6#timePeriod', f"const:{metadata['observing_practices']['mean_temperature_day_offset']}")
    hour, minute, second = metadata['observing_practices']['mean_temperature_start_time'].split(":")
    template.set_element('data','#6#hour', f"const:{hour}")
    template.set_element('data','#6#minute', f"const:{minute}")
    template.set_element('data','#6#second', f"const:{second}")
    return template


class daycliProcessor(BaseProcessor):
    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.csv2bufr.csv2bufr
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        This method is invoked by pygeoapi when this class is set as a
        `process` type resource in pygeoapi config file.

        :param data: It is the value of `inputs` passed in payload. e.g.
        {
            "inputs": {
                "data": "csv data to encode",
                "mappings": "csv2bufr mapping json file"
            }
        }

        :return: media_type, json
        """

        mimetype = "application/json"

        # first validate input and collect errors
        # todo

        # update mapping template
        template = _template()
        template = update_template(template, data['metadata'])


        bufr = []
        errors = []
        idx = 0
        for day in data['properties']['data']:
            # parse date and add to day
            day['year'], day['month'], day['day'] = day['nominal_reporting_day'].split('-')  # noqa
            error_flag = 0
            # create empty BUFR messsage
            try:
                # create new BUFR msg
                message = BUFRMessage([307075],[],[],[],38)
                # parse data
                message.parse(day, template)
                # encode
                result = message.as_bufr()
            except Exception as e:
                LOGGER.error(e)
                LOGGER.error("Error creating BUFRMessage")
                error_flag = 1
                result = None
                errors.append(f"Error processing data for day {idx}")

            if not error_flag:
                bufr.append(result)
            idx += 1

        output = {
            "messages": bufr,
            "errors": errors
        }

        return mimetype, output

    def __repr__(self):
        return "<encode_daycli> {}".format(self.name)