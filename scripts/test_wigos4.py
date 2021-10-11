#!/usr/bin/env python
 
from eccodes import (codes_set, codes_set_array, codes_bufr_new_from_samples,codes_write,codes_get_api_version,
                     codes_release, CodesInternalError,CODES_MISSING_DOUBLE,CODES_MISSING_LONG)
import pandas as pd
from datetime import datetime
import argparse
 
def read_cmdline():
    '''
    reads the command line to get the input ascii filename and the output bufr file
    usage
         prog  -a Ascii_input_file  -b Bufr_output_file
    '''
    p=argparse.ArgumentParser()
    p.add_argument("-a","--ascii",help=" input Ascii filename")
    p.add_argument("-b","--bufr", help="output Bufr filename")
    args=p.parse_args()
    return args
 
def read_ascii(inputFilename):
    '''
    function to read the Ascii data into a pandas dataframe,
    args:
          inputFilename :   full path of the Ascii file for example /tmp/data/rema_20180918.txt
      
    uses white spaces as column delimiters
    index_col=False avoids using the first column (Station) as index
    names is the list of names from the excel it can be changed but this affects the dataframe
    '''
    df=pd.read_csv(inputFilename,header=None,index_col=False,delim_whitespace=True,names=["station","year","month","day",
                                                        "ObsHour","TensBat","TempCpu","lon","lat","hp","airTinst","airTmax",
                                                      "airTmin","relHinst","relHmax","relHmin","dewPInst","dewPmax","dewPmin" ,
                                                      "PresInst","PresMax","PresMin","WindSpeed","WindDir","WindGust",
                                                       "Rad","Precip","CloudCoverTot","CloudCODE","CloudBase","Visib"])
    print df.head()
    return df
 
 
def message_encoding(FullInputFileName,fout):
    '''
    Message encoding function
    FullInputFilename      :     full path of the Ascii file for example /tmp/data/rema_20180918.txt
    fout                   :     file Object to write the output bufr file( obtained by a call to open )
     
    Requires ecCodes and the BUFR4_local template on 
                 ECCODES_PATH/share/eccodes/samples
 
    '''
    TEMPLATE='BUFR4_local'
     
    # reads the Ascii file into a pandas Dataframe
    dfFull=read_ascii(FullInputFileName)
   
    # loops over the rows of the dataFrame dfFull 
    for _,row in dfFull.iterrows():
        bid=codes_bufr_new_from_samples(TEMPLATE)
        try:
            bufr_encode_new(bid,row)
            codes_write(bid,fout)
        except CodesInternalError as ec:
            print ec
        codes_release(bid)
 
 
def bufr_encode_new(ibufr,row):
    '''
    encodes the new SYNO 307091 adding the  1125, 1126, 1127, 1128 wigos keys before.
    '''
    ivalues = ( 
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  
      1, 1, 1, 1 ,)
    codes_set_array(ibufr, 'inputShortDelayedDescriptorReplicationFactor', ivalues)
    codes_set(ibufr, 'edition', 4)
    codes_set(ibufr, 'masterTableNumber', 0)
    codes_set(ibufr, 'bufrHeaderCentre', 98)
    codes_set(ibufr, 'bufrHeaderSubCentre', 0)
    codes_set(ibufr, 'updateSequenceNumber', 0)
    codes_set(ibufr, 'dataCategory', 0)
    codes_set(ibufr, 'internationalDataSubCategory', 255)
    codes_set(ibufr, 'dataSubCategory', 170)
    codes_set(ibufr, 'masterTablesVersionNumber', 29)
    codes_set(ibufr, 'localTablesVersionNumber', 0)
# set the YMD
    codes_set(ibufr, 'typicalYear', row["year"])
    codes_set(ibufr, 'typicalMonth', row["month"])
    codes_set(ibufr, 'typicalDay', row["day"])
    codes_set(ibufr, 'typicalHour', row["ObsHour"])
    codes_set(ibufr, 'typicalMinute', 0)
    codes_set(ibufr, 'typicalSecond', 0)
    # Encodes  the Section 2 of the BUFR used internally at ECMWF (start here)
    codes_set(ibufr, 'rdbType', 1)
    codes_set(ibufr, 'oldSubtype', 176)
    codes_set(ibufr, 'localYear', row["year"])
    codes_set(ibufr, 'localMonth', row["month"])
    codes_set(ibufr, 'localDay', row["day"])
    codes_set(ibufr, 'localHour', row["ObsHour"])
    codes_set(ibufr, 'localMinute', 0)
    codes_set(ibufr, 'localSecond', 0)
    procTime=datetime.now()
    codes_set(ibufr, 'rdbtimeDay', procTime.day)
    codes_set(ibufr, 'rdbtimeHour', procTime.hour)
    codes_set(ibufr, 'rdbtimeMinute', procTime.minute)
    codes_set(ibufr, 'rdbtimeSecond', procTime.second)
    codes_set(ibufr, 'rectimeDay', procTime.day )
    codes_set(ibufr, 'rectimeHour', procTime.hour)
    codes_set(ibufr, 'rectimeMinute', procTime.minute)
    codes_set(ibufr, 'rectimeSecond', procTime.second)
    codes_set(ibufr, 'correction1', 0)
    codes_set(ibufr, 'correction1Part', 0)
    codes_set(ibufr, 'correction2', 0)
    codes_set(ibufr, 'correction2Part', 0)
    codes_set(ibufr, 'correction3', 0)
    codes_set(ibufr, 'correction3Part', 0)
    codes_set(ibufr, 'correction4', 0)
    codes_set(ibufr, 'correction4Part', 0)
    codes_set(ibufr, 'qualityControl', 70)
    codes_set(ibufr, 'newSubtype', 0)
    codes_set(ibufr, 'numberOfSubsets', 1)
    lat=row["lat"]
    lon=row["lon"]
    codes_set(ibufr, 'localLatitude', lat)
    codes_set(ibufr, 'localLongitude', lon)
    #### End of encoding local section 2
    codes_set(ibufr, 'observedData', 1)
    codes_set(ibufr, 'compressedData', 0)
 
    ivalues=(301150,307091)
    codes_set_array(ibufr, 'unexpandedDescriptors', ivalues)
 
    codes_set(ibufr, 'wigosIdentifierSeries',0 )
    codes_set(ibufr, 'wigosIssuerOfIdentifier', 76)
    codes_set(ibufr, 'wigosIssueNumber', 0)
    codes_set(ibufr, 'wigosLocalIdentifierCharacter','0760999999999')
    codes_set(ibufr, 'stateIdentifier', CODES_MISSING_LONG)
    codes_set(ibufr, 'nationalStationNumber', CODES_MISSING_LONG)
    codes_set(ibufr, 'blockNumber', CODES_MISSING_LONG)
    codes_set(ibufr, 'stationNumber', CODES_MISSING_LONG)
    codes_set(ibufr, 'stationOrSiteName',row["station"])
    codes_set(ibufr, 'stationType', CODES_MISSING_LONG)
    codes_set(ibufr, 'year', row["year"])
    codes_set(ibufr, 'month', row["month"])
    codes_set(ibufr, 'day', row["day"])
    codes_set(ibufr, 'hour', row["ObsHour"])
    codes_set(ibufr, 'minute', 0)
    codes_set(ibufr, 'latitude', lat)
    codes_set(ibufr, 'longitude', lon)
    height=row["hp"]
    codes_set(ibufr, 'heightOfStationGroundAboveMeanSeaLevel', height)
    codes_set(ibufr, 'heightOfBarometerAboveMeanSeaLevel', 1.5)
    codes_set(ibufr, 'surfaceQualifierForTemperatureData', CODES_MISSING_LONG)
    codes_set(ibufr, 'mainPresentWeatherDetectingSystem', CODES_MISSING_LONG)
    codes_set(ibufr, 'supplementaryPresentWeatherSensor', CODES_MISSING_LONG)
    codes_set(ibufr, 'visibilityMeasurementSystem', CODES_MISSING_LONG)
    codes_set(ibufr, 'cloudDetectionSystem', CODES_MISSING_LONG)
    codes_set(ibufr, 'lightningDetectionSensorType', CODES_MISSING_LONG)
    codes_set(ibufr, 'skyConditionAlgorithmType', CODES_MISSING_LONG)
    codes_set(ibufr, 'capabilityToDetectPrecipitationPhenomena', CODES_MISSING_LONG)
    codes_set(ibufr, 'capabilityToDetectOtherWeatherPhenomena', CODES_MISSING_LONG)
    codes_set(ibufr, 'capabilityToDetectObscuration', CODES_MISSING_LONG)
    codes_set(ibufr, 'capabilityToDiscriminateLightningStrikes', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#nonCoordinatePressure', CODES_MISSING_DOUBLE)
    pressure=row["PresInst"]*100
    codes_set(ibufr, 'pressureReducedToMeanSeaLevel', pressure)
    codes_set(ibufr, '3HourPressureChange', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'characteristicOfPressureTendency', CODES_MISSING_LONG)
    codes_set(ibufr, 'pressure', pressure)
    codes_set(ibufr, 'nonCoordinateGeopotentialHeight', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#1#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    temperature=row["airTinst"]+273.15
    codes_set(ibufr, '#1#airTemperature', temperature)
    dewPoint=row["dewPInst"]+273.15        
    codes_set(ibufr, 'dewpointTemperature', dewPoint)
    codes_set(ibufr, '#1#relativeHumidity', row["relHinst"])
    codes_set(ibufr, '#1#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#1#soilTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#soilTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#3#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#3#soilTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#4#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#4#soilTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#5#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#5#soilTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#6#depthBelowLandSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#1#attributeOfFollowingValue', CODES_MISSING_LONG)
    if row["Visib"]=="/////":
        visib=CODES_MISSING_DOUBLE
    else:
        visib=row["Visib"]
    codes_set(ibufr, 'horizontalVisibility', visib)
    codes_set(ibufr, '#3#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#3#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'iceDepositThickness', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'rateOfIceAccretionEstimated', CODES_MISSING_LONG)
    codes_set(ibufr, 'methodOfWaterTemperatureAndOrOrSalinityMeasurement', CODES_MISSING_LONG)
    codes_set(ibufr, 'oceanographicWaterTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'wavesDirection', CODES_MISSING_LONG)
    codes_set(ibufr, 'periodOfWaves', CODES_MISSING_LONG)
    codes_set(ibufr, 'heightOfWaves', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'methodOfStateOfGroundMeasurement', CODES_MISSING_LONG)
    codes_set(ibufr, 'stateOfGround', CODES_MISSING_LONG)
    codes_set(ibufr, 'methodOfSnowDepthMeasurement', CODES_MISSING_LONG)
    codes_set(ibufr, 'totalSnowDepth', CODES_MISSING_DOUBLE)
    if row["CloudCoverTot"]=="/":
        CloudCover=CODES_MISSING_LONG
    else:
        CloudCover=row["CloudCoverTot"]  
    codes_set(ibufr, 'cloudCoverTotal', CloudCover)
    codes_set(ibufr, '#1#verticalSignificanceSurfaceObservations', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#cloudAmount', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#cloudType', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#attributeOfFollowingValue', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#heightOfBaseOfCloud', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#verticalSignificanceSurfaceObservations', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#cloudAmount', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#cloudType', CODES_MISSING_LONG)
    codes_set(ibufr, '#3#attributeOfFollowingValue', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#heightOfBaseOfCloud', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#3#verticalSignificanceSurfaceObservations', CODES_MISSING_LONG)
    codes_set(ibufr, '#3#cloudAmount', CODES_MISSING_LONG)
    codes_set(ibufr, '#3#cloudType', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#attributeOfFollowingValue', CODES_MISSING_LONG)
    codes_set(ibufr, '#3#heightOfBaseOfCloud', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#4#verticalSignificanceSurfaceObservations', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#cloudAmount', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#cloudType', CODES_MISSING_LONG)
    codes_set(ibufr, '#5#attributeOfFollowingValue', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#heightOfBaseOfCloud', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'presentWeather', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'pastWeather1', CODES_MISSING_LONG)
    codes_set(ibufr, 'pastWeather2', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#timeSignificance', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'precipitationIntensityHighAccuracy', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'sizeOfPrecipitatingElement', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#timeSignificance', CODES_MISSING_LONG)
    codes_set(ibufr, '#3#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'precipitationType', CODES_MISSING_LONG)
    codes_set(ibufr, 'characterOfPrecipitation', CODES_MISSING_LONG)
    codes_set(ibufr, 'durationOfPrecipitation', CODES_MISSING_LONG)
    codes_set(ibufr, 'otherWeatherPhenomena', CODES_MISSING_LONG)
    codes_set(ibufr, 'intensityOfPhenomena', CODES_MISSING_LONG)
    codes_set(ibufr, 'obscuration', CODES_MISSING_LONG)
    codes_set(ibufr, 'characterOfObscuration', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#4#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#3#timeSignificance', CODES_MISSING_LONG)
    codes_set(ibufr, '#4#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#windDirection', row["WindDir"])
    codes_set(ibufr, '#1#windSpeed', row["WindSpeed"])
    codes_set(ibufr, '#4#timeSignificance', CODES_MISSING_LONG)
    codes_set(ibufr, '#5#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#maximumWindGustDirection', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#maximumWindGustSpeed', row["WindGust"])
    codes_set(ibufr, '#6#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#maximumWindGustDirection', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#maximumWindGustSpeed', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#7#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'extremeCounterclockwiseWindDirectionOfAVariableWind', CODES_MISSING_LONG)
    codes_set(ibufr, 'extremeClockwiseWindDirectionOfAVariableWind', CODES_MISSING_LONG)
    codes_set(ibufr, '#5#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#5#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#8#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'maximumTemperatureAtHeightAndOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#1#minimumTemperatureAtHeightAndOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#6#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#9#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#minimumTemperatureAtHeightAndOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#6#heightOfSensorAboveWaterSurface', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#7#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'methodOfPrecipitationMeasurement', CODES_MISSING_LONG)
    codes_set(ibufr, 'methodOfLiquidContentMeasurementOfPrecipitation', CODES_MISSING_LONG)
    codes_set(ibufr, '#10#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'totalPrecipitationOrTotalWaterEquivalent', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#8#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'methodOfEvaporationMeasurement', CODES_MISSING_LONG)
    codes_set(ibufr, '#11#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'evaporation', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#12#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'totalSunshine', CODES_MISSING_LONG)
    codes_set(ibufr, '#13#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'longWaveRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'shortWaveRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'netRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'globalSolarRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'diffuseSolarRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, 'directSolarRadiationIntegratedOverPeriodSpecified', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#14#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, 'numberOfFlashesThunderstorm', CODES_MISSING_LONG)
    codes_set(ibufr, '#15#timePeriod', CODES_MISSING_LONG)
    codes_set(ibufr, '#1#firstOrderStatistics', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#nonCoordinatePressure', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#windDirection', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#windSpeed', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#airTemperature', CODES_MISSING_DOUBLE)
    codes_set(ibufr, '#2#relativeHumidity', CODES_MISSING_LONG)
    codes_set(ibufr, '#2#firstOrderStatistics', CODES_MISSING_LONG)
    codes_set(ibufr, 'qualityInformationAwsData', CODES_MISSING_LONG)
    codes_set(ibufr, 'internalMeasurementStatusInformationAws', CODES_MISSING_LONG)
 
    # Encode the keys back in the data section
    codes_set(ibufr, 'pack', 1)
 
 
         
 
 
 
def main():
    '''
    main program reads the command line and encodes the messages into the output filename
       to run the program
        
          program_name.py   -a Ascii_input_file  -b Bufr_output_file
    '''
     
    print " codes version {0}".format(codes_get_api_version())
    cmdLine=read_cmdline()
    inputFilename=cmdLine.ascii
    outFilename=cmdLine.bufr
    fout=open(outFilename,"w")
    message_encoding(inputFilename,fout)
    fout.close()
    print " output file {0}".format(outFilename)
     
if __name__ == '__main__':
    main()
