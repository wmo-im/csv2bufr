#!/usr/bin/bash

for file in ./data/input/*_SYNOP.csv
do
file=`basename ${file}`
echo ${file}
station_name=`echo ${file} | cut -d '.' -f 1 | sed s/_SYNOP//g`
WSI=`grep ${station_name} ./data/input/station_list.csv | cut -d ',' -f 2`
csv2bufr data transform \
   ./data/input/${file} \
   --mapping malawi_synop_bufr \
   --geojson-template malawi_synop_json \
   --output-dir ./data/output \
   --station-metadata ./metadata/${WSI}.json >& ${WSI}.log
done