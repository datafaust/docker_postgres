#!/bin/bash

#download data
curl -L 'https://data.cityofnewyork.us/api/views/76xm-jjuj/rows.csv?accessType=DOWNLOAD' -o ems_raw.csv

#load data to the table
psql -h localhost -U docker -d docker  --c "\COPY ems_stage FROM 'Desktop/ems/ems_raw.csv' WITH (format csv,header);"
