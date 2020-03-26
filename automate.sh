#!/bin/bash

#automation setup
#update functions are set to run via a cron job once a week, this file will be added as a cron job

#1. new data is downloaded
#2. data is loaded to staging
#3. data is cleaned and loaded to production
#4. data in aggregate tables is updated

#change this directory to whereever you cloned the repo
my_directory="Desktop/docker_postgres"

#pull data from the most recent data you have
#yesterday=$(date -d "$date -120 days" +"%Y-%m-%d")
yesterday=`psql -X -A -d postgres -U postgres -h localhost -t -c "SELECT MAX(CAST(INCIDENT_DATETIME AS DATE)) FROM ems_prd;"`

#download data from SODA API
echo $yesterday | curl -L 'https://data.cityofnewyork.us/resource/76xm-jjuj.csv?$where=incident_datetime>="'$yesterday'"&$limit=50000' -o "$my_directory/ems_raw_update.csv"

#load data
psql -h localhost -U postgres -d postgres -w --c "\COPY ems_stage FROM '$my_directory/ems_raw_update.csv' WITH (format csv,header);"

#run update jobs
psql -h localhost -U postgres -d postgres -w --c "SELECT insert_stage_to_prd();"

psql -h localhost -U postgres -d postgres -w --c "SELECT insert_daily_borough_metrics();"

psql -h localhost -U postgres -d postgres -w --c "SELECT insert_daily_community_district_metrics();"

psql -h localhost -U postgres -d postgres -w --c "SELECT weekday_hourly_borough_metrics();"

psql -h localhost -U postgres -d postgres -w --c "DELETE FROM ems_stage;"

psql -h localhost -U postgres -d postgres -w --c "SELECT * from errors;"

#remove the update file
rm "$my_directory/ems_raw_update.csv"
