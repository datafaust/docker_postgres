#!/bin/bash

#load all data
psql -h localhost -U postgres -d postgres  --c "\COPY ems_stage FROM 'ems_raw.csv' WITH (format csv,header);"
psql -h localhost -U postgres -d postgres  --c "\COPY data_dictionary FROM 'data_dictionary.csv' WITH (format csv,header);"
psql -h localhost -U postgres -d postgres  --c "\COPY incident_disposition FROM 'incident_disposition.csv' WITH (format csv,header);"
psql -h localhost -U postgres -d postgres  --c "\COPY call_type_description FROM 'call_type_description.csv' WITH (format csv,header);"


#run clean up and setup
psql -h localhost -U postgres -d postgres  --c "SELECT initial_bulk_insert();"

#create aggregate tables
psql -h localhost -U postgres -d postgres  --c "SELECT create_daily_borough_metrics();"
psql -h localhost -U postgres -d postgres  --c "SELECT create_daily_community_district_metrics();"
psql -h localhost -U postgres -d postgres  --c "SELECT create_weekday_hourly_borough_metrics();"

#kill staging
psql -h localhost -U postgres -d postgres  --c "DELETE FROM ems_stage;"

#return all errors
psql -h localhost -U postgres -d postgres  --c "SELECT * from errors;"
