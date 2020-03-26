-----------------------------------------------------------sql schema initalization
-----process description
-----1. schema is initialized witrh functions and tables 
-----2. data is dowloaded
-----3. user runs initial_download.sh which populates tables
-----4. user runs automate_job.sh to setup cron job that automates

---------------------------Create Staging Table
CREATE TABLE ems_stage (
  cad_incident_id VARCHAR,                
  incident_datetime VARCHAR,             
  initial_call_type VARCHAR,              
  initial_severity_level_code VARCHAR,   
  final_call_type VARCHAR,                
  final_severity_level_code VARCHAR,     
  first_assignment_datetime VARCHAR,      
  valid_dispatch_rspns_time_indc VARCHAR,
  dispatch_response_seconds_qy VARCHAR,   
  first_activation_datetime VARCHAR,     
  first_on_scene_datetime VARCHAR,        
  valid_incident_rspns_time_indc VARCHAR,
  incident_response_seconds_qy VARCHAR,   
  incident_travel_tm_seconds_qy VARCHAR, 
  first_to_hosp_datetime VARCHAR,         
  first_hosp_arrival_datetime VARCHAR,   
  incident_close_datetime VARCHAR,        
  held_indicator VARCHAR,                
  incident_disposition_code VARCHAR,      
  borough VARCHAR,                       
  incident_dispatch_area VARCHAR,         
  zipcode VARCHAR,                       
  policeprecinct VARCHAR,                 
  citycouncildistrict VARCHAR,           
  communitydistrict VARCHAR,              
  communityschooldistrict VARCHAR,       
  congressionaldistrict VARCHAR,          
  reopen_indicator VARCHAR,              
  special_event_indicator VARCHAR,        
  standby_indicator VARCHAR,             
  transfer_indicator VARCHAR          
);

---------------------------Create Prd Table
-----DROP TABLE ems_prd;
CREATE TABLE ems_prd (
  cad_incident_id VARCHAR PRIMARY KEY,                
  incident_datetime TIMESTAMP,             
  initial_call_type VARCHAR,              
  initial_severity_level_code INT,   
  final_call_type VARCHAR,                
  final_severity_level_code INT,     
  first_assignment_datetime TIMESTAMP,      
  valid_dispatch_rspns_time_indc BOOLEAN,
  dispatch_response_seconds_qy INT,   
  first_activation_datetime TIMESTAMP,     
  first_on_scene_datetime TIMESTAMP,        
  valid_incident_rspns_time_indc BOOLEAN,
  incident_response_seconds_qy INT,   
  incident_travel_tm_seconds_qy INT, 
  first_to_hosp_datetime TIMESTAMP,         
  first_hosp_arrival_datetime TIMESTAMP,   
  incident_close_datetime TIMESTAMP,        
  held_indicator BOOLEAN,                
  incident_disposition_code INT,      
  borough VARCHAR,                       
  incident_dispatch_area VARCHAR,         
  zipcode INT,                       
  policeprecinct INT,                 
  citycouncildistrict INT,           
  communitydistrict INT,              
  communityschooldistrict INT,       
  congressionaldistrict INT,          
  reopen_indicator BOOLEAN,              
  special_event_indicator BOOLEAN,        
  standby_indicator BOOLEAN,             
  transfer_indicator BOOLEAN,
  incident_date DATE,
  incident_disposition_code_description VARCHAR,
  final_call_type_description varchar,
  final_call_type_description_big_category varchar
  
);

------------create auxiliary meta data tables and error table

CREATE TABLE incident_disposition (
  incident_disposition_code VARCHAR,
  incident_disposition_code_description VARCHAR
);

CREATE TABLE call_type_description (
  call_type VARCHAR,
  call_type_description VARCHAR,
  call_type_description_big_category VARCHAR
);

CREATE TABLE data_dictionary (
  field_name VARCHAR,
  field_name_description VARCHAR
);

CREATE TABLE errors (
  id SERIAL, 
  the_date timestamp,
  sql_state TEXT, 
  message TEXT, 
  detail TEXT, 
  hint TEXT, 
  context TEXT
  );




------------------------------------------------------------functions


----function descriptions
---1. RemovePunctuation(); --removes popular punctuation from a column
---2. initial_bulk_insert(); --handles initial bulk insert from staging to production
---3. insert_stage_to_prd(); --handles weekly inserts from staging to production
---4. daily_borough_metrics(); --handles updates 


---clean punctuation
---DROP FUNCTION RemovePunctuation;
CREATE OR REPLACE FUNCTION RemovePunctuation(InputString VARCHAR)
RETURNS VARCHAR
AS $$
  SELECT REPLACE(REPLACE(REPLACE(InputString, ',', ''),'.',''),'-','')
$$ LANGUAGE SQL;


---------------initial bulk insert into production from staging
CREATE OR REPLACE FUNCTION initial_bulk_insert()
RETURNS VOID AS
$BODY$
  DECLARE
_the_date timestamp without time zone NOT NULL
DEFAULT (current_timestamp AT TIME ZONE 'America/New_York');
_sql_state TEXT;
_message TEXT;
_detail TEXT;
_hint TEXT;
_context TEXT;
BEGIN
----------INSERT DATA

INSERT INTO ems_prd
SELECT
cad_incident_id,
TO_TIMESTAMP(TRIM(incident_datetime), 'MM/DD/YYYY HH24:MI:SS') as incident_datetime,
TRIM(initial_call_type) as initial_call_type,
CAST(RemovePunctuation(initial_severity_level_code) as INT) as initial_severity_level_code,
TRIM(final_call_type),
CAST(RemovePunctuation(final_severity_level_code) AS INT) AS final_severity_level_code,
TO_TIMESTAMP(TRIM(first_assignment_datetime), 'MM/DD/YYYY HH24:MI:SS') as first_assignment_datetime,
TRIM(valid_dispatch_rspns_time_indc) = 'true' as valid_dispatch_rspns_time_indc,
CAST(RemovePunctuation(dispatch_response_seconds_qy) AS INT) as dispatch_response_seconds_qy,
TO_TIMESTAMP(TRIM(first_activation_datetime), 'MM/DD/YYYY HH24:MI:SS') as first_activation_datetime,
TO_TIMESTAMP(TRIM(first_on_scene_datetime), 'MM/DD/YYYY HH24:MI:SS') as first_on_scene_datetime,
TRIM(valid_incident_rspns_time_indc) = 'true' as valid_incident_rspns_time_indc,
CAST(RemovePunctuation(incident_response_seconds_qy) AS INT) AS incident_response_seconds_qy,
CAST(RemovePunctuation(incident_travel_tm_seconds_qy) AS INT) AS incident_travel_tm_seconds_qy,
TO_TIMESTAMP(TRIM(first_to_hosp_datetime), 'MM/DD/YYYY HH24:MI:SS') as first_to_hosp_datetime,
TO_TIMESTAMP(TRIM(first_hosp_arrival_datetime), 'MM/DD/YYYY HH24:MI:SS') as first_hosp_arrival_datetime,
TO_TIMESTAMP(TRIM(incident_close_datetime), 'MM/DD/YYYY HH24:MI:SS') as incident_close_datetime,
TRIM(held_indicator) = 'true' as held_indicator,
CAST(RemovePunctuation(stage.incident_disposition_code) as INT) as incident_disposition_code,
TRIM(borough),
TRIM(incident_dispatch_area),
CAST(RemovePunctuation(zipcode) as int) as zipcode,
CAST(RemovePunctuation(policeprecinct) as INT) as policeprecinct,
CAST(RemovePunctuation(citycouncildistrict) AS INT) AS citycouncildistrict,
CAST(RemovePunctuation(communitydistrict) AS INT) AS communitydistrict,
CAST(RemovePunctuation(communityschooldistrict) AS INT) AS communityschooldistrict,
CAST(RemovePunctuation(congressionaldistrict) AS INT) AS congressionaldistrict,
TRIM(reopen_indicator) = 'true' as reopen_indicator,
TRIM(special_event_indicator) = 'true' as special_event_indicator,
TRIM(standby_indicator) = 'true' as standby_indicator,
TRIM(transfer_indicator) = 'true' as transfer_indicator,
TO_DATE(TRIM(incident_datetime), 'MM/DD/YYYY') as incident_date,
incident_disposition_code_description,
call_type_description as final_call_type_description,
call_type_description_big_category as final_call_type_description_big_category
FROM
ems_stage as stage 
left join incident_disposition as id ON stage.incident_disposition_code = id.incident_disposition_code
left join call_type_description as ctd ON stage.final_call_type = ctd.call_type;


---------------------
  EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
_sql_state := RETURNED_SQLSTATE,
_message := MESSAGE_TEXT,
_detail := PG_EXCEPTION_DETAIL,
_hint := PG_EXCEPTION_HINT,
_context := PG_EXCEPTION_CONTEXT;

INSERT INTO errors (the_date, sql_state, message, detail, hint, context)
VALUES (_the_date,_sql_state, _message, _detail, _hint, _context);
END
$BODY$
  LANGUAGE plpgsql;




---------function to send data from stage to prd
CREATE OR REPLACE FUNCTION insert_stage_to_prd()
RETURNS VOID AS
$BODY$
  DECLARE
_the_date timestamp without time zone NOT NULL
DEFAULT (current_timestamp AT TIME ZONE 'America/New_York');
_sql_state TEXT;
_message TEXT;
_detail TEXT;
_hint TEXT;
_context TEXT;
BEGIN
----------INSERT DATA
INSERT INTO ems_prd
SELECT
cad_incident_id,
TO_TIMESTAMP(TRIM(incident_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as incident_datetime,
TRIM(initial_call_type) as initial_call_type,
CAST(RemovePunctuation(initial_severity_level_code) as INT) as initial_severity_level_code,
TRIM(final_call_type),
CAST(RemovePunctuation(final_severity_level_code) AS INT) AS final_severity_level_code,
TO_TIMESTAMP(TRIM(first_assignment_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as first_assignment_datetime,
TRIM(valid_dispatch_rspns_time_indc) = 'true' as valid_dispatch_rspns_time_indc,
CAST(RemovePunctuation(dispatch_response_seconds_qy) AS INT) as dispatch_response_seconds_qy,
TO_TIMESTAMP(TRIM(first_activation_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as first_activation_datetime,
TO_TIMESTAMP(TRIM(first_on_scene_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as first_on_scene_datetime,
TRIM(valid_incident_rspns_time_indc) = 'true' as valid_incident_rspns_time_indc,
CAST(RemovePunctuation(incident_response_seconds_qy) AS INT) AS incident_response_seconds_qy,
CAST(RemovePunctuation(incident_travel_tm_seconds_qy) AS INT) AS incident_travel_tm_seconds_qy,
TO_TIMESTAMP(TRIM(first_to_hosp_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as first_to_hosp_datetime,
TO_TIMESTAMP(TRIM(first_hosp_arrival_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as first_hosp_arrival_datetime,
TO_TIMESTAMP(TRIM(incident_close_datetime), 'YYYY-MM-DD"T"HH24:MI:SS') as incident_close_datetime,
TRIM(held_indicator) = 'true' as held_indicator,
CAST(RemovePunctuation(incident_disposition_code) as INT) as incident_disposition_code,
TRIM(borough),
TRIM(incident_dispatch_area),
CAST(RemovePunctuation(zipcode) as int) as zipcode,
CAST(RemovePunctuation(policeprecinct) as INT) as policeprecinct,
CAST(RemovePunctuation(citycouncildistrict) AS INT) AS citycouncildistrict,
CAST(RemovePunctuation(communitydistrict) AS INT) AS communitydistrict,
CAST(RemovePunctuation(communityschooldistrict) AS INT) AS communityschooldistrict,
CAST(RemovePunctuation(congressionaldistrict) AS INT) AS congressionaldistrict,
TRIM(reopen_indicator) = 'true' as reopen_indicator,
TRIM(special_event_indicator) = 'true' as special_event_indicator,
TRIM(standby_indicator) = 'true' as standby_indicator,
TRIM(transfer_indicator) = 'true' as transfer_indicator,
TO_DATE(SUBSTRING(incident_datetime,1,10), 'YYYY-MM-DD') as incident_date
FROM
ems_stage ON CONFLICT(cad_incident_id) DO NOTHING;
---------------------
  EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
_sql_state := RETURNED_SQLSTATE,
_message := MESSAGE_TEXT,
_detail := PG_EXCEPTION_DETAIL,
_hint := PG_EXCEPTION_HINT,
_context := PG_EXCEPTION_CONTEXT;

INSERT INTO errors (the_date, sql_state, message, detail, hint, context)
VALUES (_the_date,_sql_state, _message, _detail, _hint, _context);
END
$BODY$
  LANGUAGE plpgsql;




-------------------------------------------------------aggregate table functions
-----------------daily borough metrics
CREATE OR REPLACE FUNCTION insert_daily_borough_metrics()
RETURNS VOID AS
$BODY$
  DECLARE
back_date timestamp := NOW() - interval '180 day';
_the_date timestamp without time zone NOT NULL
DEFAULT (current_timestamp AT TIME ZONE 'America/New_York');
_sql_state TEXT;
_message TEXT;
_detail TEXT;
_hint TEXT;
_context TEXT;

BEGIN

----------INSERT DATA
INSERT INTO daily_borough_metrics
SELECT
incident_date,
CASE WHEN TRIM(borough) IS NULL THEN 'UNKNOWN' ELSE borough END as borough,
CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END  as call_category,
ROUND(AVG(dispatch_response_seconds_qy) * 1.0/60,2) AS avg_dispatch_reponse_time_mins,
ROUND(AVG(incident_response_seconds_qy) *1.0/60,2) AS avg_incident_response_time_mins,
ROUND(AVG(incident_travel_tm_seconds_qy) *1.0/60,2) AS avg_incident_travel_time_mins,
ROUND(CAST(AVG(EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime))) * 1.0/60 AS NUMERIC),2) as avg_to_hosp_time_mins, 
count(*) as total_incidents
FROM
ems_prd
WHERE
EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime)) > 0 AND
incident_date >=  back_date ----'2019-06-01'
GROUP BY
incident_date,
CASE WHEN TRIM(borough) IS NULL THEN 'UNKNOWN' ELSE borough END,
CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END
ON CONFLICT DO NOTHING;

---------------------
  EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
_sql_state := RETURNED_SQLSTATE,
_message := MESSAGE_TEXT,
_detail := PG_EXCEPTION_DETAIL,
_hint := PG_EXCEPTION_HINT,
_context := PG_EXCEPTION_CONTEXT;

INSERT INTO errors (the_date, sql_state, message, detail, hint, context)
VALUES (_the_date,_sql_state, _message, _detail, _hint, _context);
END
$BODY$
  LANGUAGE plpgsql;





-----------------daily community metrics
CREATE OR REPLACE FUNCTION insert_daily_community_district_metrics()
RETURNS VOID AS
$BODY$
  DECLARE
back_date timestamp := NOW() - interval '180 day';
_the_date timestamp without time zone NOT NULL
DEFAULT (current_timestamp AT TIME ZONE 'America/New_York');
_sql_state TEXT;
_message TEXT;
_detail TEXT;
_hint TEXT;
_context TEXT;

BEGIN

----------INSERT DATA
INSERT INTO daily_community_district_metrics
SELECT
incident_date,
CASE WHEN communitydistrict IS NULL THEN 999 ELSE communitydistrict END as community_district,
CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END  as call_category,
ROUND(AVG(dispatch_response_seconds_qy) * 1.0/60,2) AS avg_dispatch_reponse_time_mins,
ROUND(AVG(incident_response_seconds_qy) *1.0/60,2) AS avg_incident_response_time_mins,
ROUND(AVG(incident_travel_tm_seconds_qy) *1.0/60,2) AS avg_incident_travel_time_mins,
ROUND(CAST(AVG(EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime))) * 1.0/60 AS NUMERIC),2) as avg_to_hosp_time_mins, 
count(*) as total_incidents
FROM
ems_prd
WHERE
EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime)) > 0
AND incident_date >=  back_date-------'2019-01-01' and incident_date < '2019-01-10'
GROUP BY
incident_date,
CASE WHEN communitydistrict IS NULL THEN 999 ELSE communitydistrict END,
CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END
ON CONFLICT DO NOTHING;

---------------------
  EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
_sql_state := RETURNED_SQLSTATE,
_message := MESSAGE_TEXT,
_detail := PG_EXCEPTION_DETAIL,
_hint := PG_EXCEPTION_HINT,
_context := PG_EXCEPTION_CONTEXT;

INSERT INTO errors (the_date, sql_state, message, detail, hint, context)
VALUES (_the_date,_sql_state, _message, _detail, _hint, _context);
END
$BODY$
  LANGUAGE plpgsql;


-------------------------Hourly metrics by weekday and borough


CREATE OR REPLACE FUNCTION weekday_hourly_borough_metrics()
RETURNS VOID AS
$BODY$
  DECLARE
_the_date timestamp without time zone NOT NULL
DEFAULT (current_timestamp AT TIME ZONE 'America/New_York');
_sql_state TEXT;
_message TEXT;
_detail TEXT;
_hint TEXT;
_context TEXT;

BEGIN

----------INSERT DATA
DROP TABLE IF EXISTS weekday_hourly_borough_metrics;
CREATE TABLE weekday_hourly_borough_metrics as
(
  SELECT
  CASE WHEN extract(dow from incident_date) = 0 THEN 'Sunday' 
  WHEN extract(dow from incident_date) = 1 THEN 'Monday' 
  WHEN extract(dow from incident_date) = 2 THEN 'Tuesday'
  WHEN extract(dow from incident_date) = 3 THEN 'Wednesday'
  WHEN extract(dow from incident_date) = 4 THEN 'Thursday'
  WHEN extract(dow from incident_date) = 5 THEN 'Friday' 
  WHEN extract(dow from incident_date) = 6 THEN 'Saturday' ELSE 'Unknown' END as weekday,
  extract(hour from incident_datetime) as hour,
  CASE WHEN TRIM(borough) IS NULL THEN 'UNKNOWN' ELSE borough END as borough,
  CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END  as call_category,
  ROUND(AVG(dispatch_response_seconds_qy) * 1.0/60,2) AS avg_dispatch_reponse_time_mins,
  ROUND(AVG(incident_response_seconds_qy) *1.0/60,2) AS avg_incident_response_time_mins,
  ROUND(AVG(incident_travel_tm_seconds_qy) *1.0/60,2) AS avg_incident_travel_time_mins,
  ROUND(CAST(AVG(EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime))) * 1.0/60 AS NUMERIC),2) as avg_to_hosp_time_mins, 
  count(*) as total_incidents
  FROM
  ems_prd
  WHERE
  EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime)) > 0
  -----AND incident_date >= '2019-01-01' and incident_date < '2019-01-10'
  GROUP BY
  CASE WHEN extract(dow from incident_date) = 0 THEN 'Sunday' 
  WHEN extract(dow from incident_date) = 1 THEN 'Monday' 
  WHEN extract(dow from incident_date) = 2 THEN 'Tuesday'
  WHEN extract(dow from incident_date) = 3 THEN 'Wednesday'
  WHEN extract(dow from incident_date) = 4 THEN 'Thursday'
  WHEN extract(dow from incident_date) = 5 THEN 'Friday' 
  WHEN extract(dow from incident_date) = 6 THEN 'Saturday' ELSE 'Unknown' END,
  extract(hour from incident_datetime),
  CASE WHEN TRIM(borough) IS NULL THEN 'UNKNOWN' ELSE borough END, 
  CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END
  ORDER BY weekday,borough,hour
);

---------------------
  EXCEPTION
WHEN OTHERS THEN
GET STACKED DIAGNOSTICS
_sql_state := RETURNED_SQLSTATE,
_message := MESSAGE_TEXT,
_detail := PG_EXCEPTION_DETAIL,
_hint := PG_EXCEPTION_HINT,
_context := PG_EXCEPTION_CONTEXT;

INSERT INTO errors (the_date, sql_state, message, detail, hint, context)
VALUES (_the_date,_sql_state, _message, _detail, _hint, _context);
END
$BODY$
  LANGUAGE plpgsql;










