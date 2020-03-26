-------------------------------------------------------aggregate tables initial insert
------after schema and functions are initialzed
------this runs to populate aggregate tables with existing data


------------------------------------daily metrics by borough, call type
----DROP TABLE daily_borough_metrics;
CREATE OR REPLACE FUNCTION create_daily_borough_metrics()
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
CREATE TABLE daily_borough_metrics as
(
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
  EXTRACT(EPOCH FROM (first_hosp_arrival_datetime - first_on_scene_datetime)) > 0
  ----incident_date >= '2019-01-01' and incident_date < '2019-01-10'
  GROUP BY
  incident_date,
  CASE WHEN TRIM(borough) IS NULL THEN 'UNKNOWN' ELSE borough END,
  CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END
  ORDER BY incident_date,call_category);
-------add primary key
ALTER TABLE daily_borough_metrics ADD PRIMARY KEY (incident_date, borough, call_category);
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



----------------------------------------daily metrics by community district, call type
----DROP TABLE daily_community_district_metrics;
CREATE OR REPLACE FUNCTION create_daily_community_district_metrics()
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
CREATE TABLE daily_community_district_metrics as
(
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
  ----incident_date >= '2019-01-01' and incident_date < '2019-01-10'
  GROUP BY
  incident_date,
  CASE WHEN communitydistrict IS NULL THEN 999 ELSE communitydistrict END,
  CASE WHEN TRIM(final_call_type_description_big_category) IS NULL THEN 'OTHER' ELSE final_call_type_description_big_category END
  ORDER BY incident_date,call_category);
-------add primary key
ALTER TABLE daily_community_district_metrics ADD PRIMARY KEY (incident_date, community_district, call_category);
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



-----------------------------Hourly metrics by weekday and borough
---DROP TABLE weekday_hourly_borough_metrics;
CREATE OR REPLACE FUNCTION create_weekday_hourly_borough_metrics()
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