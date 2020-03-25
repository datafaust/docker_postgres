-----------------------create user
CREATE USER docker;
CREATE DATABASE docker;
GRANT ALL PRIVILEGES ON DATABASE docker TO docker;

-----------------------create_table
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



