
-- "What is the average response time from the 
-- dispatch point to the on-scene arrival across different 
-- geographies and timeframes?"

select 
    {{ dbt_utils.generate_surrogate_key([
        'STARFIRE_INCIDENT_ID', 
        'INCIDENT_DATETIME'
    ]) }} AS fact_response_time_key,

    {{ dbt_utils.generate_surrogate_key([
        'INCIDENT_BOROUGH', 
        'ZIPCODE', 
        'POLICEPRECINCT', 
        'CITYCOUNCILDISTRICT', 
        'COMMUNITYDISTRICT', 
        'COMMUNITYSCHOOLDISTRICT', 
        'CONGRESSIONALDISTRICT'
    ]) }} AS location_key,

    {{ dbt_utils.generate_surrogate_key([
        'ALARM_BOX_BOROUGH', 
        'ALARM_BOX_NUMBER', 
        'ALARM_BOX_LOCATION'
    ]) }} AS alarm_key,

    INCIDENT_DATETIME as incident_datetime,
    FIRST_ASSIGNMENT_DATETIME as first_assignment_datetime,
    FIRST_ON_SCENE_DATETIME as first_on_scene_datetime
from 
    {{ var("source_schema") }}.fire_dispatch_serving_data
    