select distinct
    -- Generate a unique key for each alarm record
    {{ dbt_utils.generate_surrogate_key([
        'ALARM_BOX_BOROUGH', 
        'ALARM_BOX_NUMBER', 
        'ALARM_BOX_LOCATION'
    ]) }} AS alarm_key,
    
    -- Ensure that fields are consistently formatted and handle potential nulls
    COALESCE(ALARM_BOX_BOROUGH, 'none') AS alarm_box_borough, 
    COALESCE(ALARM_BOX_NUMBER, -1) AS alarm_box_number, 
    COALESCE(ALARM_BOX_LOCATION, 'none') AS alarm_box_location, 
    COALESCE(ALARM_SOURCE_DESCRIPTION_TX, 'none') AS alarm_source_description, 
    COALESCE(ALARM_LEVEL_INDEX_DESCRIPTION, 'none') AS alarm_level_index_description, 
    COALESCE(HIGHEST_ALARM_LEVEL, 'none') AS highest_alarm_level

from
    {{ var("source_schema") }}.fire_dispatch_serving_data