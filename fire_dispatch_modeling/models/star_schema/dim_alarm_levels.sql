select distinct
    -- Generate a unique key for each alarm record
    {{ dbt_utils.generate_surrogate_key([
        'ALARM_BOX_BOROUGH', 
        'ALARM_BOX_NUMBER', 
        'ALARM_BOX_LOCATION'
    ]) }} AS alarm_key,
    
    -- Ensure that fields are consistently formatted and handle potential nulls
    COALESCE(ALARM_BOX_BOROUGH, 'Unknown') AS alarm_box_borough, 
    COALESCE(ALARM_BOX_NUMBER, 'Unknown') AS alarm_box_number, 
    COALESCE(ALARM_BOX_LOCATION, -1) AS alarm_box_location, 
    COALESCE(ALARM_SOURCE_DESCRIPTION_TX, -1) AS alarm_source_description, 
    COALESCE(ALARM_LEVEL_INDEX_DESCRIPTION, -1) AS alarm_level_index_description, 
    COALESCE(HIGHEST_ALARM_LEVEL, -1) AS highest_alarm_level

from
    {{ var("source_schema") }}.biglake_dispatch_iceberg