select distinct
    -- Generate a unique key for each location record based on multiple geographic attributes
    {{ dbt_utils.generate_surrogate_key([
        'INCIDENT_BOROUGH', 
        'ZIPCODE', 
        'POLICEPRECINCT', 
        'CITYCOUNCILDISTRICT', 
        'COMMUNITYDISTRICT', 
        'COMMUNITYSCHOOLDISTRICT', 
        'CONGRESSIONALDISTRICT'
    ]) }} AS location_key,
    
    -- Ensure that fields are consistently formatted and handle potential nulls
    COALESCE(INCIDENT_BOROUGH, 'Unknown') AS incident_borough, 
    COALESCE(ZIPCODE, 'Unknown') AS zipcode, 
    COALESCE(POLICEPRECINCT, -1) AS police_precinct, 
    COALESCE(CITYCOUNCILDISTRICT, -1) AS city_council_district, 
    COALESCE(COMMUNITYDISTRICT, -1) AS community_district, 
    COALESCE(COMMUNITYSCHOOLDISTRICT, -1) AS community_school_district, 
    COALESCE(CONGRESSIONALDISTRICT, -1) AS congressional_district

from
    {{ var("source_schema") }}.biglake_dispatch_iceberg
