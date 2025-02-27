
{{
    config(
        on_configuration_change = 'apply',
        pre_hook = [
        "DROP MATERIALIZED VIEW IF EXISTS `fdny-data-analysis.fdny_data_analysis_star_schema.avg_response_time_from_dispatch_to_arrival_view`;"
        ]
    )
}}

SELECT
    dd.year_number,
    dd.month_of_year,
    dd.day_of_week,
    dl.incident_borough,
    dl.zipcode,
    dl.police_precinct,
    da.alarm_level_index_description,
    AVG(TIMESTAMP_DIFF(frt.first_on_scene_datetime, frt.first_assignment_datetime, SECOND)) AS avg_response_time_seconds
FROM
    {{ var("star_schema") }}.fact_response_times frt
JOIN
    {{ var("star_schema") }}.dim_dates dd ON DATE(frt.incident_datetime) = dd.date_day
JOIN
    {{ var("star_schema") }}.dim_locations dl ON frt.location_key = dl.location_key
JOIN
    {{ var("star_schema") }}.dim_alarm_levels da ON frt.alarm_key = da.alarm_key
GROUP BY
    dd.year_number,
    dd.month_of_year,
    dd.day_of_week,
    dl.incident_borough,
    dl.zipcode,
    dl.police_precinct,
    da.alarm_level_index_description;