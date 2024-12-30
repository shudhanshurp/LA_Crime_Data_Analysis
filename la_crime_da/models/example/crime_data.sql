{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT 
        division_number,
        date_reported,
        date_occurred,
        area,
        area_name,
        reporting_district,
        part,
        crime_code,
        crime_description,
        modus_operandi,
        victim_age,
        victim_sex,
        victim_descent,
        premise_code,
        premise_description,
        weapon_code,
        weapon_description,
        status,
        status_description,
        crime_code_1,
        location,
        latitude,
        longitude,
        report_delay,
        hour_occurred,
        day_of_week,
        age_group
    FROM read_parquet('s3://la-crime-data-sp/transformed-data/cleaned_data.parquet/*.parquet')
)

SELECT * FROM source_data