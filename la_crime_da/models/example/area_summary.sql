{{ config(materialized='table') }}

WITH base AS (
    SELECT
        area_name,
        division_number,
        latitude,
        longitude
    FROM {{ ref('crime_data') }} 
    GROUP BY area_name, division_number, latitude, longitude
)

SELECT
    area_name,
    COUNT(division_number) AS CrimeCount, 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latitude) AS MedianLatitude,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY longitude) AS MedianLongitude
FROM base
GROUP BY area_name
