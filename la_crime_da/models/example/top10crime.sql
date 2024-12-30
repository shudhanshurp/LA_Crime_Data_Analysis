{{ config(materialized='table') }}

WITH crime_counts AS (
    SELECT
        crime_code,
        COUNT(*) AS CrimeCount
    FROM {{ ref('crime_data') }}
    GROUP BY crime_code
)

SELECT
    crime_code,
    CrimeCount
FROM crime_counts
ORDER BY CrimeCount DESC
LIMIT 10
