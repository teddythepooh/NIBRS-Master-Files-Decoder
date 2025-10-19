SELECT
    CONCAT(ori, '-', incident_number) AS incident_id,
    TO_DATE(incident_date, 'YYYYMMDD') AS incident_date
FROM {{ source('raw', 'offense_segment') }}
