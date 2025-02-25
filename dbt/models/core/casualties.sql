WITH casualties_data AS (
    SELECT 
        accident_id,
        class AS casualty_class,
        severity AS casualty_severity,
        mode AS casualty_type,
        age_band AS age_band_of_casualty,
        NULL AS sex_of_casualty  -- Assuming sex_of_casualty is not available in the source data
    FROM {{ ref('stg_casualties') }}
)

SELECT * FROM casualties_data