{{ config(
    materialized='table',
    schema='gold'
) }}

WITH silver AS (
    SELECT *
    FROM {{ ref('diabetes_cleaned') }}
),
features AS (
    SELECT
        patient_nbr AS patient_id,
        race,
        gender,
        age,
        CASE 
            WHEN admission_type_id = 1 THEN 'Emergency'
            WHEN admission_type_id = 2 THEN 'Urgent'
            WHEN admission_type_id = 3 THEN 'Elective'
            WHEN admission_type_id = 4 THEN 'Newborn'
            WHEN admission_type_id = 7 THEN 'Trauma Center'
            WHEN (admission_type_id = 5 
                OR admission_type_id = 6
                OR admission_type_id = 8) 
            THEN 'Not Available'
            ELSE 'Other'
        END AS admission_type,
        discharge_disposition_id,
        admission_source_id,
        time_in_hospital,
        num_lab_procedures,
        num_procedures,
        num_medications,
        (number_outpatient + number_emergency + number_inpatient) AS total_visits,
        (num_lab_procedures / time_in_hospital) AS lab_procedures_per_day,
        (num_medications / time_in_hospital) AS medications_per_day,
        CASE 
            WHEN age IN ('[70-80)', '[80-90)', '[90-100)')
                AND (number_inpatient + number_emergency + number_inpatient) >= 3
            THEN 1
            ELSE 0
        END AS elderly_frequent_visits,
        CASE 
            WHEN admission_type_id = 1 AND time_in_hospital <= 3
            THEN 1
            ELSE 0
        END AS emergency_short_stay,
        CASE 
            WHEN insulin IN ('Up', 'Down', 'Steady') THEN 1
            ELSE 0  
        END AS on_insulin,
        CASE
        (CASE WHEN metformin IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN repaglinide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN nateglinide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN chlorpropamide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glimepiride IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN acetohexamide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glipizide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glyburide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN tolbutamide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN pioglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN rosiglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN acarbose IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN miglitol IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN troglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN tolazamide IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN insulin IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glyburide_metformin IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glipizide_metformin IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN glimepiride_pioglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN metformin_rosiglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END +
        CASE WHEN metformin_pioglitazone IN ('Up', 'Down', 'Steady') THEN 1 ELSE 0 END) 
        AS num_diabetes_drugs,
        CASE WHEN change = 'Yes' THEN 1 ELSE 0 END AS medication_changed,
        readmitted_30_days
    FROM silver
)