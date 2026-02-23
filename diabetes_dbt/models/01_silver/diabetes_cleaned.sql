{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('diabetes_raw') }}
),
cleaned AS (
    SELECT
        encounter_id,
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
        CASE
            WHEN discharge_disposition_id IN (1, 6, 8) THEN 'Home'
            WHEN discharge_disposition_id IN (2, 3, 4, 5, 22, 23, 24, 27, 28, 29, 30) THEN 'Transfer'
            ELSE 'Other'
        END AS discharge_disposition,
        CASE
            WHEN admission_source_id IN (7) THEN 'Emergency'
            WHEN admission_source_id IN (1, 2, 3) THEN 'Referral'
            WHEN admission_source_id IN (4, 5, 6, 10, 22, 25) THEN 'Transfer'
        END AS admission_source,
        time_in_hospital,
        num_lab_procedures,
        num_procedures,
        num_medications,
        number_outpatient,
        number_emergency,
        number_inpatient,
        diag_1,
        diag_2,
        diag_3,
        number_diagnoses,
        max_glu_serum,
        A1Cresult,
        metformin,
        repaglinide,
        nateglinide,
        chlorpropamide,
        glimepiride,
        acetohexamide,
        glipizide,
        glyburide,
        tolbutamide,
        pioglitazone,
        rosiglitazone,
        acarbose,
        miglitol,
        troglitazone,
        tolazamide,
        insulin,
        glyburide_metformin,
        glipizide_metformin,
        glimepiride_pioglitazone,
        metformin_rosiglitazone,
        metformin_pioglitazone,
        CASE
            WHEN change = 'Yes' THEN 1
            ELSE 0
        END AS medication_changed,
        diabetesMed,
        readmitted,
        CASE
            WHEN readmitted = '<30' THEN 1
            ELSE 0
        END AS readmitted_30_days
    FROM source
    WHERE discharge_disposition_id NOT IN (11, 13, 14, 18, 19, 20, 21, 25, 26)
        AND admission_source_id NOT IN (8, 9, 11, 12, 13, 14, 15, 17, 20, 21, 23, 24, 26)
        AND (diag_1 != '?' OR diag_2 != '?' OR diag_3 != '?')
        AND gender != 'Unknown/Invalid'
        AND race != '?'
)
SELECT *
FROM cleaned