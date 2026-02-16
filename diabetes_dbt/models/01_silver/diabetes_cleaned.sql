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
        patient_nbr,
        race,
        gender,
        age,
        admission_type_id,
        discharge_disposition_id,
        admission_source_id,
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
        change,
        diabetesMed,
        readmitted,
        CASE
            WHEN readmitted = '<30' THEN 1
            ELSE 0
        END AS readmitted_30_days
    FROM source
    WHERE discharge_disposition_id NOT IN (11, 13, 14, 18, 19, 20, 21)
    AND (diag_1 != '?' OR diag_2 != '?' OR diag_3 != '?')
    AND gender != 'Unknown/Invalid'
    AND race != '?'
)
SELECT *
FROM cleaned