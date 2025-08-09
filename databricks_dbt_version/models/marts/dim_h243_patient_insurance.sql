{{config(
    materialized='table',
    schema='gold'
)}}

SELECT 
    patient_id,
    has_medicare,
    has_medicaid,
    has_tricare,
    has_private_insurance
FROM 
    {{ref('stg_h243_patient_insurance')}}