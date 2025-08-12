{{config(
    materialized='view',
    schema='silver'
)}}

SELECT 
    DUPERSID as patient_id,
    INSCOV22 as insurance_coverage_status,
    INSURC22 as insurace_coverage_type,
    hasmedicare as has_medicare,
    hasmedicaid as has_medicaid,
    hastricare as has_tricare,
    hasprivateinsurance as has_private_insurance
FROM 
    {{source('silver','raw_cleaned_h243_patient_info')}}

