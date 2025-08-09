{{config(
    materialized='table',
    schema='gold'
)}}


-- need to determine if patient was readmitted or not - use inpatient check in 
WITH readmission_check AS (
    SELECT 
        patient_id,
        BOOL_OR(is_readmitted_same_month) as is_readmitted,
        MAX(checkin_counter) as checkin_count
    FROM 
        {{ref('dim_h239d_inpatient_checkin')}}
    GROUP BY 
        patient_id 
)



SELECT 
    patient_info.patient_id,
    CASE 
        WHEN readmission_check.is_readmitted IS TRUE THEN 1 ELSE 0 
        END 
    AS is_readmitted,
    readmission_check.checkin_count,
    {{dbt_utils.star(from=(ref('dim_h243_patient_info')), except=["patient_id"])}}, -- bring in all columns except patient_id
    {{dbt_utils.star(from=(ref('dim_h243_patient_insurance')), except=["patient_id"])}}, -- bring in all columns except patient_id
    {{dbt_utils.star(from=(ref('dim_h241_medical_conditions')), except=["patient_id"])}} -- bring in all columns except patient_id
FROM 
    {{ref('dim_h243_patient_info')}} as patient_info 
JOIN 
    readmission_check
ON 
    patient_info.patient_id = readmission_check.patient_id
JOIN 
    {{ref('dim_h241_medical_conditions')}} medical_conditions 
ON 
    patient_info.patient_id = medical_conditions.patient_id 
JOIN 
    {{ref('dim_h243_patient_insurance')}} as insurance 
ON 
    insurance.patient_id = patient_info.patient_id 