{{config(
  materialized='view',
  schema='silver'
)}}

SELECT 
    dupersid as patient_id,
    inscov22 as insurance_coverage_status,
    insurc22 as insurace_coverage_type,
    CASE
        WHEN hasmedicare IS TRUE THEN 1 ELSE 0 
        END 
    AS has_medicare,
    CASE
        WHEN hasmedicaid IS TRUE THEN 1 ELSE 0
        END
    AS has_medicaid,
    CASE 
        WHEN hastricare IS TRUE THEN 1 ELSE 0
        END
    AS has_tricare,
    CASE 
        WHEN hasprivateinsurance IS TRUE THEN 1 ELSE 0
        END
    AS has_private_insurance
FROM 
    {{source('bronze','raw_h243_patient_insurance_info')}}