{{config(
    materialized='table',
    schema='gold'
)}}

SELECT
    *
FROM
    {{ref('stg_h241_medical_conditions')}}