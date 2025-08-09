{{config(
  materialized='view',
  schema='silver'
)}}

SELECT
    dupersid as patient_id,
    age22x as patient_age,
    perwt22f as person_weight_estimate,
    dobmm as date_of_birth_month,
    dobyy as date_of_birth_year,
    educyr as years_of_education,
    region22 as country_region,
    empst53 as employment_status,
    povcat22 as poverty_category,
    CASE WHEN iswhite IS TRUE THEN 1 ELSE 0 END AS is_white,
    CASE WHEN isblack IS TRUE THEN 1 ELSE 0 END AS is_black,
    CASE WHEN isamericanindian IS TRUE THEN 1 ELSE 0 END AS is_american_indian,
    CASE WHEN isasian IS TRUE THEN 1 ELSE 0 END AS is_asian,
    CASE WHEN ismultipleraces IS TRUE THEN 1 ELSE 0 END AS is_multiple_races,
    CASE WHEN ismale IS TRUE THEN 1 ELSE 0 END AS is_male,
    CASE WHEN unknownmarriageinfo IS TRUE THEN 1 ELSE 0 END AS unknown_marriage_info,
    CASE WHEN refusedmarriageinfo IS TRUE THEN 1 ELSE 0 END AS refused_marriage_info,
    CASE WHEN ismarried IS TRUE THEN 1 ELSE 0 END AS is_married,
    CASE WHEN iswidowed IS TRUE THEN 1 ELSE 0 END AS is_widowed,
    CASE WHEN isdivorced IS TRUE THEN 1 ELSE 0 END AS is_divorced,
    CASE WHEN isseparated IS TRUE THEN 1 ELSE 0 END AS is_separated,
    CASE WHEN nevermarried IS TRUE THEN 1 ELSE 0 END AS is_never_married,
    CASE WHEN livingwithpartner IS TRUE THEN 1 ELSE 0 END AS is_living_with_partner
FROM 
    {{source('bronze','raw_h243_patient_info')}}