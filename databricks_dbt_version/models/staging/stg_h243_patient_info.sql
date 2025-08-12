{{config(
    materialized='view',
    schema='silver'
)}}

SELECT
    DUPERSID as patient_id,
    AGE22X as patient_age,
    PERWT22F as person_weight_estimate,
    DOBMM as date_of_birth_month,
    DOBYY as date_of_birth_year,
    EDUCYR as years_of_education,
    REGION22 as country_region,
    EMPST53 as employment_status,
    POVCAT22 as poverty_category,
    iswhite as is_white,
    isblack as is_black,
    isamericanindian as is_american_indian,
    isasian as is_asian,
    ismultipleraces as is_multiple_races,
    ismale as is_male,
    unknownmarriageinfo as unknown_marriage_info,
    refusedmarriageinfo as refused_marriage_info,
    ismarried as is_married,
    iswidowed as is_widowed,
    isdivorced as is_divorced,
    isseparated as is_separated,
    nevermarried as is_never_married,
    livingwithpartner as is_living_with_partner
FROM 
    {{source('silver','raw_cleaned_h243_patient_info')}}