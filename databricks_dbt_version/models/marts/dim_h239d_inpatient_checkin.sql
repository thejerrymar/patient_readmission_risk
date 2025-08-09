-- dim table will have total checkin counts
    -- just count the number of rows for checkin 
-- dim table will also have readmit flag 
    -- readmit flag is when a checkout out month matches the next checkin month

{{config(
    materialized='table',
    schema='gold'
)}}

WITH inpatient_stays AS (
    SELECT
        patient_id,
        checkin_eventid,
        checkin_year,
        checkin_month,
        checkout_year,
        checkout_month,
        total_nights_stayed,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id 
            ORDER BY checkin_eventid 
        ) AS checkin_counter
    FROM 
        {{ref('stg_h239d_inpatient_checkin')}}
),
next_checkin_info AS (
    SELECT
        *,
        LEAD(checkin_month) OVER(
            PARTITION BY patient_id ORDER BY checkin_eventid
        ) as next_checkin_month,
        LEAD(checkin_year) OVER(
            PARTITION BY patient_id ORDER BY checkin_eventid 
        ) as next_checkin_year
    FROM 
        inpatient_stays   
),
readmission_flags AS (
    SELECT
        *,
        CASE 
            WHEN checkout_month > 0 AND checkout_month = next_checkin_month AND checkout_year = next_checkin_year 
            THEN TRUE 
            ELSE FALSE 
        END as is_readmitted_same_month
    FROM 
        next_checkin_info
)

SELECT
    patient_id, 
    checkin_eventid,
    checkin_month,
    checkin_year,
    checkout_month,
    checkout_year,
    checkin_counter,
    is_readmitted_same_month
FROM 
    readmission_flags 
    





