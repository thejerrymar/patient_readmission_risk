{{config(
    materialized='view',
    schema='silver'
)}}

WITH raw AS (
    SELECT *
    FROM 
        {{source('bronze','raw_h239d_inpatient_info')}}
)

SELECT
    dupersid as patient_id,
    evntidx as checkin_eventid,
    ipbegyr as checkin_year,
    CASE 
        WHEN ipbegmm BETWEEN 1 AND 12 THEN ipbegmm
        ELSE 0
    END AS checkin_month,
    ipendyr as checkout_year,
    CASE 
        WHEN ipendmm BETWEEN 1 AND 12 THEN ipendmm
        ELSE 0
    END AS checkout_month,
    numnighx as total_nights_stayed
FROM
    raw
