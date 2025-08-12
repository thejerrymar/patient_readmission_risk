{{config(
    materialized='view',
    schema='silver'
)}}

WITH raw AS (
    SELECT *
    FROM 
        {{source('bronze','raw_h239d_inpatient')}}
)

SELECT
    DUPERSID as patient_id,
    EVNTIDX as checkin_eventid,
    IPBEGYR as checkin_year,
    CASE 
        WHEN IPBEGMM BETWEEN 1 AND 12 THEN IPBEGMM
        ELSE 0
    END AS checkin_month,
    IPENDYR as checkout_year,
    CASE 
        WHEN IPENDMM BETWEEN 1 AND 12 THEN IPENDMM
        ELSE 0
    END AS checkout_month,
    NUMNIGHX as total_nights_stayed
FROM
    raw
