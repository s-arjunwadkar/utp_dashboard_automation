-- Materialize (copy) TxDOT shared data to TTI Snowflake
-- Project Stage Details
CREATE SCHEMA IF NOT EXISTS SHARVIL_UTP_2026_DASHBOARD.BRONZE;

-- Context (optional but handy)
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA BRONZE;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE DYNAMIC TABLE SHARVIL_UTP_2026_DASHBOARD.BRONZE.PROJECT_DETAILS_BRONZE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = SHARVIL_UTP_DASHBOARD
  DATA_RETENTION_TIME_IN_DAYS = 1
  COMMENT = 'Column-pruned mirror of TXDOT shared PROJECT_DETAILS; Excludes Cancelled projects and Estimated Fiscal Year Range is 2026-2035'
AS
SELECT
    /* Pin types where it matters so upstream type changes are caught */
    CAST("DISTRICT/DIVISION ABBREVIATION"            AS STRING)      AS district_division_abbr,
    CAST("DISTRICT/DIVISION"                         AS STRING)      AS district_division,
    CAST("MPO DESCRIPTION"                           AS STRING)      AS mpo_description,
    CAST("FUNDING CATEGORY"                          AS STRING)      AS funding_category,
    CAST("AUTHORIZED AMOUNT"                         AS FLOAT)       AS authorized_amount,
    CAST("PROJECT ID"                                AS STRING)      AS project_id,
    CAST("CONTROL SECTION JOB (CSJ)"                 AS STRING)      AS csj,
    CAST("CONTROLLING PROJECT ID (CCSJ)"             AS STRING)      AS ccsj,
    CAST("ESTIMATED FISCAL YEAR"                     AS NUMBER(4,0)) AS estimated_fiscal_year,
    CAST("RESPONSIBLE DISTRICT NAME"                 AS STRING)      AS responsible_district_name,
    CAST("COUNTY"                                    AS STRING)      AS county,
    CAST("HIGHWAY"                                   AS STRING)      AS highway,
    CAST("PROJECT DESCRIPTION"                       AS STRING)      AS project_description,
    CAST("LIMITS FROM"                               AS STRING)      AS limits_from,
    CAST("LIMITS TO"                                 AS STRING)      AS limits_to,
    CAST("LET SCHEDULE FISCAL YEAR"                  AS STRING)      AS let_schedule_fiscal_year,
    CAST("LET TYPE DESCRIPTION"                      AS STRING)      AS let_type_description,
    CAST("WATERFALL FORCE ACCOUNT CHARGE"            AS FLOAT)       AS waterfall_force_account_charge,
    CAST("WATERFALL INCENTIVES/DISINCENTIVES CHARGE" AS FLOAT)       AS waterfall_incentives_disincentives_charge,
    CAST("PROJECT STAGE"                             AS STRING)      AS project_stage,
    CAST("FUNDING LINE NUMBER"                       AS STRING)      AS funding_line_number,
    CAST("WORK PROGRAM CODE"                         AS STRING)      AS work_program_code,
    CAST("PID CODE"                                  AS STRING)      AS pid_code,
    CAST("FUNDING APPROVAL STATUS DESCRIPTION"       AS STRING)      AS funding_approval_status_description,
    CAST("FUNDING GROUP NAME"                        AS STRING)      AS funding_group_name,
    CAST("ALTERNATIVE DELIVERY"                      AS STRING)      AS alternative_delivery
FROM TXDOT_DM_MP_TTI.BAL_PROJECT.PROJECT_DETAILS
WHERE COALESCE(UPPER(TRIM("PROJECT STAGE")), '') NOT IN ('CANCELLED','CANCELED')
AND "ESTIMATED FISCAL YEAR" BETWEEN 2026 AND 2035;
/*
  AND "ESTIMATED FISCAL YEAR" BETWEEN
        IFF(MONTH(CURRENT_DATE()) >= 9, YEAR(CURRENT_DATE()) + 1, YEAR(CURRENT_DATE()))
    AND IFF(MONTH(CURRENT_DATE()) >= 9, YEAR(CURRENT_DATE()) + 10, YEAR(CURRENT_DATE()) + 9);
*/    

--SELECT * FROM SHARVIL_UTP_2026_DASHBOARD.BRONZE.PROJECT_DETAILS_BRONZE;