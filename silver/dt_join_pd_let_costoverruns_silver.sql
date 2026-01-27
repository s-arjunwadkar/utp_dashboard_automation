USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE DYNAMIC TABLE SILVER.PD_LET_COSTOVERRUNS_JOINED
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = SHARVIL_UTP_DASHBOARD
  DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'This table joins project details file with LET data and Cost overruns to update the FY2026 amounts.'
AS
WITH pd_not_fy AS (
SELECT *,
authorized_amount AS orig_authorized_amount
FROM SILVER.PROJECT_DETAILS_FILTERED_SILVER
WHERE ESTIMATED_FISCAL_YEAR != 2026
),

pd_fy AS (
SELECT *,
authorized_amount AS orig_authorized_amount
FROM SILVER.PROJECT_DETAILS_FILTERED_SILVER
WHERE ESTIMATED_FISCAL_YEAR = 2026
),

pd_let_co_csj AS (
SELECT 
    pd.district_division_abbr,
    pd.district_division,
    pd.mpo_description,
    pd.funding_category,
    CASE 
        WHEN pd.csj = lc.csj THEN 0
        ELSE pd.authorized_amount
    END AS authorized_amount,
    pd.project_id,
    pd.csj,
    pd.ccsj,
    pd.estimated_fiscal_year,
    pd.responsible_district_name,
    pd.county,
    pd.highway,
    pd.project_description,
    pd.limits_from,
    pd.limits_to,
    pd.let_schedule_fiscal_year,
    pd.let_type_description,
    pd.waterfall_force_account_charge,
    pd.waterfall_incentives_disincentives_charge,
    pd.project_stage,
    pd.funding_line_number,
    pd.work_program_code,
    pd.pid_code,
    pd.funding_approval_status_description,
    pd.funding_group_name,
    pd.alternative_delivery,
    pd.orig_authorized_amount
FROM pd_fy AS pd
LEFT JOIN SILVER.LET_COSTOVERRUNS_JOINED_VIEW AS lc
ON pd.csj = lc.csj
),

pd_fy_updated AS (
SELECT 
    pd.district_division_abbr,
    pd.district_division,
    pd.mpo_description,
    pd.funding_category,
    CASE
        WHEN lc.new_total IS NOT NULL THEN lc.new_total
        ELSE pd.authorized_amount
    END AS authorized_amount,
    pd.project_id,
    pd.csj,
    pd.ccsj,
    pd.estimated_fiscal_year,
    pd.responsible_district_name,
    pd.county,
    pd.highway,
    pd.project_description,
    pd.limits_from,
    pd.limits_to,
    pd.let_schedule_fiscal_year,
    pd.let_type_description,
    pd.waterfall_force_account_charge,
    pd.waterfall_incentives_disincentives_charge,
    pd.project_stage,
    pd.funding_line_number,
    pd.work_program_code,
    pd.pid_code,
    pd.funding_approval_status_description,
    pd.funding_group_name,
    pd.alternative_delivery,
    pd.orig_authorized_amount
FROM pd_let_co_csj AS pd
LEFT JOIN SILVER.LET_COSTOVERRUNS_JOINED_VIEW AS lc
ON pd.csj = lc.csj
   AND pd.funding_line_number = lc.funding_line_number
   AND pd.funding_category = lc.category
   AND pd.work_program_code = lc.work_program
)

SELECT *
FROM pd_fy_updated
UNION ALL
SELECT *
FROM pd_not_fy;