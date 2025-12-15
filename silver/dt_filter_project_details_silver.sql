CREATE SCHEMA IF NOT EXISTS SHARVIL_UTP_2026_DASHBOARD.SILVER;

-- Context (optional but handy)
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA SILVER;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE DYNAMIC TABLE SHARVIL_UTP_2026_DASHBOARD.SILVER.PROJECT_DETAILS_FILTERED_SILVER
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = SHARVIL_UTP_DASHBOARD
  DATA_RETENTION_TIME_IN_DAYS = 1
  COMMENT = 'Mirror of PROJECT_DETAILS_BRONZE; keeps only rows where Funding Group Name = Construction and Funding Approval Status = Approved and Funding Category != CAN, PPD & RL.'
AS
SELECT
  district_division_abbr,
  district_division,
  mpo_description,
  funding_category,
  authorized_amount,
  project_id,
  csj,
  ccsj,
  estimated_fiscal_year,
  responsible_district_name,
  county,
  highway,
  project_description,
  limits_from,
  limits_to,
  let_schedule_fiscal_year,
  let_type_description,
  waterfall_force_account_charge,
  waterfall_incentives_disincentives_charge,
  project_stage,
  funding_line_number,
  work_program_code,
  pid_code,
  funding_approval_status_description,
  funding_group_name,
  alternative_delivery
FROM SHARVIL_UTP_2026_DASHBOARD.BRONZE.PROJECT_DETAILS_BRONZE
WHERE COALESCE(TRIM(funding_group_name), '') ILIKE 'Construction'
  AND COALESCE(TRIM(funding_approval_status_description), '') ILIKE 'Approved'
  AND COALESCE(UPPER(TRIM(funding_category)), '') NOT IN ('CAN', 'PPD', 'RL');

-- quick validation
--SELECT * FROM SHARVIL_UTP_2026_DASHBOARD.SILVER.PROJECT_DETAILS_FILTERED_SILVER;