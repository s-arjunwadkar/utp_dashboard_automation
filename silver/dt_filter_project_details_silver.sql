CREATE SCHEMA IF NOT EXISTS UTP_DASHBOARD.SILVER;

-- Context (optional but handy)
USE DATABASE UTP_DASHBOARD;
USE SCHEMA SILVER;
USE WAREHOUSE UTP_DASHBOARD_WH;

CREATE OR REPLACE DYNAMIC TABLE UTP_DASHBOARD.SILVER.PROJECT_DETAILS_FILTERED_SILVER
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = UTP_DASHBOARD_WH
  DATA_RETENTION_TIME_IN_DAYS = 1
  COMMENT = 'Mirror of PROJECT_DETAILS_BRONZE; keeps only rows where Funding Group Name = Construction and Funding Approval Status = Approved and Funding Category != CAN, PPD & RL. Also handle cases where district_division has division names. In such cases, we will use the responsible district name as the district division.'
AS
WITH unique_districts AS (
SELECT DISTINCT
    district_division_abbr,
    responsible_district_name AS district
FROM BRONZE.PROJECT_DETAILS_BRONZE
WHERE responsible_district_name IS NOT NULL AND district_division = responsible_district_name
),

district_updated AS (
SELECT 
    pd.*,
    CASE
        WHEN ud.district IS NULL THEN pd.responsible_district_name
        ELSE pd.district_division
    END AS district_division_new
FROM BRONZE.PROJECT_DETAILS_BRONZE AS pd
LEFT JOIN unique_districts AS ud
ON pd.district_division = ud.district
WHERE COALESCE(TRIM(pd.funding_group_name), '') ILIKE 'Construction'
  AND COALESCE(TRIM(pd.funding_approval_status_description), '') ILIKE 'Approved'
  AND COALESCE(UPPER(TRIM(pd.funding_category)), '') NOT IN ('CAN', 'PPD', 'RL')
)

SELECT
  CASE
    WHEN (ud.district = pd.district_division_new) AND (ud.district_division_abbr != pd.district_division_abbr) THEN ud.district_division_abbr
    ELSE pd.district_division_abbr
  END AS district_division_abbr,
  pd.district_division_new AS district_division,
  pd.mpo_description,
  pd.funding_category,
  pd.authorized_amount,
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
  pd.alternative_delivery
FROM district_updated AS pd
LEFT JOIN unique_districts AS ud
ON pd.district_division_new = ud.district
;