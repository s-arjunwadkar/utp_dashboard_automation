USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE DYNAMIC TABLE SILVER.PD_MPO_SHORT
  TARGET_LAG = '1 hour'
  WAREHOUSE  = SHARVIL_UTP_DASHBOARD
  DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'This table brings mpo short forms from mpo reference table. However, before doing that the South East Texas Regional Planning Commission which is coming from the source has special character which is not explicitly visible without generating HEX ENCODE. First that was handled.'
AS
WITH pd_mpo_desc_correction AS (
SELECT
      funding_category,
      new_category,
      district_division_abbr,
      district_division,
      CASE
        WHEN mpo_description ILIKE '%south%' THEN 'South East Texas Regional Planning Commission'
        ELSE mpo_description
      END AS mpo_description,
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
      alternative_delivery,
      org_scope
FROM SILVER.V_PD_WITH_NEW_CATEGORY
)

SELECT
      pd.funding_category AS parent_category,
      pd.new_category AS category,
      pd.district_division_abbr,
      pd.district_division,
      pd.mpo_description,
      mpo.mpo_short,
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
      pd.alternative_delivery,
      pd.org_scope
FROM pd_mpo_desc_correction AS pd
LEFT JOIN REF.MPO_REFERENCE AS mpo
ON TRIM(LOWER(pd.mpo_description)) = TRIM(LOWER(mpo.mpo_description))
;

-- SELECT * FROM SILVER.PD_MPO_SHORT;