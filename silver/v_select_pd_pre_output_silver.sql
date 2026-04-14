USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;

CREATE OR REPLACE VIEW SILVER.V_PD_PRE_OUTPUT
COMMENT = 'This view creates a new attribute district_mpo_division and then selects relevant attributes for the pre output layer of project details.'
AS
WITH pd_main AS (
SELECT
    parent_category, 
    category, 
    district_division_abbr,
    district_division,
    mpo_short,
    CASE
        WHEN LOWER(org_scope) = 'district' THEN CONCAT(district_division_abbr, ' - ', district_division)
        WHEN LOWER(org_scope) = 'mpo' AND mpo_short IS NOT NULL THEN CONCAT(district_division_abbr, ' - ', mpo_short)
        WHEN category = '6' THEN 'Bridge Division'
        WHEN category = '8' THEN 'Traffic Division'
        WHEN category = '9' AND (work_program_code ILIKE '%FX' OR pid_code IN ('BRA', 'TE', 'SRS')) THEN 'Transportation Alternatives Flex Program'
        WHEN category = '9' AND work_program_code ILIKE '%JA' THEN 'Transportation Alternatives Flex IIJA Program'
        WHEN category = '9' AND work_program_code ILIKE '%TP' AND (pid_code != 'TM' OR pid_code IS NULL) THEN 'Transportation Alternatives Program - Non-TMAs'
        WHEN category = '10' THEN 'Supplemental Transportation Projects'
        WHEN category = '10CR' AND work_program_code = '10CBNS' THEN 'Carbon Reduction Program - Statewide'
        WHEN category = '11' AND work_program_code = '16B11' THEN 'Rider 11B Program'
        WHEN category = '11' AND work_program_code = 'COCO' THEN 'Cost Overruns/Change Orders'
        ELSE district_division
    END AS district_mpo_division,
    csj,
    estimated_fiscal_year,
    county,
    highway,
    limits_from,
    limits_to,
    project_description,
    let_type_description,
    let_schedule_fiscal_year,
    authorized_amount,
    orig_authorized_amount,
    org_scope,
    alternative_delivery
FROM SILVER.PD_MPO_SHORT
WHERE NOT ((org_scope = 'MPO' AND mpo_description IS NULL) OR org_scope IS NULL) -- Exclude records with missing MPO description for MPO org scope and records with null org scope
),

pd_final AS (
    SELECT
        *,
        CASE
        WHEN district_mpo_division IN ('BMT - HGAC MPO', 'HOU - HGAC MPO')
          THEN 'HOU/BMT - HGAC MPO'
        WHEN district_mpo_division IN ('DAL - NCTCOG MPO', 'FTW - NCTCOG MPO', 'PAR - NCTCOG MPO')
          THEN 'DAL/FTW/PAR - NCTCOG MPO'
        ELSE district_mpo_division
      END AS display_name
    FROM pd_main
)

SELECT 
    display_name AS district_mpo_division,
    district_division AS district,
    csj,
    county,
    highway,
    limits_from,
    limits_to,
    project_description,
    category, 
    estimated_fiscal_year AS fy,
    let_type_description,
    let_schedule_fiscal_year AS let_sch_fy,
    authorized_amount,
    orig_authorized_amount,
    CASE
        WHEN LOWER(TRIM(alternative_delivery)) IN ('yes', 'potential') THEN 'DB'
        ELSE 'DBB'
    END AS db_or_dbb
FROM pd_final
ORDER BY district_mpo_division, district, csj, category, fy;