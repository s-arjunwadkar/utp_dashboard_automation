USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE VIEW SILVER.PD_TOTAL_PROGRAMMED
COMMENT = 'This view creates a new attribute district_mpo_division and then calculates total programmed amount per category per district_mpo_division per estimated_fiscal_year.'
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
    estimated_fiscal_year, 
    authorized_amount,
    org_scope
FROM SILVER.PD_MPO_SHORT
WHERE NOT (category = '11' AND work_program_code = '2910GR') -- Should be CAT 10/ Fix the Work Program (BY TxDOT)
      OR (org_scope = 'MPO' AND mpo_description IS NULL) -- Exclude records with missing MPO description for MPO org scope
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
    parent_category,
    category,
    display_name AS district_mpo_division,
    estimated_fiscal_year,
    SUM(authorized_amount) AS total_authorized_amount
FROM pd_final
GROUP BY parent_category, category, display_name, estimated_fiscal_year
ORDER BY category, district_mpo_division, estimated_fiscal_year
;

-- SELECT * FROM SILVER.PD_TOTAL_PROGRAMMED;