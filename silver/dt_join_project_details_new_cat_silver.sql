USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
USE SCHEMA SILVER;

CREATE OR REPLACE DYNAMIC TABLE SILVER.V_PD_WITH_NEW_CATEGORY
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = SHARVIL_UTP_DASHBOARD
  DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Add a subcategory attribute to project details.'
AS
SELECT
  p.*,
  /* fall back to the original category if no rule matched */
  COALESCE(m.new_category, p.funding_category) AS new_category,
  m.org_scope
FROM SILVER.PROJECT_DETAILS_FILTERED_SILVER p
LEFT JOIN REF.V_CATEGORY_MAP_CURRENT m
  ON m.category_parent = p.funding_category
 AND (
       /* exact matches */
       (m.work_program_exact IS NOT NULL AND m.work_program_exact = p.work_program_code)
    OR (m.pid_exact          IS NOT NULL AND m.pid_exact          = p.pid_code)
       /* regex matches */
    OR (m.work_program_regex IS NOT NULL AND REGEXP_LIKE(p.work_program_code, m.work_program_regex, 'i'))
    OR (m.pid_regex          IS NOT NULL AND REGEXP_LIKE(p.pid_code,          m.pid_regex,          'i'))
       /* default catch-all: no filters present on the rule */
    OR (m.work_program_exact IS NULL AND m.work_program_regex IS NULL
        AND m.pid_exact IS NULL AND m.pid_regex IS NULL)
     )
/* choose the single best rule per PD row */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY p.csj, p.funding_category, p.work_program_code, p.pid_code, p.funding_line_number
  ORDER BY
    IFF(m.work_program_exact IS NOT NULL OR m.pid_exact IS NOT NULL, 3,
        IFF(m.work_program_regex IS NOT NULL OR m.pid_regex IS NOT NULL, 2, 1)) DESC
) = 1;

-- SELECT * FROM SILVER.V_PD_WITH_NEW_CATEGORY;