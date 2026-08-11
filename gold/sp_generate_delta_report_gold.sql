USE ROLE SYSADMIN;
USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;
USE SCHEMA GOLD;
CREATE OR REPLACE PROCEDURE GOLD.SP_DELTA_REPORT(p_advance_baseline BOOLEAN)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
   v_current_row_count NUMBER;
   v_previous_row_count NUMBER;
   v_delta_row_count NUMBER;
   v_nonzero_delta_count NUMBER;
   v_baseline_status STRING DEFAULT 'NOT_ADVANCED';
BEGIN
   ----------------------------------------------------------------------
   -- Step 1: Capture row counts before comparison
   ----------------------------------------------------------------------
   SELECT COUNT(*)
   INTO :v_current_row_count
   FROM UTP_DASHBOARD.GOLD.V_CSJ_LEVEL_OUTPUT_1_GOLD;
   SELECT COUNT(*)
   INTO :v_previous_row_count
   FROM UTP_DASHBOARD.GOLD.CSJ_LEVEL_OUTPUT_1_PREVIOUS;

   ----------------------------------------------------------------------
   -- Step 2: Create current delta report as a TABLE
   --
   -- Comparison grain:
   -- CSJ + DISTRICT_MPO_DIVISION + CATEGORY + FY
   ----------------------------------------------------------------------
   CREATE OR REPLACE TABLE UTP_DASHBOARD.GOLD.T_DELTA_REPORT_OUTPUT_GOLD AS
   WITH today_results AS (
       SELECT
           TRIM(csj) AS csj,
           TRIM(district_mpo_division) AS district_mpo_division,
           TRIM(category) AS category,
           fy::INTEGER AS fy,
           ROUND(SUM(COALESCE(authorized_amount, 0)), 2) AS authorized_amount
       FROM UTP_DASHBOARD.GOLD.V_CSJ_LEVEL_OUTPUT_1_GOLD
       GROUP BY
           TRIM(csj),
           TRIM(district_mpo_division),
           TRIM(category),
           fy::INTEGER
   ),
   yesterday_results AS (
       SELECT
           TRIM(csj) AS csj,
           TRIM(district_mpo_division) AS district_mpo_division,
           TRIM(category) AS category,
           fy::INTEGER AS fy,
           ROUND(SUM(COALESCE(authorized_amount, 0)), 2) AS authorized_amount_yesterday
       FROM UTP_DASHBOARD.GOLD.CSJ_LEVEL_OUTPUT_1_PREVIOUS
       GROUP BY
           TRIM(csj),
           TRIM(district_mpo_division),
           TRIM(category),
           fy::INTEGER
   )
   SELECT
       COALESCE(t.csj, y.csj) AS csj,
       COALESCE(t.district_mpo_division, y.district_mpo_division) AS district_mpo_division,
       COALESCE(t.category, y.category) AS category,
       COALESCE(t.fy, y.fy) AS fy,
       t.authorized_amount AS authorized_amount,
       y.authorized_amount_yesterday AS authorized_amount_yesterday,
       ROUND(
           COALESCE(t.authorized_amount, 0)
           - COALESCE(y.authorized_amount_yesterday, 0),
           2
       ) AS authorized_amount_difference,
       CASE
           WHEN y.csj IS NULL THEN 'ADDED'
           WHEN t.csj IS NULL THEN 'REMOVED'
           WHEN ROUND(
               COALESCE(t.authorized_amount, 0)
               - COALESCE(y.authorized_amount_yesterday, 0),
               2
           ) <> 0 THEN 'CHANGED'
           ELSE 'UNCHANGED'
       END AS change_status,
       CURRENT_TIMESTAMP() AS compared_at,
       CASE
           WHEN :p_advance_baseline THEN 'OFFICIAL_RUN'
           ELSE 'TEST_RUN_BASELINE_NOT_ADVANCED'
       END AS run_type
   FROM today_results t
   FULL OUTER JOIN yesterday_results y
       ON t.csj = y.csj
      AND t.district_mpo_division = y.district_mpo_division
      AND t.category = y.category
      AND t.fy = y.fy;

   ----------------------------------------------------------------------
   -- Step 3: Capture delta counts
   ----------------------------------------------------------------------
   SELECT COUNT(*)
   INTO :v_delta_row_count
   FROM UTP_DASHBOARD.GOLD.T_DELTA_REPORT_OUTPUT_GOLD;
   SELECT COUNT(*)
   INTO :v_nonzero_delta_count
   FROM UTP_DASHBOARD.GOLD.T_DELTA_REPORT_OUTPUT_GOLD
   WHERE authorized_amount_difference <> 0;

   ----------------------------------------------------------------------
   -- Step 4: Advance baseline only if requested
   ----------------------------------------------------------------------
   IF (p_advance_baseline) THEN
       CREATE OR REPLACE TABLE UTP_DASHBOARD.GOLD.CSJ_LEVEL_OUTPUT_1_PREVIOUS AS
       SELECT
           TRIM(csj) AS csj,
           TRIM(district_mpo_division) AS district_mpo_division,
           TRIM(category) AS category,
           fy::INTEGER AS fy,
           authorized_amount
       FROM UTP_DASHBOARD.GOLD.V_CSJ_LEVEL_OUTPUT_1_GOLD;
       v_baseline_status := 'ADVANCED';
   ELSE
       v_baseline_status := 'NOT_ADVANCED';
   END IF;

   ----------------------------------------------------------------------
   -- Step 5: Return status
   ----------------------------------------------------------------------
   RETURN
       'SP_DELTA_REPORT completed. '
       || 'Advance baseline flag: ' || p_advance_baseline
       || ', Baseline status: ' || v_baseline_status
       || ', Current rows: ' || v_current_row_count
       || ', Previous rows: ' || v_previous_row_count
       || ', Delta rows: ' || v_delta_row_count
       || ', Non-zero delta rows: ' || v_nonzero_delta_count
       || '.';
END;
$$;