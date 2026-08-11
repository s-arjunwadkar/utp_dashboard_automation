USE ROLE SYSADMIN;
USE DATABASE UTP_DASHBOARD;
USE WAREHOUSE UTP_DASHBOARD_WH;
USE SCHEMA SILVER;
----------------------------------------------------------------------
-- Suspend root task before modifying task graph
----------------------------------------------------------------------
-- ALTER TASK IF EXISTS SILVER.REFRESH_FINAL_DT_TASK SUSPEND;

----------------------------------------------------------------------
-- Task 1: Refresh final downstream dynamic table
----------------------------------------------------------------------
CREATE OR REPLACE TASK SILVER.REFRESH_FINAL_DT_TASK
 WAREHOUSE = UTP_DASHBOARD_WH
 SCHEDULE = 'USING CRON 55 5 * * * America/Chicago'
 COMMENT = 'Refresh final downstream dynamic table at 5:55 AM Chicago / 6:55 AM Eastern.'
AS
 ALTER DYNAMIC TABLE SILVER.PD_MISSING_MPO_DESC REFRESH;

----------------------------------------------------------------------
-- Task 2: Run official delta report after refresh succeeds
----------------------------------------------------------------------
CREATE OR REPLACE TASK SILVER.RUN_DELTA_REPORT_TASK
 WAREHOUSE = UTP_DASHBOARD_WH
 AFTER SILVER.REFRESH_FINAL_DT_TASK
 COMMENT = 'Create official delta report after final dynamic table refresh succeeds and advance previous baseline.'
AS
 CALL GOLD.SP_DELTA_REPORT(TRUE);

----------------------------------------------------------------------
-- Resume child first, then root
----------------------------------------------------------------------
ALTER TASK SILVER.RUN_DELTA_REPORT_TASK RESUME;
ALTER TASK SILVER.REFRESH_FINAL_DT_TASK RESUME;