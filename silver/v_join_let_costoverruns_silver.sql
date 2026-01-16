USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
USE SCHEMA SILVER;

CREATE OR REPLACE VIEW SILVER.LET_COSTOVERRUNS_JOINED_VIEW
COMMENT = 'This view joins let data file with cost overruns file to get the new total funding amount by adding the force account waterfall and incentive disincentive waterfall to the original funding amount in let data file.' 
AS
SELECT
    ld.csj,
    ld.funding_line_number,
    ld.work_program,
    ld.category,
    ld.funding_amount,
    co.force_account_waterfall,
    co.incentive_disincentive_waterfall,
    ld.funding_amount + co.force_account_waterfall + co.incentive_disincentive_waterfall AS new_total
FROM BRONZE.LET_DATA_FILE AS ld
INNER JOIN BRONZE.COST_OVERRUNS_FILE AS co
ON
        ld.csj = co.csj
    AND ld.funding_line_number = co.funding_line_number
    AND ld.category = co.funding_category
    AND ld.work_program = co.work_program_code
ORDER BY ld.csj, ld.funding_line_number, ld.category, ld.work_program
;