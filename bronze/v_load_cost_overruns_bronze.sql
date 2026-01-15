USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
 
CREATE OR REPLACE VIEW BRONZE.COST_OVERRUNS_FILE
COMMENT = 'This is the cost overruns file that we get bi-week.'
AS
SELECT
    DISTRICT_DIVISION::STRING AS district_mpo_division,                 
    LET_SCHEDULE_FISCAL_YEAR::NUMBER(4,0) AS let_schedule_fiscal_year,
    CONTROL_SECTION_JOB_CSJ::STRING AS csj,
    FUNDING_LINE_NUMBER::STRING AS funding_line_number,
    FUNDING_APPROVAL_STATUS_DESCRIPTION::STRING AS funding_approval_status_description,
    FUNDING_CATEGORY::STRING AS funding_category,
    WORK_PROGRAM_CODE::STRING AS work_program_code,
    ESTIMATED_FISCAL_YEAR::NUMBER(4,0) AS estimated_fiscal_year,
    FUNDING_GROUP_NAME::STRING AS funding_group_name,
    AUTHORIZED_AMOUNT::FLOAT AS authorized_amount,
    FORCE_ACCOUNT_WATERFALL::STRING AS force_account_waterfall,
    INCENTIVE_DISINCENTIVE_WATERFALL::STRING AS incentive_disincentive_waterfall
FROM BRONZE.COST_OVERRUNS;