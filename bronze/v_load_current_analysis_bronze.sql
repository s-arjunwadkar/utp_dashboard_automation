USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;
 
CREATE OR REPLACE VIEW BRONZE.LET_DATA_FILE
COMMENT = 'This is the current analysis(LET) file that we get bi-week.'
AS
SELECT
    LET_YEAR::NUMBER(4,0) AS let_fiscal_year,
    DISTRICT::STRING AS district,
    CATEGORY::STRING AS category,
    FUNDING_AMOUNT::FLOAT AS funding_amount,
    CSJ::STRING AS csj,
    ACTUAL_LET_DATE::DATE AS actual_let_date,
    PID::STRING AS pid,
    MPO_CD_DSCR::STRING AS mpo_description,
    WORK_PROGRAM::STRING AS work_program,
    ROW_CSJ::STRING AS row_csj,
    FUNDING_LINE_NO::STRING AS funding_line_number
FROM BRONZE.LET_DATA;                        