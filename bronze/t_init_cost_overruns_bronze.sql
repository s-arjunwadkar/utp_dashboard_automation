USE ROLE SYSADMIN;
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA BRONZE;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE TABLE BRONZE.COSTOVERRUNS (
    DISTRICT/DIVISION                       VARCHAR,
    LET_SCHEDULE_FISCAL_YEAR                NUMBER(4,0),
    PROJECT_ESTIMATED_LET_DATE              DATE,
    HIGHWAY                                 VARCHAR,
    CONTROLLING_PROJECT_ID(CCSJ)            VARCHAR,
    CONTROL_SECTION_JOB(CSJ)                VARCHAR,
    FUNDING_LINE_NUMBER                     INTEGER,
    FUNDING_APPROVAL_STATUS_DESCRIPTION     VARCHAR,
    FIXED_FUNDS                             VARCHAR,
    FUNDING_CATEGORY                        VARCHAR,
    WORKP_ROGRAM_CODE                       VARCHAR,
    ESTIMATED_FISCAL_YEAR                   NUMBER(4,0),
    Filter_for_2/4/12_CSJ                   VARCHAR,
    FUNDING_GROUP_NAME                      VARCHAR,
    LET_TYPE_DESCRIPTION                    VARCHAR,
    AUTHORIZED_AMOUNT                       FLOAT,
    FORCE_ACCOUNT_WATERFALL                 FLOAT,
    INCENTIVE/DISINCENTIVE_WATERFALL        FLOAT,
    LOW_BID_BY_CSJ                          FLOAT,
    WATERFALL_FUNDING_LINE_AMOUNT           FLOAT,
    INGESTED_AT                             TIMESTAMP_NTZ
)
COMMENT = 'Initialize the cost overruns table to load data from the new cost overruns file each month.';
