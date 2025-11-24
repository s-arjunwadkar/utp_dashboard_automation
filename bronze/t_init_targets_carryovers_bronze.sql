USE ROLE SYSADMIN;
USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA BRONZE;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE OR REPLACE TABLE BRONZE.CARRYOVERS (
    CATEGORY               VARCHAR,
    DISTRICT_MPO_DIVISION  VARCHAR,
    CARRYOVERS             FLOAT,
    FY_2026                FLOAT,
    FY_2027                FLOAT,
    FY_2028                FLOAT,
    FY_2029                FLOAT,
    FY_2030                FLOAT,
    FY_2031                FLOAT,
    FY_2032                FLOAT,
    FY_2033                FLOAT,
    FY_2034                FLOAT,
    FY_2035                FLOAT
)
COMMENT = 'Initilize the carryovers table to load data from the new carryovers file each week.';
