USE DATABASE SHARVIL_UTP_2026_DASHBOARD;
USE SCHEMA REF;
USE WAREHOUSE SHARVIL_UTP_DASHBOARD;

CREATE SCHEMA IF NOT EXISTS REF;

CREATE OR REPLACE TABLE REF.CATEGORY_MAP (
  category_parent     STRING      NOT NULL,  -- e.g. '4', '10', '11'
  work_program_exact  STRING,                -- e.g. '04CN'
  work_program_regex  STRING,                -- e.g. '.*11$'
  pid_exact           STRING,
  pid_regex           STRING,                -- Will drop if not required
  new_category        STRING      NOT NULL,  -- e.g. '4R', '4U', '10CR', '11ES'
  org_scope           STRING,                -- e.g. 'By District','By MPO','Statewide'
  comments            STRING,
  is_active           BOOLEAN     DEFAULT TRUE,
  valid_from          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  valid_to            TIMESTAMP_LTZ
)
COMMENT = 'This table stores information about categories, their subcategory based on Work Program & PID. Also organization scope of each subcategory.';