USE DATABASE {DATABASE_NAME};
USE SCHEMA SCH_TRUSTCAB_AUDIT;

create or replace tag PII_INFO  allowed_values  'NUMERIC_INFO' , 'STRING_INFO' COMMENT='Tag used for masking sensitive data'
;

create or replace tag SENSITIVITY COMMENT='Tag for classifying sensitive data (e.g., PII, FINANCIAL)'
;