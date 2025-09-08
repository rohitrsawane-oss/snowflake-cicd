USE DATABASE {DATABASE_NAME};
-- Create schemas

create or replace schema CORTEX_DEMO_SCH COMMENT='Temporary schema for Cortex AI demo';

create or replace schema SCH_TRUSTCAB_AUDIT;

create or replace schema SCH_TRUSTCAB_BRONZE COMMENT='Raw/Landing zone - Bronze layer for ingested data';

create or replace schema SCH_TRUSTCAB_GOLD COMMENT='Curated/Consumption layer - Gold layer for analytics';

create or replace schema SCH_TRUSTCAB_SILVER COMMENT='Transformed/Cleansed data - Silver layer for business logic';