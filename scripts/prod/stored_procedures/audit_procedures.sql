USE DATABASE {DATABASE_NAME};
USE SCHEMA SCH_TRUSTCAB_AUDIT;

CREATE OR REPLACE PROCEDURE SCH_TRUSTCAB_AUDIT.LOAD_FILES_FROM_ARCHIVE_TO_EPIC("BRONZE_TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE

    v_stage_name VARCHAR;
v_azure_file_path VARCHAR;
    v_archive_file_path VARCHAR;
    v_sql VARCHAR;
    v_result VARCHAR DEFAULT ''Success'';
BEGIN
    -- Fetch metadata for the given table
    SELECT  STAGE_NAME, AZURE_FILE_PATH, COMPLETE_ARCHIVE_FILE_PATH
    INTO  v_stage_name, v_azure_file_path, v_archive_file_path
    FROM TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.FILE_LOAD_METADATA
    WHERE BRONZE_TABLE_NAME = :BRONZE_TABLE_NAME
    AND IS_ACTIVE = ''TRUE'';
-- Check if metadata exists
    IF (v_stage_name IS NULL) THEN
        RETURN ''Error: No active metadata found for table '' ||
:BRONZE_TABLE_NAME;
    END IF;




    -- Dynamic SQL to copy files to archive using COPY FILES
    v_sql := ''COPY FILES INTO @'' ||
v_stage_name || ''/'' || v_azure_file_path || ''/'' ||
               '' FROM @'' || v_stage_name || ''/'' || v_archive_file_path ||;
-- Execute the COPY command
    EXECUTE IMMEDIATE v_sql;

    RETURN v_result;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        RETURN ''Error: SQL execution failed - '' ||
SQLERRM;
    WHEN OTHER THEN
        RETURN ''Error: Unexpected error - '' || SQLERRM;
END;
';

CREATE OR REPLACE PROCEDURE SCH_TRUSTCAB_AUDIT.LOAD_FILES_FROM_EPIC_STAGE("BRONZE_TABLE_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    v_target_table_name VARCHAR;
    v_only_target_table_name VARCHAR;
v_stage_name VARCHAR;
    v_azure_file_path VARCHAR;
    v_file_pattern VARCHAR;
    v_file_format_type VARCHAR;
    v_sql VARCHAR;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    record_count INTEGER;
    error_code INTEGER;
    error_message VARCHAR;
v_load_status VARCHAR;
    v_loaded_count INTEGER;
    v_partial_count INTEGER;
    v_failed_count INTEGER;
    v_total_files INTEGER;
    v_copy_result RESULTSET;
    v_copy_message VARCHAR;
    v_archive_file_path VARCHAR;
    v_sql_1 VARCHAR;
    v_file_count INTEGER;
v_has_files BOOLEAN := FALSE;
    v_list_query_id VARCHAR;
    -- ## NEW VARIABLES FOR DYNAMIC MAPPING ##
    v_target_schema_name VARCHAR;
v_column_mappings VARCHAR;
    v_target_column_list VARCHAR;
BEGIN
    -- Step 0: Assign CURRENT_TIMESTAMP to variable
    start_time := CURRENT_TIMESTAMP();
-- Extract schema and table name parts
    v_only_target_table_name := SPLIT_PART(:BRONZE_TABLE_NAME, ''.'', -1);
    v_target_schema_name := SPLIT_PART(:BRONZE_TABLE_NAME, ''.'', 2);
-- Fetch metadata for the given table
    SELECT BRONZE_TABLE_NAME, STAGE_NAME, AZURE_FILE_PATH, ARCHIVE_FILE_PATH, FILE_PATTERN, FILE_FORMAT_TYPE
    INTO v_target_table_name, v_stage_name, v_azure_file_path, v_archive_file_path, v_file_pattern, v_file_format_type
    FROM TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.FILE_LOAD_METADATA
    WHERE BRONZE_TABLE_NAME = :BRONZE_TABLE_NAME
    AND IS_ACTIVE = ''TRUE'';
-- Check if metadata exists
    IF (v_stage_name IS NULL) THEN
        RETURN ''Error: No active metadata found for table '' ||
:BRONZE_TABLE_NAME;
    END IF;

    -- ################## MODIFICATION START ##################
    -- Dynamically generate the column mappings from the file positions ($1, $2, etc.)
    -- This excludes the audit columns which are populated separately.
SELECT
        LISTAGG(''$'' || c.ORDINAL_POSITION || '' AS "'' || c.COLUMN_NAME || ''"'', '',
                        '')
        WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
    INTO :v_column_mappings
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = :v_target_schema_name
      AND c.TABLE_NAME = :v_only_target_table_name
      AND c.COLUMN_NAME NOT IN (''SOURCE_FILE'', ''FILE_ROW_NUM'', ''LOAD_TIMESTAMP'');
-- Generate a comma-separated list of ALL columns in the target table for the COPY INTO statement
    SELECT
        LISTAGG(''"'' || c.COLUMN_NAME || ''"'', '', '')
        WITHIN GROUP (ORDER BY c.ORDINAL_POSITION)
    INTO :v_target_column_list
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = :v_target_schema_name
      AND c.TABLE_NAME = :v_only_target_table_name;
-- Check if column mappings were successfully generated
    IF (v_column_mappings IS NULL) THEN
        RETURN ''Error: Could not generate column mappings for table '' ||
:BRONZE_TABLE_NAME || ''. Check if table exists and has non-audit columns.'';
    END IF;
-- ################### MODIFICATION END ###################

    -- Truncate all records from table before loading data 
    EXECUTE IMMEDIATE ''TRUNCATE TABLE '' ||
:BRONZE_TABLE_NAME;
    
    -- Check if there are any files in the source folder
    EXECUTE IMMEDIATE ''LIST @'' ||
v_stage_name || ''/'' || v_azure_file_path || '' PATTERN = '''''' || v_file_pattern || '''''''' ;
    v_list_query_id := LAST_QUERY_ID();
SELECT COUNT(*) INTO v_file_count FROM TABLE(RESULT_SCAN(:v_list_query_id));
    
    IF (v_file_count = 0) THEN
        INSERT INTO TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.TRANSPORTATION_PROCESS_LOG (TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, LAST_LOAD_TIME, STATUS, ROW_COUNT, FIRST_ERROR_MESSAGE) 
        VALUES (SPLIT_PART(:BRONZE_TABLE_NAME, ''.'', 2), :v_only_target_table_name, CURRENT_TIMESTAMP(), ''NO_FILES_FOUND'', 0, ''No files found in source folder'');
RETURN :BRONZE_TABLE_NAME || '' | No files found in source folder | Status: SKIPPED'';
    END IF;
-- Dynamic SQL to copy files to archive
    v_sql_1 := ''COPY FILES INTO @'' || v_stage_name ||
''/'' || v_archive_file_path || '' FROM @'' || v_stage_name || ''/'' || v_azure_file_path || '' PATTERN = '''''' ||
v_file_pattern || '''''''' ;
    EXECUTE IMMEDIATE v_sql_1;
    
    -- ################## MODIFICATION START ##################
    -- Dynamic SQL to copy files from stage to table using the dynamic mappings
    v_sql := ''COPY INTO '' ||
v_target_table_name || '' ('' || :v_target_column_list || '')'' ||
             ''
FROM (
'' ||
                 ''   SELECT
'' ||
                     ''      '' || :v_column_mappings || '',
'' ||
                     ''      METADATA$FILENAME AS "SOURCE_FILE",
'' ||
      
               ''      METADATA$FILE_ROW_NUMBER AS "FILE_ROW_NUM",
'' ||
                     ''      CONVERT_TIMEZONE(''''America/Los_Angeles'''', ''''Asia/Calcutta'''', CURRENT_TIMESTAMP()) AS "LOAD_TIMESTAMP"
'' ||
                 ''   FROM @'' || v_stage_name || ''/'' || v_azure_file_path || '' t'' ||
             
''
)
'' ||
             ''PATTERN = '''''' || v_file_pattern || ''''''
'' ||
             ''FILE_FORMAT = (FORMAT_NAME = '' || v_file_format_type || '')
'' ||
''ON_ERROR = ''''CONTINUE''''
'' ||
             ''PURGE = TRUE'';
    -- ################### MODIFICATION END ###################

    EXECUTE IMMEDIATE v_sql;
end_time := CURRENT_TIMESTAMP();
    
    INSERT INTO TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.TRANSPORTATION_PROCESS_LOG (TARGET_SCHEMA_NAME, FILE_NAME, TARGET_TABLE_NAME, LAST_LOAD_TIME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COL_NAME, ERROR_COUNT)
    SELECT SCHEMA_NAME, FILE_NAME, TABLE_NAME, LAST_LOAD_TIME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COL_NAME, ERROR_COUNT
    FROM TRUSTCAB_PROD.INFORMATION_SCHEMA.LOAD_HISTORY
    WHERE TABLE_NAME = :v_only_target_table_name AND LAST_LOAD_TIME >= :start_time AND LAST_LOAD_TIME <= :end_time;
WITH load_stats AS (
        SELECT STATUS, COUNT(*) as file_count, SUM(ROW_COUNT) as row_sum
        FROM TRUSTCAB_PROD.INFORMATION_SCHEMA.LOAD_HISTORY
        WHERE TABLE_NAME = :v_only_target_table_name AND LAST_LOAD_TIME >= :start_time AND LAST_LOAD_TIME <= :end_time
        GROUP BY STATUS
    )
    SELECT COALESCE(SUM(row_sum), 0), COALESCE(SUM(CASE WHEN STATUS = ''LOADED'' THEN file_count END), 0), COALESCE(SUM(CASE WHEN STATUS = ''PARTIALLY_LOADED'' THEN file_count END), 0), COALESCE(SUM(CASE WHEN STATUS = ''LOAD_FAILED'' THEN file_count END), 0), COALESCE(SUM(file_count), 0)
    INTO 
record_count, v_loaded_count, v_partial_count, v_failed_count, v_total_files
    FROM load_stats;
IF (v_failed_count > 0) THEN
        v_load_status := CASE WHEN v_loaded_count > 0 OR v_partial_count > 0 THEN ''PARTIALLY_LOADED'' ELSE ''FAILED'' END;
ELSEIF (v_partial_count > 0) THEN
        v_load_status := ''PARTIALLY_LOADED'';
ELSE
        v_load_status := ''LOADED'';
    END IF;
    
    RETURN :BRONZE_TABLE_NAME || '' |
Total rows: '' || record_count || '' | Files loaded: '' || v_loaded_count || '' | Files partial: '' ||
v_partial_count || '' | Files failed: '' || v_failed_count || '' | Total files: '' || v_total_files || '' |
Status: '' || v_load_status;
    
EXCEPTION
    WHEN OTHER THEN
        error_code := SQLCODE;
error_message := SQLERRM;
        INSERT INTO TRUSTCAB_PROD.SCH_TRUSTCAB_AUDIT.TRANSPORTATION_PROCESS_LOG(TARGET_TABLE_NAME, STATUS, FIRST_ERROR_MESSAGE) 
        VALUES (:BRONZE_TABLE_NAME, ''Failure'', :error_message);
RETURN ''Error occurred in '' || :BRONZE_TABLE_NAME || '': '' || :error_message;
END;
';