import snowflake.connector
import yaml
import argparse
import os
import glob
import re
from pathlib import Path

def load_config(environment):
    """Load environment configuration"""
    with open(f'config/{environment}.yml', 'r') as file:
        return yaml.safe_load(file)

def connect_snowflake():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

def parse_stored_procedure_sql(sql_content):
    """Parse stored procedure SQL content to handle $$-delimited blocks"""
    procedures = []
    
    # Replace parameters first if needed
    # Look for CREATE OR REPLACE PROCEDURE patterns
    procedure_pattern = r'(CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE.*?)(?=CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE|\Z)'
    
    matches = re.finditer(procedure_pattern, sql_content, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        procedure_sql = match.group(1).strip()
        if procedure_sql:
            procedures.append(procedure_sql)
    
    # If no procedures found with regex, try to split by $$ delimiters
    if not procedures:
        # Handle $$-delimited stored procedures
        parts = sql_content.split('$$')
        current_proc = ""
        
        for i, part in enumerate(parts):
            if i % 2 == 0:  # Outside $$ block
                current_proc += part
            else:  # Inside $$ block
                current_proc += '$$' + part + '$$'
                if 'CREATE' in current_proc.upper() and 'PROCEDURE' in current_proc.upper():
                    procedures.append(current_proc.strip())
                    current_proc = ""
        
        # Add any remaining content
        if current_proc.strip():
            procedures.append(current_proc.strip())
    
    # If still no procedures, fall back to semicolon splitting
    if not procedures:
        procedures = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    
    return procedures

def execute_stored_procedure_file(cursor, file_path, replacements=None):
    """Execute stored procedure file with parameter replacement"""
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Replace parameters if provided
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Parse stored procedures
    procedures = parse_stored_procedure_sql(sql_content)
    
    for procedure in procedures:
        try:
            # Show first 100 characters for logging
            display_text = procedure.replace('\n', ' ').replace('\r', '')[:100]
            print(f"Executing stored procedure: {display_text}...")
            
            cursor.execute(procedure)
            print("✅ Stored procedure executed successfully")
        except Exception as e:
            print(f"❌ Error executing stored procedure: {e}")
            raise

def deploy_stored_procedures(environment):
    """Deploy stored procedures to specified environment"""
    config = load_config(environment)
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    try:
        # Parameter replacements
        replacements = {
            'DATABASE_NAME': config['database'],
            'WAREHOUSE_NAME': config['warehouse'],
            'ROLE_NAME': config['role']
        }
        
        # Deploy stored procedures only
        folder_path = f"scripts/{environment}/stored_procedures"
        if os.path.exists(folder_path):
            print(f"\n📂 Deploying stored procedures...")
            sql_files = glob.glob(f"{folder_path}/*.sql")
            sql_files.sort()  # Ensure consistent order
            
            for sql_file in sql_files:
                print(f"Processing stored procedure file: {sql_file}...")
                execute_stored_procedure_file(cursor, sql_file, replacements)
        else:
            print(f"⚠️ No stored procedures directory found at {folder_path}")
        
        print(f"\n🎉 {environment.upper()} stored procedures deployment completed successfully!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy Stored Procedures to Snowflake environment')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'])
    args = parser.parse_args()
    
    deploy_stored_procedures(args.environment)