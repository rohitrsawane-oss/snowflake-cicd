import snowflake.connector
import yaml
import argparse
import os
import glob
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

def execute_sql_file(cursor, file_path, replacements=None):
    """Execute SQL file with parameter replacement"""
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Replace parameters if provided
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Split and execute statements
    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
    
    for statement in statements:
        try:
            print(f"Executing: {statement[:100]}...")
            cursor.execute(statement)
            print("✓ Success")
        except Exception as e:
            print(f"✗ Error: {e}")
            raise

def deploy_environment(environment):
    """Deploy to specified environment"""
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
        
        # Deploy in order: DDL -> Stored Procedures -> Tasks -> RBAC
        deployment_order = ['ddl', 'stored_procedures', 'tasks', 'rbac']
        
        for folder in deployment_order:
            folder_path = f"scripts/{environment}/{folder}"
            if os.path.exists(folder_path):
                print(f"\n📂 Deploying {folder}...")
                sql_files = glob.glob(f"{folder_path}/*.sql")
                sql_files.sort()  # Ensure consistent order
                
                for sql_file in sql_files:
                    print(f"Executing {sql_file}...")
                    execute_sql_file(cursor, sql_file, replacements)
        
        print(f"\n🎉 {environment.upper()} deployment completed successfully!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy to Snowflake environment')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'])
    args = parser.parse_args()
    
    deploy_environment(args.environment)