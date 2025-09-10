import snowflake.connector
import yaml
import argparse
import os
from pathlib import Path

def load_config(environment):
    """Load environment configuration"""
    try:
        with open(f'config/{environment}.yml', 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"❌ Error: Config file 'config/{environment}.yml' not found.")
        raise

def connect_snowflake():
    """Create Snowflake connection using environment variables"""
    try:
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {e}")
        raise

def execute_sql_file(cursor, file_path, replacements=None):
    """Execute SQL file with parameter replacement"""
    try:
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
                print("✅ Success")
            except Exception as e:
                print(f"❌ Error during statement execution: {e}")
                raise
    except FileNotFoundError:
        print(f"❌ Error: SQL file '{file_path}' not found.")
        raise

def execute_task(environment, task_name):
    """Execute a specific task script in the specified environment"""
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
        
        # Define the file path for the specific task
        task_file_path = f"scripts/{environment}/tasks/{task_name}.sql"

        print(f"\n📂 Executing task '{task_name}' in {environment.upper()}...")
        execute_sql_file(cursor, task_file_path, replacements)
        
        print(f"\n🎉 Task '{task_name}' deployment completed successfully!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute a specific task script in a Snowflake environment')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'],
                        help='The target environment (dev or prod)')
    parser.add_argument('--task_name', required=True,
                        help='The name of the task script to execute (e.g., "my_first_task")')
    args = parser.parse_args()
    
    execute_task(args.environment, args.task_name)
