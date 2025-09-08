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

def parse_task_sql_simple(sql_content):
    """Simple parsing that reads the entire file as one task if it contains CREATE TASK"""
    # Remove leading/trailing whitespace
    sql_content = sql_content.strip()
    
    # Check if this looks like a task file
    if not re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK', sql_content, re.IGNORECASE):
        return []
    
    # For files with single tasks, just return the entire content
    # This assumes each .sql file contains one complete task
    
    # Basic validation: if there's BEGIN, there should be END
    has_begin = 'BEGIN' in sql_content.upper()
    has_end = re.search(r'\bEND\s*;?\s*$', sql_content, re.IGNORECASE | re.MULTILINE)
    
    if has_begin and not has_end:
        print("⚠️ Warning: Task has BEGIN but no END. Adding END;")
        if not sql_content.endswith(';'):
            sql_content += '\nEND;'
        else:
            sql_content += '\nEND;'
    elif has_begin and has_end:
        # Ensure it ends with semicolon
        if not sql_content.strip().endswith(';'):
            sql_content = sql_content.strip() + ';'
    
    return [sql_content]

def execute_task_file_debug(cursor, file_path, replacements=None):
    """Execute task file with extensive debugging"""
    print(f"\n🔍 DEBUG: Reading file {file_path}")
    
    # Read and display raw file content
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    print(f"📄 Raw file content ({len(sql_content)} characters):")
    print("=" * 80)
    print(sql_content)
    print("=" * 80)
    
    # Show file ending explicitly
    print(f"📍 File ends with: '{sql_content[-100:]}'" if len(sql_content) > 100 else f"Complete file: '{sql_content}'")
    
    # Replace parameters if provided
    if replacements:
        original_content = sql_content
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
        if sql_content != original_content:
            print(f"\n🔄 After parameter replacement:")
            print("=" * 80)
            print(sql_content)
            print("=" * 80)
    
    # Parse tasks using simple method
    tasks = parse_task_sql_simple(sql_content)
    
    print(f"\n📊 Parsing results: Found {len(tasks)} task(s)")
    
    if not tasks:
        print(f"❌ No valid tasks found in {file_path}")
        return
    
    for i, task in enumerate(tasks):
        print(f"\n🎯 Task {i+1}:")
        print(f"   Length: {len(task)} characters")
        print(f"   Starts with: '{task[:50]}...'")
        print(f"   Ends with: '...{task[-50:]}'" if len(task) > 50 else f"   Complete: '{task}'")
        
        # Check for BEGIN/END balance
        has_begin = 'BEGIN' in task.upper()
        has_end = 'END' in task.upper()
        print(f"   Has BEGIN: {has_begin}")
        print(f"   Has END: {has_end}")
        
        try:
            # Extract task name for better logging
            task_name_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([\w.]+)', task, re.IGNORECASE)
            task_name = task_name_match.group(1) if task_name_match else f"Task_{i+1}"
            
            print(f"\n🚀 Executing task: {task_name}")
            print("📋 Complete Task SQL being sent to Snowflake:")
            print("-" * 60)
            print(task)
            print("-" * 60)
            
            # Validate task structure before execution
            if has_begin and not has_end:
                print("❌ ERROR: Task contains BEGIN but no END block!")
                raise ValueError(f"Incomplete task structure for {task_name} - missing END block")
            
            # Execute the complete task as a single statement
            cursor.execute(task)
            print("✅ Task created/updated successfully")
            
        except Exception as e:
            print(f"❌ Error executing task {task_name}: {e}")
            print(f"🔍 Debug info:")
            print(f"   Task SQL length: {len(task)} characters")
            print(f"   Task starts: '{task[:100]}'")
            print(f"   Task ends: '{task[-100:]}'" if len(task) > 100 else f"   Complete task: '{task}'")
            raise

def deploy_tasks_debug(environment):
    """Deploy tasks with extensive debugging"""
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
        
        print(f"🔧 Configuration for {environment}:")
        for key, value in replacements.items():
            print(f"   {key}: {value}")
        
        # Deploy tasks only
        folder_path = f"scripts/{environment}/tasks"
        print(f"\n📂 Looking for tasks in: {folder_path}")
        
        if os.path.exists(folder_path):
            print(f"✅ Tasks directory found")
            sql_files = glob.glob(f"{folder_path}/*.sql")
            sql_files.sort()  # Ensure consistent order
            
            print(f"📁 Found {len(sql_files)} SQL file(s):")
            for sql_file in sql_files:
                print(f"   - {sql_file}")
            
            if not sql_files:
                print("⚠️ No .sql files found in tasks directory")
                return
            
            for sql_file in sql_files:
                print(f"\n" + "="*80)
                print(f"🗃️ Processing task file: {sql_file}")
                print("="*80)
                execute_task_file_debug(cursor, sql_file, replacements)
        else:
            print(f"❌ Tasks directory not found at {folder_path}")
        
        print(f"\n🎉 {environment.upper()} tasks deployment completed successfully!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy Tasks to Snowflake environment with debugging')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'])
    args = parser.parse_args()
    
    deploy_tasks_debug(args.environment)