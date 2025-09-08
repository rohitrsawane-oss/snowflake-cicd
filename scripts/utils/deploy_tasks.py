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

def parse_task_sql(sql_content):
    """Parse task SQL content to handle multi-statement BEGIN/END blocks"""
    tasks = []
    
    # Pattern to match CREATE OR REPLACE TASK statements
    # This handles tasks with BEGIN/END blocks containing multiple statements
    task_pattern = r'(CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+\w+.*?(?:BEGIN.*?END|AS\s+.*?);)'
    
    matches = re.finditer(task_pattern, sql_content, re.DOTALL | re.IGNORECASE)
    
    for match in matches:
        task_sql = match.group(1).strip()
        if task_sql:
            tasks.append(task_sql)
    
    # If no tasks found with regex, try a different approach
    if not tasks:
        # Look for CREATE TASK patterns and extract complete statements
        lines = sql_content.split('\n')
        current_task = ""
        in_task = False
        begin_end_depth = 0
        
        for line in lines:
            line_upper = line.strip().upper()
            
            if line_upper.startswith('CREATE') and 'TASK' in line_upper:
                in_task = True
                current_task = line + '\n'
            elif in_task:
                current_task += line + '\n'
                
                if 'BEGIN' in line_upper:
                    begin_end_depth += 1
                elif 'END' in line_upper:
                    begin_end_depth -= 1
                    if begin_end_depth <= 0:
                        # End of task found
                        if line.strip().endswith(';'):
                            tasks.append(current_task.strip())
                            current_task = ""
                            in_task = False
                            begin_end_depth = 0
                elif line.strip().endswith(';') and begin_end_depth == 0:
                    # Simple task without BEGIN/END
                    tasks.append(current_task.strip())
                    current_task = ""
                    in_task = False
        
        # Add any remaining task
        if current_task.strip():
            tasks.append(current_task.strip())
    
    # If still no tasks found, fall back to basic semicolon splitting
    if not tasks:
        potential_tasks = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        tasks = [task for task in potential_tasks if 'CREATE' in task.upper() and 'TASK' in task.upper()]
    
    return tasks

def execute_task_file(cursor, file_path, replacements=None):
    """Execute task file with parameter replacement"""
    with open(file_path, 'r') as file:
        sql_content = file.read()
    
    # Replace parameters if provided
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Parse tasks
    tasks = parse_task_sql(sql_content)
    
    if not tasks:
        print(f"⚠️ No valid tasks found in {file_path}")
        return
    
    for i, task in enumerate(tasks):
        try:
            # Extract task name for better logging
            task_name_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([\w.]+)', task, re.IGNORECASE)
            task_name = task_name_match.group(1) if task_name_match else f"Task_{i+1}"
            
            print(f"Executing task: {task_name}...")
            
            # Debug: Print the complete task SQL being executed
            print(f"📋 Complete Task SQL:")
            print("-" * 50)
            print(task)
            print("-" * 50)
            
            # Validate task structure
            if 'BEGIN' in task.upper() and 'END' not in task.upper():
                print("❌ ERROR: Task contains BEGIN but no END block!")
                raise ValueError(f"Incomplete task structure for {task_name} - missing END block")
            
            # Execute the complete task as a single statement
            cursor.execute(task)
            print("✅ Task created/updated successfully")
            
        except Exception as e:
            print(f"❌ Error executing task {task_name}: {e}")
            print(f"Task SQL length: {len(task)} characters")
            print(f"Task ends with: '{task[-50:]}'" if len(task) > 50 else f"Complete task: '{task}'")
            raise

def deploy_tasks(environment):
    """Deploy tasks to specified environment"""
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
        
        # Deploy tasks only
        folder_path = f"scripts/{environment}/tasks"
        if os.path.exists(folder_path):
            print(f"\n📂 Deploying tasks...")
            sql_files = glob.glob(f"{folder_path}/*.sql")
            sql_files.sort()  # Ensure consistent order
            
            for sql_file in sql_files:
                print(f"Processing task file: {sql_file}...")
                execute_task_file(cursor, sql_file, replacements)
        else:
            print(f"⚠️ No tasks directory found at {folder_path}")
        
        print(f"\n🎉 {environment.upper()} tasks deployment completed successfully!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy Tasks to Snowflake environment')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'])
    args = parser.parse_args()
    
    deploy_tasks(args.environment)