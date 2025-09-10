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

def prepare_task_sql(sql_content, replacements=None):
    """Prepare task SQL by ensuring proper formatting for multi-statement tasks"""
    
    # Replace parameters if provided
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Remove extra whitespace and normalize
    sql_content = sql_content.strip()
    
    # Check if this is a task creation statement
    if not re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK', sql_content, re.IGNORECASE):
        return None, "Not a task creation statement"
    
    # Handle tasks with BEGIN...END blocks - keep original format without dollar quotes
    if re.search(r'BEGIN\s*\n.*?END\s*;?\s*

def execute_task_statements_sequentially(cursor, sql_content, replacements=None):
    """Execute SQL by handling USE statements separately and task creation as single unit"""
    
    print(f"\n🔍 Processing SQL content ({len(sql_content)} characters)")
    
    # Replace parameters
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Split into logical statements but keep task creation as one unit
    statements = []
    
    # Extract USE statements first
    use_statements = re.findall(r'USE\s+(?:DATABASE|SCHEMA)\s+[^;]+;', sql_content, re.IGNORECASE)
    for use_stmt in use_statements:
        statements.append(use_stmt.strip())
    
    # Remove USE statements from content to get the task creation
    task_content = sql_content
    for use_stmt in use_statements:
        task_content = task_content.replace(use_stmt, '').strip()
    
    # Process the task creation
    if task_content:
        formatted_task, status = prepare_task_sql(task_content, replacements)
        if formatted_task:
            statements.append(formatted_task)
        else:
            print(f"❌ Failed to prepare task SQL: {status}")
            return False
    
    # Execute statements sequentially
    for i, statement in enumerate(statements, 1):
        if not statement.strip():
            continue
            
        print(f"\n🚀 Executing statement {i}:")
        print("-" * 60)
        print(statement)
        print("-" * 60)
        
        try:
            cursor.execute(statement)
            
            # Identify statement type for better logging
            if statement.upper().startswith('USE'):
                print("✅ Context set successfully")
            elif 'CREATE' in statement.upper() and 'TASK' in statement.upper():
                # Extract task name
                task_name_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([\w.]+)', statement, re.IGNORECASE)
                task_name = task_name_match.group(1) if task_name_match else "TASK"
                print(f"✅ Task '{task_name}' created/updated successfully")
            else:
                print("✅ Statement executed successfully")
                
        except Exception as e:
            print(f"❌ Error executing statement: {e}")
            print(f"🔍 Statement that failed:")
            print(f"   Length: {len(statement)} characters")
            print(f"   Content preview: {statement[:200]}{'...' if len(statement) > 200 else ''}")
            raise
    
    return True

def execute_task_file_enhanced(cursor, file_path, replacements=None):
    """Execute task file with enhanced multi-semicolon support"""
    print(f"\n🔍 Processing file: {file_path}")
    
    try:
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        print(f"📄 File content loaded ({len(sql_content)} characters)")
        
        # Show content preview
        print("📋 Content preview:")
        print("=" * 60)
        preview = sql_content[:300] + ('...' if len(sql_content) > 300 else '')
        print(preview)
        print("=" * 60)
        
        # Execute using sequential method
        success = execute_task_statements_sequentially(cursor, sql_content, replacements)
        
        if success:
            print(f"✅ Successfully processed {file_path}")
        else:
            print(f"❌ Failed to process {file_path}")
            
        return success
        
    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        raise

def deploy_tasks_enhanced(environment):
    """Deploy tasks with enhanced multi-semicolon support"""
    print(f"🚀 Starting enhanced task deployment for {environment.upper()}")
    
    config = load_config(environment)
    
    print(f"\n📊 Environment configuration:")
    print(f"   Database: {config.get('database', 'Not specified')}")
    print(f"   Warehouse: {config.get('warehouse', 'Not specified')}")
    print(f"   Role: {config.get('role', 'Not specified')}")
    
    # Connect to Snowflake
    print(f"\n🔌 Connecting to Snowflake...")
    conn = connect_snowflake()
    cursor = conn.cursor()
    print("✅ Connected successfully")
    
    try:
        # Parameter replacements
        replacements = {
            'DATABASE_NAME': config.get('database', ''),
            'WAREHOUSE_NAME': config.get('warehouse', ''),
            'ROLE_NAME': config.get('role', '')
        }
        
        print(f"\n🔧 Parameter replacements:")
        for key, value in replacements.items():
            print(f"   {key} → {value}")
        
        # Find and deploy tasks
        folder_path = f"scripts/{environment}/tasks"
        print(f"\n📂 Looking for tasks in: {folder_path}")
        
        if not os.path.exists(folder_path):
            print(f"❌ Tasks directory not found at {folder_path}")
            return False
        
        print(f"✅ Tasks directory found")
        
        # Get all SQL files
        sql_files = glob.glob(f"{folder_path}/*.sql")
        sql_files.sort()  # Ensure consistent order
        
        print(f"📁 Found {len(sql_files)} SQL file(s):")
        for sql_file in sql_files:
            print(f"   - {os.path.basename(sql_file)}")
        
        if not sql_files:
            print("⚠️ No .sql files found in tasks directory")
            return False
        
        # Process each file
        success_count = 0
        total_count = len(sql_files)
        
        for sql_file in sql_files:
            print(f"\n" + "=" * 80)
            print(f"🗃️ Processing: {os.path.basename(sql_file)}")
            print("=" * 80)
            
            try:
                if execute_task_file_enhanced(cursor, sql_file, replacements):
                    success_count += 1
                else:
                    print(f"❌ Failed to process {os.path.basename(sql_file)}")
            except Exception as e:
                print(f"❌ Exception while processing {os.path.basename(sql_file)}: {e}")
        
        # Summary
        print(f"\n" + "=" * 80)
        print(f"📊 DEPLOYMENT SUMMARY")
        print("=" * 80)
        print(f"✅ Successful: {success_count}/{total_count}")
        print(f"❌ Failed: {total_count - success_count}/{total_count}")
        
        if success_count == total_count:
            print(f"🎉 All tasks deployed successfully to {environment.upper()}!")
            return True
        else:
            print(f"⚠️ Some tasks failed to deploy to {environment.upper()}")
            return False
        
    finally:
        print(f"\n🔌 Closing connection...")
        cursor.close()
        conn.close()
        print("✅ Connection closed")

def simulate_snow_sql_command(environment, sql_file_path):
    """Simulate the 'snow sql -f' command behavior"""
    print(f"🎯 Simulating: snow sql -f {sql_file_path}")
    print(f"📍 Environment: {environment}")
    
    # This function mimics what the snow CLI would do
    # but using the Python connector instead
    
    config = load_config(environment)
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    try:
        # Parameter replacements
        replacements = {
            'DATABASE_NAME': config.get('database', ''),
            'WAREHOUSE_NAME': config.get('warehouse', ''),
            'ROLE_NAME': config.get('role', '')
        }
        
        return execute_task_file_enhanced(cursor, sql_file_path, replacements)
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy Tasks to Snowflake with multi-semicolon support')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'], 
                       help='Target environment')
    parser.add_argument('--file', type=str, 
                       help='Single SQL file to execute (simulates snow sql -f)')
    
    args = parser.parse_args()
    
    try:
        if args.file:
            # Simulate single file execution like 'snow sql -f'
            success = simulate_snow_sql_command(args.environment, args.file)
            exit(0 if success else 1)
        else:
            # Deploy all tasks in directory
            success = deploy_tasks_enhanced(args.environment)
            exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Deployment interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n💥 Deployment failed with error: {e}")
        exit(1), sql_content, re.IGNORECASE | re.DOTALL):
        # This is a multi-statement task with BEGIN...END
        print("🔍 Detected multi-statement task with BEGIN...END block")
        print("✅ Using original format without dollar quotes")
        
        # Just ensure it ends with semicolon
        if not sql_content.endswith(';'):
            sql_content += ';'
        
        return sql_content, "Success"
    
    # Handle simple tasks or tasks already properly formatted
    if not sql_content.endswith(';'):
        sql_content += ';'
    
    return sql_content, "Success"

def execute_task_statements_sequentially(cursor, sql_content, replacements=None):
    """Execute SQL by handling USE statements separately and task creation as single unit"""
    
    print(f"\n🔍 Processing SQL content ({len(sql_content)} characters)")
    
    # Replace parameters
    if replacements:
        for key, value in replacements.items():
            sql_content = sql_content.replace(f'{{{key}}}', value)
    
    # Split into logical statements but keep task creation as one unit
    statements = []
    
    # Extract USE statements first
    use_statements = re.findall(r'USE\s+(?:DATABASE|SCHEMA)\s+[^;]+;', sql_content, re.IGNORECASE)
    for use_stmt in use_statements:
        statements.append(use_stmt.strip())
    
    # Remove USE statements from content to get the task creation
    task_content = sql_content
    for use_stmt in use_statements:
        task_content = task_content.replace(use_stmt, '').strip()
    
    # Process the task creation
    if task_content:
        formatted_task, status = prepare_task_sql(task_content, replacements)
        if formatted_task:
            statements.append(formatted_task)
        else:
            print(f"❌ Failed to prepare task SQL: {status}")
            return False
    
    # Execute statements sequentially
    for i, statement in enumerate(statements, 1):
        if not statement.strip():
            continue
            
        print(f"\n🚀 Executing statement {i}:")
        print("-" * 60)
        print(statement)
        print("-" * 60)
        
        try:
            cursor.execute(statement)
            
            # Identify statement type for better logging
            if statement.upper().startswith('USE'):
                print("✅ Context set successfully")
            elif 'CREATE' in statement.upper() and 'TASK' in statement.upper():
                # Extract task name
                task_name_match = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+([\w.]+)', statement, re.IGNORECASE)
                task_name = task_name_match.group(1) if task_name_match else "TASK"
                print(f"✅ Task '{task_name}' created/updated successfully")
            else:
                print("✅ Statement executed successfully")
                
        except Exception as e:
            print(f"❌ Error executing statement: {e}")
            print(f"🔍 Statement that failed:")
            print(f"   Length: {len(statement)} characters")
            print(f"   Content preview: {statement[:200]}{'...' if len(statement) > 200 else ''}")
            raise
    
    return True

def execute_task_file_enhanced(cursor, file_path, replacements=None):
    """Execute task file with enhanced multi-semicolon support"""
    print(f"\n🔍 Processing file: {file_path}")
    
    try:
        # Read file content
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        print(f"📄 File content loaded ({len(sql_content)} characters)")
        
        # Show content preview
        print("📋 Content preview:")
        print("=" * 60)
        preview = sql_content[:300] + ('...' if len(sql_content) > 300 else '')
        print(preview)
        print("=" * 60)
        
        # Execute using sequential method
        success = execute_task_statements_sequentially(cursor, sql_content, replacements)
        
        if success:
            print(f"✅ Successfully processed {file_path}")
        else:
            print(f"❌ Failed to process {file_path}")
            
        return success
        
    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        raise

def deploy_tasks_enhanced(environment):
    """Deploy tasks with enhanced multi-semicolon support"""
    print(f"🚀 Starting enhanced task deployment for {environment.upper()}")
    
    config = load_config(environment)
    
    print(f"\n📊 Environment configuration:")
    print(f"   Database: {config.get('database', 'Not specified')}")
    print(f"   Warehouse: {config.get('warehouse', 'Not specified')}")
    print(f"   Role: {config.get('role', 'Not specified')}")
    
    # Connect to Snowflake
    print(f"\n🔌 Connecting to Snowflake...")
    conn = connect_snowflake()
    cursor = conn.cursor()
    print("✅ Connected successfully")
    
    try:
        # Parameter replacements
        replacements = {
            'DATABASE_NAME': config.get('database', ''),
            'WAREHOUSE_NAME': config.get('warehouse', ''),
            'ROLE_NAME': config.get('role', '')
        }
        
        print(f"\n🔧 Parameter replacements:")
        for key, value in replacements.items():
            print(f"   {key} → {value}")
        
        # Find and deploy tasks
        folder_path = f"scripts/{environment}/tasks"
        print(f"\n📂 Looking for tasks in: {folder_path}")
        
        if not os.path.exists(folder_path):
            print(f"❌ Tasks directory not found at {folder_path}")
            return False
        
        print(f"✅ Tasks directory found")
        
        # Get all SQL files
        sql_files = glob.glob(f"{folder_path}/*.sql")
        sql_files.sort()  # Ensure consistent order
        
        print(f"📁 Found {len(sql_files)} SQL file(s):")
        for sql_file in sql_files:
            print(f"   - {os.path.basename(sql_file)}")
        
        if not sql_files:
            print("⚠️ No .sql files found in tasks directory")
            return False
        
        # Process each file
        success_count = 0
        total_count = len(sql_files)
        
        for sql_file in sql_files:
            print(f"\n" + "=" * 80)
            print(f"🗃️ Processing: {os.path.basename(sql_file)}")
            print("=" * 80)
            
            try:
                if execute_task_file_enhanced(cursor, sql_file, replacements):
                    success_count += 1
                else:
                    print(f"❌ Failed to process {os.path.basename(sql_file)}")
            except Exception as e:
                print(f"❌ Exception while processing {os.path.basename(sql_file)}: {e}")
        
        # Summary
        print(f"\n" + "=" * 80)
        print(f"📊 DEPLOYMENT SUMMARY")
        print("=" * 80)
        print(f"✅ Successful: {success_count}/{total_count}")
        print(f"❌ Failed: {total_count - success_count}/{total_count}")
        
        if success_count == total_count:
            print(f"🎉 All tasks deployed successfully to {environment.upper()}!")
            return True
        else:
            print(f"⚠️ Some tasks failed to deploy to {environment.upper()}")
            return False
        
    finally:
        print(f"\n🔌 Closing connection...")
        cursor.close()
        conn.close()
        print("✅ Connection closed")

def simulate_snow_sql_command(environment, sql_file_path):
    """Simulate the 'snow sql -f' command behavior"""
    print(f"🎯 Simulating: snow sql -f {sql_file_path}")
    print(f"📍 Environment: {environment}")
    
    # This function mimics what the snow CLI would do
    # but using the Python connector instead
    
    config = load_config(environment)
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    try:
        # Parameter replacements
        replacements = {
            'DATABASE_NAME': config.get('database', ''),
            'WAREHOUSE_NAME': config.get('warehouse', ''),
            'ROLE_NAME': config.get('role', '')
        }
        
        return execute_task_file_enhanced(cursor, sql_file_path, replacements)
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Deploy Tasks to Snowflake with multi-semicolon support')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'], 
                       help='Target environment')
    parser.add_argument('--file', type=str, 
                       help='Single SQL file to execute (simulates snow sql -f)')
    
    args = parser.parse_args()
    
    try:
        if args.file:
            # Simulate single file execution like 'snow sql -f'
            success = simulate_snow_sql_command(args.environment, args.file)
            exit(0 if success else 1)
        else:
            # Deploy all tasks in directory
            success = deploy_tasks_enhanced(args.environment)
            exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print(f"\n⏹️ Deployment interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n💥 Deployment failed with error: {e}")
        exit(1)