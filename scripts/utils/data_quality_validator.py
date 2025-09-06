import snowflake.connector
import yaml
import argparse
import os

def load_data_quality_rules():
    """Load data quality validation rules"""
    with open('config/data_quality_rules.yml', 'r') as file:
        return yaml.safe_load(file)

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

def run_null_checks(cursor, database, checks):
    """Run null value checks"""
    print("\n🔍 Running NULL checks...")
    failed_checks = []
    
    for check in checks:
        table = check['table']
        columns = check['columns']
        
        for column in columns:
            query = f"""
            SELECT COUNT(*) as null_count 
            FROM {database}.{table} 
            WHERE {column} IS NULL
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            null_count = result[0] if result else 0
            
            if null_count > 0:
                failed_checks.append(f"❌ {table}.{column}: {null_count} NULL values found")
            else:
                print(f"✅ {table}.{column}: No NULL values")
    
    return failed_checks

def run_unique_checks(cursor, database, checks):
    """Run uniqueness checks"""
    print("\n🔍 Running UNIQUE checks...")
    failed_checks = []
    
    for check in checks:
        table = check['table']
        columns = check['columns']
        
        for column in columns:
            query = f"""
            SELECT COUNT(*) as total_count, 
                   COUNT(DISTINCT {column}) as unique_count
            FROM {database}.{table}
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            total_count = result[0] if result else 0
            unique_count = result[1] if result else 0
            
            if total_count != unique_count:
                duplicates = total_count - unique_count
                failed_checks.append(f"❌ {table}.{column}: {duplicates} duplicate values found")
            else:
                print(f"✅ {table}.{column}: All values are unique")
    
    return failed_checks

def run_count_checks(cursor, database, checks):
    """Run row count checks"""
    print("\n🔍 Running COUNT checks...")
    failed_checks = []
    
    for check in checks:
        table = check['table']
        min_count = check['min_count']
        
        query = f"SELECT COUNT(*) FROM {database}.{table}"
        cursor.execute(query)
        result = cursor.fetchone()
        actual_count = result[0] if result else 0
        
        if actual_count < min_count:
            failed_checks.append(f"❌ {table}: {actual_count} rows (minimum {min_count} required)")
        else:
            print(f"✅ {table}: {actual_count} rows (meets minimum requirement)")
    
    return failed_checks

def validate_data_quality(environment):
    """Run all data quality checks"""
    config = load_config(environment)
    rules = load_data_quality_rules()
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    all_failed_checks = []
    
    try:
        database = config['database']
        checks = rules['data_quality_checks']
        
        # Run all types of checks
        all_failed_checks.extend(run_null_checks(cursor, database, checks.get('null_checks', [])))
        all_failed_checks.extend(run_unique_checks(cursor, database, checks.get('unique_checks', [])))
        all_failed_checks.extend(run_count_checks(cursor, database, checks.get('count_checks', [])))
        
        # Report results
        if all_failed_checks:
            print("\n❌ Data Quality Validation FAILED:")
            for check in all_failed_checks:
                print(check)
            exit(1)
        else:
            print(f"\n🎉 All data quality checks passed for {environment.upper()} environment!")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Validate data quality in Snowflake')
    parser.add_argument('--environment', required=True, choices=['dev', 'prod'])
    args = parser.parse_args()
    
    validate_data_quality(args.environment)