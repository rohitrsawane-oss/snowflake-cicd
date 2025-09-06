#!/usr/bin/env python3
"""
Basic post-deployment validator - simple connection and basic checks only
"""
import argparse
import snowflake.connector
import os

def basic_validation(environment):
    """Run basic validation checks after deployment"""
    
    print(f"🔍 Starting basic validation for {environment} environment")
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
        
        print("✅ Successfully connected to Snowflake")
        
        cursor = conn.cursor()
        
        # Basic connectivity test
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        print(f"✅ Snowflake Version: {version[0]}")
        
        # Check current role and warehouse
        cursor.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE()")
        role_warehouse = cursor.fetchone()
        print(f"✅ Current Role: {role_warehouse[0]}")
        print(f"✅ Current Warehouse: {role_warehouse[1]}")
        
        # Simple query to verify basic functionality
        cursor.execute("SELECT CURRENT_TIMESTAMP()")
        timestamp = cursor.fetchone()
        print(f"✅ Current Timestamp: {timestamp[0]}")
        
        cursor.close()
        conn.close()
        
        print(f"✅ Basic validation completed successfully for {environment}")
        return True
        
    except Exception as e:
        print(f"❌ Basic validation failed: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run basic post-deployment validation')
    parser.add_argument('--environment', required=True, 
                       help='Environment to validate (dev/staging/prod)')
    
    args = parser.parse_args()
    
    success = basic_validation(args.environment)
    exit(0 if success else 1)