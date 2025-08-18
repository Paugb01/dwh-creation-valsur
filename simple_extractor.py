"""
Simple MySQL Data Extractor
Extracts data from pk_gest_xer database and saves to parquet files
Uses configuration files for secure credential management
"""
import pymysql
import pandas as pd
import os
from datetime import datetime
from config_manager import config_manager

def get_working_connection():
    """Get a working MySQL connection using configuration"""
    try:
        db_config = config_manager.get_database_config()
        connection = pymysql.connect(**db_config)
        print("✓ PyMySQL connection successful!")
        return connection
    except FileNotFoundError as e:
        print(f"❌ Configuration error: {e}")
        raise
    except Exception as e:
        raise Exception(f"PyMySQL connection failed: {e}")

def extract_single_table(table_name, limit=None):
    """Extract data from a single table"""
    print(f"Extracting data from table: {table_name}")
    
    connection = get_working_connection()
    extraction_config = config_manager.get_extraction_config()
    
    try:
        # Build query
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        # Extract data
        df = pd.read_sql(query, connection)
        print(f"Extracted {len(df)} rows from {table_name}")
        
        # Create output directory using config
        output_dir = os.path.join(
            extraction_config['output_directory'], 
            extraction_config['bronze_layer_path'], 
            table_name
        )
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"Saved to: {filepath}")
        
        return filepath
        
    finally:
        connection.close()

def main():
    """Test extraction with a single table"""
    print("Starting data extraction test...")
    
    try:
        # Load configurations
        extraction_config = config_manager.get_extraction_config()
        test_limit = extraction_config.get('test_limit', 100)
        
        print(f"Using configuration:")
        print(f"  - Output directory: {extraction_config['output_directory']}")
        print(f"  - Test limit: {test_limit}")
        
        # Test connection first
        connection = get_working_connection()
        print("✓ Database connection successful")
        
        # Get first table for testing
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        test_table = tables[0][0]  # Get first table name
        connection.close()
        
        print(f"Testing with table: {test_table}")
        
        # Extract small sample
        filepath = extract_single_table(test_table, limit=test_limit)
        
        print("✓ Extraction test completed successfully!")
        print(f"Data saved to: {filepath}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
