"""
MySQL to GCS Data Extractor
Extracts data from MySQL database and uploads to Google Cloud Storage
Uses secure configuration management for credentials
"""
import pymysql
import pandas as pd
import os
from datetime import datetime
from google.cloud import storage
from google.auth import default
import tempfile
from config_manager import config_manager

class MySQLToGCSExtractor:
    def __init__(self):
        self.config_manager = config_manager
        self.storage_client = None
        self.bucket = None
        
    def initialize_gcs(self):
        """Initialize Google Cloud Storage client and bucket"""
        try:
            gcp_config = self.config_manager.get_gcp_config()
            
            # Check if service account key is provided
            if gcp_config.get('service_account_key_path') and gcp_config['service_account_key_path'] != "path/to/service-account.json":
                # Use service account key file
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcp_config['service_account_key_path']
                print(f"Using service account key: {gcp_config['service_account_key_path']}")
            else:
                print("Using default credentials (gcloud auth or environment)")
            
            # Initialize client
            self.storage_client = storage.Client(project=gcp_config.get('project_id'))
            
            # Get bucket
            bucket_name = gcp_config.get('bucket_name')
            if bucket_name and bucket_name != "your-dwh-bucket":
                self.bucket = self.storage_client.bucket(bucket_name)
                print(f"✓ Connected to GCS bucket: {bucket_name}")
                return True
            else:
                print("❌ GCS bucket name not configured")
                return False
                
        except Exception as e:
            print(f"❌ Failed to initialize GCS: {e}")
            return False
    
    def get_mysql_connection(self):
        """Get MySQL connection using configuration"""
        try:
            db_config = self.config_manager.get_database_config()
            connection = pymysql.connect(**db_config)
            print("✓ MySQL connection successful!")
            return connection
        except Exception as e:
            raise Exception(f"MySQL connection failed: {e}")
    
    def extract_table_data(self, table_name, limit=None):
        """Extract data from a MySQL table"""
        print(f"Extracting data from table: {table_name}")
        
        connection = self.get_mysql_connection()
        
        try:
            # Build query
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            # Extract data
            df = pd.read_sql(query, connection)
            print(f"Extracted {len(df)} rows from {table_name}")
            return df
            
        finally:
            connection.close()
    
    def save_to_local_bronze(self, df, table_name):
        """Save DataFrame to local bronze layer"""
        if df.empty:
            print(f"No data to save for {table_name}")
            return None
            
        extraction_config = self.config_manager.get_extraction_config()
        
        # Create output directory
        output_dir = os.path.join(
            extraction_config['output_directory'], 
            extraction_config['bronze_layer_path'], 
            table_name
        )
        os.makedirs(output_dir, exist_ok=True)
        
        # Save file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"Saved locally to: {filepath}")
        return filepath
    
    def upload_to_gcs(self, local_filepath, table_name):
        """Upload parquet file to Google Cloud Storage"""
        if not self.storage_client or not self.bucket:
            print("❌ GCS not initialized. Skipping upload.")
            return None
            
        try:
            # Create GCS path with date partitioning
            date_str = datetime.now().strftime("%Y/%m/%d")
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            gcs_path = f"bronze/pk_gest_xer/{table_name}/date={date_str}/{table_name}_{timestamp_str}.parquet"
            
            # Upload file
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_filepath)
            
            full_gcs_path = f"gs://{self.bucket.name}/{gcs_path}"
            print(f"✓ Uploaded to GCS: {full_gcs_path}")
            return full_gcs_path
            
        except Exception as e:
            print(f"❌ Failed to upload to GCS: {e}")
            return None
    
    def save_metadata(self, table_name, local_path, gcs_path, record_count):
        """Save extraction metadata"""
        metadata = {
            'table_name': table_name,
            'extraction_timestamp': datetime.now().isoformat(),
            'record_count': record_count,
            'local_path': local_path,
            'gcs_path': gcs_path,
            'status': 'success' if gcs_path else 'local_only'
        }
        
        extraction_config = self.config_manager.get_extraction_config()
        metadata_dir = os.path.join(
            extraction_config['output_directory'], 
            extraction_config['metadata_path']
        )
        os.makedirs(metadata_dir, exist_ok=True)
        
        # Save metadata
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metadata_file = os.path.join(metadata_dir, f"{table_name}_extraction_{timestamp}.json")
        
        import json
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Metadata saved to: {metadata_file}")
        return metadata_file
    
    def extract_and_upload_table(self, table_name, limit=None):
        """Complete extraction and upload process for a single table"""
        try:
            # Extract data
            df = self.extract_table_data(table_name, limit)
            
            if df.empty:
                print(f"No data extracted from {table_name}")
                return False
            
            # Save locally
            local_path = self.save_to_local_bronze(df, table_name)
            
            # Upload to GCS (if configured)
            gcs_path = None
            if self.storage_client and self.bucket:
                gcs_path = self.upload_to_gcs(local_path, table_name)
            
            # Save metadata
            self.save_metadata(table_name, local_path, gcs_path, len(df))
            
            print(f"✅ Successfully processed {table_name}: {len(df)} records")
            return True
            
        except Exception as e:
            print(f"❌ Failed to process {table_name}: {e}")
            return False

def main():
    """Main function for testing GCS integration"""
    print("MySQL to GCS Data Extractor - Testing")
    print("=" * 50)
    
    # Initialize extractor
    extractor = MySQLToGCSExtractor()
    
    # Test MySQL connection
    try:
        connection = extractor.get_mysql_connection()
        print("✓ MySQL connection test successful")
        
        # Get a test table
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        test_table = tables[0][0]
        connection.close()
        
        print(f"Testing with table: {test_table}")
        
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        return
    
    # Test GCS connection
    print("\n" + "-" * 30)
    print("Testing GCS Connection...")
    print("-" * 30)
    
    gcs_available = extractor.initialize_gcs()
    
    if not gcs_available:
        print("⚠️  GCS not available. Will save locally only.")
        print("To enable GCS:")
        print("1. Update secrets.json with your GCS configuration")
        print("2. Set up service account key or gcloud auth")
    
    # Extract and process data
    print("\n" + "-" * 30)
    print("Extracting Data...")
    print("-" * 30)
    
    # Get test limit from config
    extraction_config = extractor.config_manager.get_extraction_config()
    test_limit = extraction_config.get('test_limit', 100)
    
    success = extractor.extract_and_upload_table(test_table, limit=test_limit)
    
    if success:
        print("\n✅ Extraction and upload completed successfully!")
        if gcs_available:
            print("📁 Data available both locally and in GCS")
        else:
            print("📁 Data available locally (configure GCS for cloud storage)")
    else:
        print("\n❌ Extraction failed!")

if __name__ == "__main__":
    main()
