"""
Simplified Batch Data Extractor (based on BaseExtractor)
Extracts data from MySQL database in batch mode with GCS upload support
"""
import json
import logging
import pandas as pd
import pymysql
from pathlib import Path
from datetime import datetime
from google.cloud import storage
from typing import Dict, List, Optional
import os

class BatchExtractor:
    """Simple batch extractor for full table extraction from MySQL with GCS support"""

    def __init__(self, config_path: str = "config/config.json", secrets_path: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "config/secrets.json")):
        self.config = self.load_config(config_path)
        self.secrets = self.load_secrets(secrets_path)
        self.setup_logging()
        self.storage_client = None
        self.setup_gcs()
        
    def load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Use default config if file not found
            print(f"Warning: Config file not found: {config_path}, using defaults")
            return {
                "source": "mysql_database",
                "destination": "extracted_data",
                "extraction": {
                    "output_format": "parquet",
                    "batch_size": 10000,
                    "enable_gcs_upload": True,
                    "output_directory": "extracted_data",
                    "bronze_layer_path": "bronze",
                    "metadata_path": "metadata"
                },
                "logging": {
                    "level": "INFO",
                    "log_to_file": True,
                    "log_file_path": "logs/batch_pipeline.log"
                }
            }
    
    def load_secrets(self, secrets_path: str) -> Dict:
        """Load secrets from JSON file"""
        try:
            print(f"DEBUG: Attempting to load secrets from: {secrets_path}")
            with open(secrets_path, 'r') as f:
                secrets = json.load(f)
            print(f"DEBUG: Secrets loaded successfully. MySQL host: {secrets.get('mysql', {}).get('host', 'NOT FOUND')}")
            return secrets
        except FileNotFoundError:
            print(f"Warning: Secrets file not found: {secrets_path}")
            print("Please create a secrets.json file with database and GCP credentials")
            return {
                "mysql": {
                    "host": "localhost",  # This is why it's defaulting to localhost!
                    "port": 3306,
                    "database": "your_database",
                    "username": "your_username",
                    "password": "your_password"
                },
                "gcp": {
                    "project_id": "your-project-id",
                    "bucket_name": "your-dwh-bucket",
                    "service_account_key_path": ".keys/service-account.json"
                }
            }
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        
        # Create logs directory if it doesn't exist
        if log_config.get('log_to_file', False):
            log_file = log_config.get('log_file_path', 'logs/batch_pipeline.log')
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_config.get('log_file_path', 'logs/batch_pipeline.log')),
                logging.StreamHandler()
            ] if log_config.get('log_to_file', False) else [logging.StreamHandler()]
        )
        
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def setup_gcs(self):
        """Setup Google Cloud Storage client"""
        if not self.config.get('extraction', {}).get('enable_gcs_upload', True):
            self.logger.info("GCS upload disabled in configuration")
            return
            
        try:
            gcp_config = self.secrets.get('gcp', {})
            credentials_path = gcp_config.get('service_account_key_path', '.keys/service-account.json')
            
            if Path(credentials_path).exists():
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                self.storage_client = storage.Client(project=gcp_config.get('project_id'))
                self.logger.info("✓ GCS client initialized successfully")
            else:
                self.logger.warning(f"GCS credentials file not found: {credentials_path}")
                self.logger.warning("GCS upload will be disabled")
                
        except Exception as e:
            self.logger.warning(f"Failed to initialize GCS client: {e}")
            self.logger.warning("Continuing with local storage only")
    
    def get_mysql_connection(self):
        """Create MySQL connection using PyMySQL"""
        mysql_config = self.secrets.get('mysql', {})
        
        # Debug output
        print(f"DEBUG: Full secrets object: {self.secrets}")
        print(f"DEBUG: MySQL config extracted: {mysql_config}")
        print(f"DEBUG: Host being used: {mysql_config.get('host', 'localhost')}")
        
        try:
            connection = pymysql.connect(
                host=mysql_config.get('host', 'localhost'),
                port=mysql_config.get('port', 3306),
                user=mysql_config.get('username'),
                passwd=mysql_config.get('password'),
                database=mysql_config.get('database'),
                connect_timeout=30,
                read_timeout=30,
                write_timeout=30
            )
            print("✓ Connected to MySQL database successfully!")
            return connection
        except Exception as e:
            print(f"❌ Failed to connect to MySQL: {e}")
            raise
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables_result = cursor.fetchall()
                
                # Extract table names from tuples (default cursor returns tuples)
                tables = [table[0] for table in tables_result]
                    
                self.logger.info(f"Found {len(tables)} tables in database")
                return sorted(tables)
        finally:
            connection.close()
    
    def extract_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """Extract data from a single table"""
        connection = self.get_mysql_connection()
        try:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
            
            self.logger.info(f"Extracting data from table: {table_name}")
            df = pd.read_sql(query, connection)
            self.logger.info(f"Extracted {len(df)} rows from {table_name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from {table_name}: {e}")
            raise
        finally:
            connection.close()
    
    def save_locally(self, df: pd.DataFrame, table_name: str, suffix: str = "") -> str:
        """Save DataFrame locally as Parquet (following BaseExtractor structure)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{suffix}_{timestamp}.parquet" if suffix else f"{table_name}_{timestamp}.parquet"
        
        # Create directory structure like BaseExtractor
        extraction_config = self.config.get('extraction', {})
        base_path = Path(extraction_config.get('output_directory', 'extracted_data'))
        bronze_path = base_path / extraction_config.get('bronze_layer_path', 'bronze') / table_name
        bronze_path.mkdir(parents=True, exist_ok=True)
        
        file_path = bronze_path / filename
        df.to_parquet(file_path, index=False)
        
        self.logger.info(f"✓ Saved {len(df)} records to {file_path}")
        return str(file_path)
    
    def upload_to_gcs(self, local_path: str, table_name: str, extraction_type: str = "batch") -> Optional[str]:
        """Upload file to Google Cloud Storage (following BaseExtractor structure)"""
        if not self.storage_client:
            self.logger.info("GCS client not available, skipping upload")
            return None
        
        try:
            gcp_config = self.secrets.get('gcp', {})
            bucket_name = gcp_config.get('bucket_name')
            
            if not bucket_name or bucket_name == "your-dwh-bucket":
                self.logger.warning("GCS bucket name not configured")
                return None
            
            # Create GCS path with date partitioning (same structure as BaseExtractor)
            date_partition = datetime.now().strftime("%Y/%m/%d")
            filename = Path(local_path).name
            database_name = self.secrets.get('mysql', {}).get('database', 'unknown_db')
            blob_path = f"bronze/{database_name}/{table_name}/date={date_partition}/{filename}"
            
            # Upload file
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            
            gcs_path = f"gs://{bucket_name}/{blob_path}"
            self.logger.info(f"✓ Uploaded to {gcs_path}")
            return gcs_path
            
        except Exception as e:
            self.logger.error(f"Failed to upload to GCS: {e}")
            return None
    
    def save_metadata(self, table_name: str, metadata: Dict) -> str:
        """Save extraction metadata (following BaseExtractor structure)"""
        # Create metadata directory
        extraction_config = self.config.get('extraction', {})
        base_path = Path(extraction_config.get('output_directory', 'extracted_data'))
        metadata_path = base_path / extraction_config.get('metadata_path', 'metadata')
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Save metadata file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extraction_type = metadata.get('extraction_type', 'batch')
        metadata_file = metadata_path / f"{table_name}_{extraction_type}_{timestamp}.json"
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
            
        self.logger.info(f"✓ Metadata saved to {metadata_file}")
        return str(metadata_file)
    
    def extract_table_with_upload(self, table_name: str, limit: Optional[int] = None) -> Dict:
        """Extract table, save locally, upload to GCS, and save metadata"""
        try:
            # Extract data
            df = self.extract_table(table_name, limit=limit)
            
            if df.empty:
                self.logger.warning(f"Table {table_name} is empty, skipping")
                return {'success': False, 'reason': 'empty_table'}
            
            # Save locally
            local_path = self.save_locally(df, table_name, suffix="batch")
            
            # Upload to GCS
            gcs_path = self.upload_to_gcs(local_path, table_name, extraction_type="batch")
            
            # Create and save metadata
            metadata = {
                'table_name': table_name,
                'extraction_type': 'batch',
                'extraction_timestamp': datetime.now().isoformat(),
                'records_extracted': len(df),
                'local_path': local_path,
                'gcs_path': gcs_path,
                'limit_applied': limit,
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict()
            }
            
            metadata_path = self.save_metadata(table_name, metadata)
            
            return {
                'success': True,
                'table_name': table_name,
                'records': len(df),
                'local_path': local_path,
                'gcs_path': gcs_path,
                'metadata_path': metadata_path
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process table {table_name}: {e}")
            return {'success': False, 'table_name': table_name, 'error': str(e)}
    
    def extract_all_tables(self, limit_per_table: Optional[int] = None) -> Dict[str, Dict]:
        """Extract all tables with full processing (local save + GCS upload + metadata)"""
        tables = self.get_all_tables()
        results = {}
        
        self.logger.info(f"Starting batch extraction of {len(tables)} tables")
        
        for i, table_name in enumerate(tables, 1):
            self.logger.info(f"Processing table {i}/{len(tables)}: {table_name}")
            result = self.extract_table_with_upload(table_name, limit=limit_per_table)
            results[table_name] = result
        
        # Summary
        successful = sum(1 for r in results.values() if r.get('success', False))
        total_records = sum(r.get('records', 0) for r in results.values() if r.get('success', False))
        
        self.logger.info(f"Batch extraction completed: {successful}/{len(tables)} tables successful, {total_records:,} total records")
        return results
    
    def extract(self, source: str, destination: str, table_name: Optional[str] = None, 
               limit: Optional[int] = None) -> bool:
        """Main extraction method (for compatibility with run_batch.py)"""
        try:
            if table_name:
                # Extract single table
                result = self.extract_table_with_upload(table_name, limit=limit)
                return result.get('success', False)
            else:
                # Extract all tables
                results = self.extract_all_tables(limit_per_table=limit)
                successful = sum(1 for r in results.values() if r.get('success', False))
                return successful > 0
            
        except Exception as e:
            self.logger.error(f"Batch extraction failed: {e}")
            return False
