"""
Base extractor class with common functionality for all MySQL extractors
"""
import json
import logging
import pandas as pd
import pymysql
from pathlib import Path
from datetime import datetime
from google.cloud import storage
from typing import Dict, Optional, List
import os

class BaseExtractor:
    """Base class for all MySQL extractors with common functionality"""
    
    def __init__(self, config_path: str = "config/config.json", secrets_path: str = "config/secrets.json"):
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
            logging.error(f"Config file not found: {config_path}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config file: {e}")
            raise
    
    def load_secrets(self, secrets_path: str) -> Dict:
        """Load secrets from JSON file"""
        try:
            with open(secrets_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Secrets file not found: {secrets_path}")
            logging.error("Please run 'python scripts/setup.py' to create the secrets file")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in secrets file: {e}")
            raise
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        
        # Create logs directory if it doesn't exist
        if log_config.get('log_to_file', False):
            log_file = log_config.get('log_file_path', 'logs/pipeline.log')
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_config.get('log_file_path', 'logs/pipeline.log')),
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
            credentials_path = gcp_config.get('service_account_key_path', '.keys/dwh-building-gcp.json')
            
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
        # Support both old and new config structure
        if 'database' in self.secrets:
            # Old structure
            mysql_config = self.secrets['database']
            mysql_config.update({
                'user': mysql_config.get('username', mysql_config.get('user')),
                'passwd': mysql_config.get('password')
            })
        else:
            # New structure
            mysql_config = self.secrets.get('mysql', {})
            mysql_config.update({
                'passwd': mysql_config.get('password')
            })
        
        db_config = self.config.get('database', {})
        
        return pymysql.connect(
            host=mysql_config.get('host', db_config.get('host', 'localhost')),
            port=mysql_config.get('port', db_config.get('port', 3306)),
            user=mysql_config.get('user', mysql_config.get('username')),
            passwd=mysql_config.get('passwd'),
            database=mysql_config.get('database', db_config.get('database')),
            connect_timeout=db_config.get('connection_timeout', 10),
            read_timeout=30,
            write_timeout=30
        )
    
    def get_table_columns(self, table_name: str) -> List[Dict]:
        """Get table column information"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                
                return [{
                    'name': col[0],
                    'type': col[1],
                    'null': col[2],
                    'key': col[3],
                    'default': col[4],
                    'extra': col[5]
                } for col in columns]
        finally:
            connection.close()
    
    def save_locally(self, df: pd.DataFrame, table_name: str, suffix: str = "") -> str:
        """Save DataFrame locally as Parquet"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{suffix}_{timestamp}.parquet" if suffix else f"{table_name}_{timestamp}.parquet"
        
        # Create directory structure
        extraction_config = self.config.get('extraction', {})
        base_path = Path(extraction_config.get('output_directory', 'extracted_data'))
        bronze_path = base_path / extraction_config.get('bronze_layer_path', 'bronze') / table_name
        bronze_path.mkdir(parents=True, exist_ok=True)
        
        file_path = bronze_path / filename
        df.to_parquet(file_path, index=False)
        
        self.logger.info(f"✓ Saved {len(df)} records to {file_path}")
        return str(file_path)
    
    def upload_to_gcs(self, local_path: str, table_name: str, extraction_type: str = "full") -> Optional[str]:
        """Upload file to Google Cloud Storage"""
        if not self.storage_client:
            self.logger.info("GCS client not available, skipping upload")
            return None
        
        try:
            gcp_config = self.secrets.get('gcp', {})
            bucket_name = gcp_config.get('bucket_name')
            
            if not bucket_name or bucket_name == "your-dwh-bucket":
                self.logger.warning("GCS bucket name not configured")
                return None
            
            # Create GCS path with date partitioning
            date_partition = datetime.now().strftime("%Y/%m/%d")
            filename = Path(local_path).name
            blob_path = f"bronze/pk_gest_xer/{table_name}/date={date_partition}/{filename}"
            
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
    
    def save_metadata(self, table_name: str, metadata: Dict):
        """Save extraction metadata"""
        # Create metadata directory
        extraction_config = self.config.get('extraction', {})
        base_path = Path(extraction_config.get('output_directory', 'extracted_data'))
        metadata_path = base_path / extraction_config.get('metadata_path', 'metadata')
        metadata_path.mkdir(parents=True, exist_ok=True)
        
        # Save metadata file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extraction_type = metadata.get('extraction_type', 'unknown')
        metadata_file = metadata_path / f"{table_name}_{extraction_type}_{timestamp}.json"
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
            
        self.logger.info(f"✓ Metadata saved to {metadata_file}")
        return str(metadata_file)
