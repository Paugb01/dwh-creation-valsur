"""
Worker to move specific tables from GCS to BigQuery bronze layer
"""
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from dotenv import load_dotenv
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from pathlib import Path
import os

# Load environment variables
load_dotenv()

class GCSToBigQueryWorker:
    """Worker class to handle GCS to BigQuery data movement"""

    def __init__(self, gcs_credentials=None, bq_credentials=None, config_path: str = None):
        # Use the correct path for config file
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "secrets.json")
        
        self.config = self._load_config(config_path)
        self.project_id = self.config['gcp']['project_id']
        self.bucket_name = self.config['gcp']['bucket_name']
        self.source_database = self.config['mysql']['database']

        # Store credentials for use in BigQuery operations
        self.gcs_credentials = gcs_credentials
        self.bq_credentials = bq_credentials
        
        # Get service account path for fallback
        service_account_path = self.config['gcp'].get('service_account_key_path')
        
        # Initialize clients with provided credentials or fallback to config/default
        if gcs_credentials:
            self.storage_client = storage.Client(project=self.project_id, credentials=gcs_credentials)
        else:
            # Fallback to service account from config or default authentication
            if service_account_path and os.path.exists(service_account_path):
                credentials = service_account.Credentials.from_service_account_file(service_account_path)
                self.storage_client = storage.Client(project=self.project_id, credentials=credentials)
                self.gcs_credentials = credentials  # Store for BigQuery use
            else:
                self.storage_client = storage.Client(project=self.project_id)
        
        if bq_credentials:
            self.bq_client = bigquery.Client(project=self.project_id, credentials=bq_credentials)
        else:
            # Fallback to service account from config or default authentication
            if service_account_path and os.path.exists(service_account_path):
                credentials = service_account.Credentials.from_service_account_file(service_account_path)
                self.bq_client = bigquery.Client(project=self.project_id, credentials=credentials)
                self.bq_credentials = credentials  # Store for consistency
            else:
                self.bq_client = bigquery.Client(project=self.project_id)
        
        # Setup logging
        self._setup_logging()
        
        # Define target tables
        self.target_tables = [
            'alm_his_1', 'alm_his_2', 'alm_pie_2', 
            'alm_pie_1', 'piezas_1', 'piezas_2'
        ]
        
        # Dataset configurations
        self.bronze_dataset = 'bronze1'
        self.silver_dataset = 'silver1'
        self.gold_dataset = 'gold1'
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Validate required keys
            required_keys = ['mysql', 'gcp']
            for key in required_keys:
                if key not in config:
                    raise KeyError(f"Required configuration key '{key}' not found in {config_path}")
            
            # Validate GCP configuration
            gcp_required = ['project_id', 'bucket_name']
            for key in gcp_required:
                if key not in config['gcp']:
                    raise KeyError(f"Required GCP configuration key '{key}' not found")
            
            return config
            
        except Exception as e:
            print(f"Error loading configuration: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging for the worker"""
        # Ensure logs directory exists
        logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        
        log_filename = f'gcs_to_bq_worker_{datetime.now().strftime("%Y%m%d")}.log'
        log_filepath = os.path.join(logs_dir, log_filename)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_datasets(self):
        """Create BigQuery datasets if they don't exist"""
        datasets = [self.bronze_dataset, self.silver_dataset, self.gold_dataset]
        
        for dataset_name in datasets:
            dataset_id = f"{self.project_id}.{dataset_name}"
            
            try:
                self.bq_client.get_dataset(dataset_id)
                self.logger.info(f"Dataset {dataset_name} already exists")
            except NotFound:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"  # Change to your preferred location
                dataset.description = f"{dataset_name.capitalize()} layer for data warehouse"
                
                dataset = self.bq_client.create_dataset(dataset)
                self.logger.info(f"Created dataset: {dataset_name}")
    
    def find_latest_gcs_files(self, table_name: str) -> Optional[str]:
        """Find the latest GCS file for a given table"""
        bucket = self.storage_client.bucket(self.bucket_name)
        prefix = f"bronze/{self.source_database}/{table_name}/"
        
        blobs = list(bucket.list_blobs(prefix=prefix))
        
        if not blobs:
            self.logger.warning(f"No files found for table {table_name} in GCS")
            return None
        
        # Filter for parquet files and sort by name (which includes date)
        parquet_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]
        
        if not parquet_blobs:
            self.logger.warning(f"No parquet files found for table {table_name}")
            return None
        
        # Get the most recent file (assumes chronological naming)
        latest_blob = sorted(parquet_blobs, key=lambda x: x.name)[-1]
        gcs_path = f"gs://{self.bucket_name}/{latest_blob.name}"
        
        self.logger.info(f"Found latest file for {table_name}: {gcs_path}")
        return gcs_path
    
    def load_table_to_bronze(self, table_name: str, gcs_path: str) -> bool:
        """Load a table from GCS to BigQuery bronze layer"""
        table_id = f"{self.project_id}.{self.bronze_dataset}.{table_name}"
        
        try:
            # Method 1: Try direct URI load first (works if BigQuery service account has access)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
            )
            
            try:
                load_job = self.bq_client.load_table_from_uri(
                    gcs_path, table_id, job_config=job_config
                )
                load_job.result()
                
                # Get table info
                table = self.bq_client.get_table(table_id)
                self.logger.info(f"Successfully loaded {table.num_rows:,} rows to bronze.{table_name}")
                return True
                
            except Exception as uri_error:
                self.logger.warning(f"URI load failed for {table_name}, trying alternative method: {uri_error}")
                
                # Method 2: Download from GCS and upload to BigQuery using pandas-gbq
                import pandas as pd
                import pandas_gbq
                import io
                
                # Extract blob path from GCS URI
                blob_path = gcs_path.replace(f"gs://{self.bucket_name}/", "")
                bucket = self.storage_client.bucket(self.bucket_name)
                blob = bucket.blob(blob_path)
                
                # Download blob content to memory
                self.logger.info(f"Downloading {blob_path} from GCS...")
                blob_data = blob.download_as_bytes()
                
                # Read Parquet data into DataFrame
                df = pd.read_parquet(io.BytesIO(blob_data))
                self.logger.info(f"Loaded {len(df):,} rows from Parquet file")
                
                # Upload DataFrame to BigQuery using pandas-gbq
                self.logger.info(f"Uploading data to BigQuery table {table_id}...")
                
                # Use pandas-gbq for better integration and future compatibility
                pandas_gbq.to_gbq(
                    df, 
                    table_id, 
                    project_id=self.project_id,
                    if_exists='replace',  # Equivalent to WRITE_TRUNCATE
                    credentials=self.bq_credentials if self.bq_credentials else None
                )
                
                # Get table info
                table = self.bq_client.get_table(table_id)
                self.logger.info(f"Successfully loaded {table.num_rows:,} rows to bronze.{table_name}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to load {table_name} to bronze: {e}")
            return False
    
    def run_bronze_ingestion(self, specific_tables: Optional[List[str]] = None) -> Dict[str, bool]:
        """Run the complete bronze ingestion process"""
        self.logger.info("Starting GCS to BigQuery bronze ingestion")
        
        # Create datasets
        self.create_datasets()
        
        # Determine which tables to process
        tables_to_process = specific_tables if specific_tables else self.target_tables
        results = {}
        
        for table_name in tables_to_process:
            self.logger.info(f"Processing table: {table_name}")
            
            # Find latest GCS file
            gcs_path = self.find_latest_gcs_files(table_name)
            
            if not gcs_path:
                results[table_name] = False
                continue
            
            # Load to bronze
            success = self.load_table_to_bronze(table_name, gcs_path)
            results[table_name] = success
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        self.logger.info(f"Bronze ingestion completed: {successful}/{total} tables successful")
        
        return results
    
    def get_table_info(self, dataset: str, table_name: str) -> Dict:
        """Get information about a BigQuery table"""
        try:
            table_id = f"{self.project_id}.{dataset}.{table_name}"
            table = self.bq_client.get_table(table_id)
            
            return {
                'exists': True,
                'num_rows': table.num_rows,
                'size_bytes': table.num_bytes,
                'created': table.created.isoformat() if table.created else None,
                'modified': table.modified.isoformat() if table.modified else None,
                'schema_fields': len(table.schema)
            }
        except NotFound:
            return {'exists': False}
        except Exception as e:
            self.logger.error(f"Error getting table info for {dataset}.{table_name}: {e}")
            return {'exists': False, 'error': str(e)}
    
    def validate_bronze_tables(self) -> Dict[str, Dict]:
        """Validate that all bronze tables exist and have data"""
        validation_results = {}
        
        for table_name in self.target_tables:
            info = self.get_table_info(self.bronze_dataset, table_name)
            validation_results[table_name] = info
            
            if info.get('exists'):
                self.logger.info(f"[OK] {table_name}: {info.get('num_rows', 0):,} rows")
            else:
                self.logger.warning(f"[MISSING] {table_name}: Table not found")
        
        return validation_results


if __name__ == "__main__":
    # Example usage
    # Option 1: Use explicit credentials
    # gcs_creds = service_account.Credentials.from_service_account_file("path/to/gcs-key.json")
    # bq_creds = service_account.Credentials.from_service_account_file("path/to/bq-key.json")
    # worker = GCSToBigQueryWorker(gcs_credentials=gcs_creds, bq_credentials=bq_creds)
    
    # Option 2: Use config file or default authentication
    worker = GCSToBigQueryWorker()
    
    # Run bronze ingestion
    results = worker.run_bronze_ingestion()
    
    # Validate results
    validation = worker.validate_bronze_tables()
    
    print("\n" + "="*60)
    print("BRONZE INGESTION SUMMARY")
    print("="*60)
    
    for table_name, success in results.items():
        status = "[OK] SUCCESS" if success else "[FAIL] FAILED"
        info = validation.get(table_name, {})
        rows = info.get('num_rows', 0) if info.get('exists') else 0
        print(f"{table_name:<15} {status:<10} {rows:>10,} rows")