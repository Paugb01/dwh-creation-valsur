"""
GCS to BigQuery Worker
Optimized for single service account usage and improved readability
"""
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from dotenv import load_dotenv
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
import os
import sys

# Add utils path for credential management
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils import get_credentials, load_configuration, setup_environment

# Load environment variables
load_dotenv()


class GCSToBigQueryWorker:
    """
    Worker class to handle GCS to BigQuery data movement
    Uses single service account for all GCP operations
    """

    def __init__(self, credentials: Optional[service_account.Credentials] = None, 
                 config_path: Optional[str] = None, config: Optional[Dict] = None):
        """
        Initialize the worker with credentials and configuration
        
        Args:
            credentials: Service account credentials (optional, will auto-detect if None)
            config_path: Path to config file (optional)
            config: Config dictionary (optional)
        """
        # Setup environment
        setup_environment()
        
        # Initialize logging first
        self.logger = self._setup_logging()
        
        # Load configuration
        self.config = self._load_configuration(config, config_path)
        
        # Extract project and bucket info
        self.project_id = self.config['gcp']['project_id']
        self.bucket_name = self.config['gcp']['bucket_name']
        
        # Initialize credentials (single service account for all services)
        self.credentials = credentials or get_credentials()
        if self.credentials:
            self.logger.info("Service account credentials loaded successfully")
        else:
            self.logger.warning("No credentials found, using default authentication")
        
        # Initialize Google Cloud clients
        self._initialize_clients()
        
        # Setup dataset and table configurations
        self._setup_datasets()
    
    def _load_configuration(self, config: Optional[Dict], config_path: Optional[str]) -> Dict:
        """Load configuration from various sources"""
        if config is not None:
            self.logger.info("Using provided configuration")
            return config
        
        if config_path is not None:
            return self._load_config_file(config_path)
        
        # Try environment-based loading
        config = load_configuration('SECRETS')
        if config:
            self.logger.info("Configuration loaded from environment")
            return config
        
        # Fallback to default path
        fallback_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "secrets.json")
        self.logger.info(f"Using fallback configuration: {fallback_path}")
        return self._load_config_file(fallback_path)
    
    def _load_config_file(self, config_path: str) -> Dict:
        """Load configuration from JSON file with validation"""
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Validate required configuration
            self._validate_config(config)
            self.logger.info(f"Configuration loaded from: {config_path}")
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise
    
    def _validate_config(self, config: Dict):
        """Validate configuration structure"""
        required_keys = ['gcp']
        for key in required_keys:
            if key not in config:
                raise KeyError(f"Required configuration key '{key}' not found")
        
        gcp_required = ['project_id', 'bucket_name']
        for key in gcp_required:
            if key not in config['gcp']:
                raise KeyError(f"Required GCP configuration key '{key}' not found")
    
    def _initialize_clients(self):
        """Initialize Google Cloud clients with single service account"""
        try:
            # Initialize clients with shared credentials
            if self.credentials:
                self.storage_client = storage.Client(
                    project=self.project_id, 
                    credentials=self.credentials
                )
                self.bq_client = bigquery.Client(
                    project=self.project_id,
                    credentials=self.credentials
                )
            else:
                # Use default authentication
                self.storage_client = storage.Client(project=self.project_id)
                self.bq_client = bigquery.Client(project=self.project_id)
            
            self.logger.info("Google Cloud clients initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize cloud clients: {e}")
            raise
    
    def _setup_datasets(self):
        """Setup dataset and table configurations from config file"""
        # Read BigQuery configuration from config file
        bq_config = self.config.get('bigquery', {})
        
        # Dataset names
        datasets = bq_config.get('datasets', {})
        self.bronze_dataset = datasets.get('bronze', 'bronze1')
        self.silver_dataset = datasets.get('silver', 'silver1') 
        self.gold_dataset = datasets.get('gold', 'gold1')
        
        # Source database name (should match what extractor uses)
        self.source_database = bq_config.get('source_database', 'pk_gest_xer')
        
        # Target tables to process
        self.target_tables = bq_config.get('target_tables', [
            'alm_his_1', 'alm_his_2', 'alm_pie_2',
            'alm_pie_1', 'piezas_1', 'piezas_2'
        ])
        
        # BigQuery location
        self.bq_location = bq_config.get('location', 'US')
        
        self.logger.info(f"Configured datasets - Bronze: {self.bronze_dataset}, Silver: {self.silver_dataset}, Gold: {self.gold_dataset}")
        self.logger.info(f"Source database: {self.source_database}")
        self.logger.info(f"Target tables: {len(self.target_tables)} tables configured")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
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
        return logging.getLogger(self.__class__.__name__)
    
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
                dataset.location = self.bq_location  # Use configured location
                dataset.description = f"{dataset_name.capitalize()} layer for data warehouse"
                
                dataset = self.bq_client.create_dataset(dataset)
                self.logger.info(f"Created dataset: {dataset_name} in {self.bq_location}")
        
    def find_latest_gcs_file(self, table_name: str) -> Optional[str]:
        """Find the latest GCS file for a given table"""
        bucket = self.storage_client.bucket(self.bucket_name)
        prefix = f"bronze/{self.source_database}/{table_name}/"
        
        try:
            blobs = list(bucket.list_blobs(prefix=prefix))
            
            if not blobs:
                self.logger.warning(f"No files found for table {table_name} in GCS")
                return None
            
            # Filter for parquet files and get the most recent
            parquet_blobs = [blob for blob in blobs if blob.name.endswith('.parquet')]
            
            if not parquet_blobs:
                self.logger.warning(f"No parquet files found for table {table_name}")
                return None
            
            latest_blob = sorted(parquet_blobs, key=lambda x: x.name)[-1]
            gcs_path = f"gs://{self.bucket_name}/{latest_blob.name}"
            
            self.logger.info(f"Found latest file for {table_name}: {gcs_path}")
            return gcs_path
            
        except Exception as e:
            self.logger.error(f"Error finding GCS files for {table_name}: {e}")
            return None
    
    def load_table_to_bronze(self, table_name: str, gcs_path: str) -> bool:
        """Load a table from GCS to BigQuery bronze layer"""
        table_id = f"{self.project_id}.{self.bronze_dataset}.{table_name}"
        
        try:
            # Get load configuration from config file
            load_config_settings = self.config.get('bigquery', {}).get('load_config', {})
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                source_format=getattr(bigquery.SourceFormat, load_config_settings.get('source_format', 'PARQUET')),
                write_disposition=getattr(bigquery.WriteDisposition, load_config_settings.get('write_disposition', 'WRITE_TRUNCATE')),
                autodetect=load_config_settings.get('autodetect', True),
                create_disposition=getattr(bigquery.CreateDisposition, load_config_settings.get('create_disposition', 'CREATE_IF_NEEDED'))
            )
            
            # Try direct URI load first
            try:
                load_job = self.bq_client.load_table_from_uri(gcs_path, table_id, job_config=job_config)
                load_job.result()
                
                table = self.bq_client.get_table(table_id)
                self.logger.info(f"Successfully loaded {table.num_rows:,} rows to bronze.{table_name}")
                return True
                
            except Exception as uri_error:
                self.logger.warning(f"URI load failed for {table_name}, trying alternative method: {uri_error}")
                return self._load_via_pandas(table_name, gcs_path, table_id)
                
        except Exception as e:
            self.logger.error(f"Failed to load {table_name} to bronze: {e}")
            return False
    
    def _load_via_pandas(self, table_name: str, gcs_path: str, table_id: str) -> bool:
        """Alternative loading method using pandas for better compatibility"""
        try:
            import pandas as pd
            import pandas_gbq
            import io
            
            # Download file from GCS
            blob_path = gcs_path.replace(f"gs://{self.bucket_name}/", "")
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(blob_path)
            
            self.logger.info(f"Downloading {blob_path} from GCS...")
            blob_data = blob.download_as_bytes()
            
            # Read parquet data
            df = pd.read_parquet(io.BytesIO(blob_data))
            self.logger.info(f"Loaded {len(df):,} rows from Parquet file")
            
            # Upload to BigQuery
            pandas_gbq.to_gbq(
                df, 
                table_id, 
                project_id=self.project_id,
                if_exists='replace',
                credentials=self.credentials
            )
            
            table = self.bq_client.get_table(table_id)
            self.logger.info(f"Successfully loaded {table.num_rows:,} rows to bronze.{table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Pandas loading failed for {table_name}: {e}")
            return False
    
    def run_bronze_ingestion(self, specific_tables: Optional[List[str]] = None) -> Dict[str, bool]:
        """Run the complete bronze ingestion process"""
        self.logger.info("Starting GCS to BigQuery bronze ingestion")
        
        # Create datasets
        self.create_datasets()
        
        # Process tables
        tables_to_process = specific_tables or self.target_tables
        results = {}
        
        for table_name in tables_to_process:
            self.logger.info(f"Processing table: {table_name}")
            
            gcs_path = self.find_latest_gcs_file(table_name)
            if not gcs_path:
                results[table_name] = False
                continue
            
            success = self.load_table_to_bronze(table_name, gcs_path)
            results[table_name] = success
        
        # Log summary
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
    """
    Main execution script
    """
    print("Starting GCS to BigQuery Worker")
    print("=" * 60)
    
    try:
        # Initialize worker with automatic credential detection
        worker = GCSToBigQueryWorker()
        
        print(f"Project: {worker.project_id}")
        print(f"Bucket: {worker.bucket_name}")
        print(f"Target Tables: {len(worker.target_tables)}")
        print("-" * 60)
        
        # Run bronze ingestion
        print("Starting bronze ingestion...")
        results = worker.run_bronze_ingestion()
        
        # Validate results
        print("\nValidating results...")
        validation = worker.validate_bronze_tables()
        
        # Summary report
        print("\n" + "=" * 60)
        print("BRONZE INGESTION SUMMARY")
        print("=" * 60)
        
        successful_tables = 0
        total_rows = 0
        
        for table_name, success in results.items():
            status = "SUCCESS" if success else "FAILED"
            info = validation.get(table_name, {})
            rows = info.get('num_rows', 0) if info.get('exists') else 0
            
            if success:
                successful_tables += 1
                total_rows += rows
                
            print(f"{table_name:<20} {status:<10} {rows:>10,} rows")
        
        print("-" * 60)
        print(f"TOTAL SUCCESS: {successful_tables}/{len(results)} tables")
        print(f"TOTAL ROWS: {total_rows:,}")
        
        if successful_tables == len(results):
            print("ALL TABLES PROCESSED SUCCESSFULLY!")
        else:
            print("SOME TABLES FAILED - CHECK LOGS FOR DETAILS")
            
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()