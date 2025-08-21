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
from datetime import datetime, date
from typing import List, Dict, Optional
import os
import sys

# Add utils path for credential management
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from core.utils import read_file_safely

def get_credentials():
    """Load credentials from secrets.json and create proper Google service account credentials"""
    try:
        secrets_path = os.path.join(project_root, "config", "secrets.json")
        with open(secrets_path, 'r') as f:
            secrets_data = json.load(f)
        
        # Extract service account info from secrets
        service_account_info = secrets_data.get('gcp', {}).get('service_account_json', {})
        
        if service_account_info:
            # Create credentials from service account info
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/devstorage.read_write'
                ]
            )
            return credentials
        else:
            # Fallback to environment-based authentication
            return None
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

def load_configuration():
    """Load configuration from config.json"""
    config_path = os.path.join(project_root, "config", "config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

# Load environment variables
load_dotenv()


class GCSToBigQueryWorker:
    """
    Worker class to handle GCS to BigQuery data movement
    Uses single service account for all GCP operations
    """

    def __init__(self, config=None, secrets=None, credentials: Optional[service_account.Credentials] = None, 
                 config_path: Optional[str] = None):
        """
        Initialize the worker with credentials and configuration
        
        Args:
            config: Config dictionary (can be dict or path to config file)
            secrets: Secrets dictionary (can be dict or path to secrets file)
            credentials: Service account credentials (optional, will auto-detect if None)
            config_path: Path to config file (optional, for backwards compatibility)
        """
        # Initialize logging first
        self.logger = self._setup_logging()
        
        # Load configuration - handle both dict and file path
        if isinstance(config, dict):
            self.config = config
            self.logger.info("Using provided configuration dictionary")
        else:
            self.config = self._load_configuration(config, config_path)
        
        # Load secrets - handle both dict and file path
        if isinstance(secrets, dict):
            self.secrets = secrets
            self.logger.info("Using provided secrets dictionary")
        else:
            # Load secrets for GCP configuration
            self.secrets = self._load_secrets()
        
        # Extract project and bucket info from secrets with environment fallback
        self.project_id = self.secrets.get('gcp', {}).get('project_id') or os.getenv('GCP_PROJECT_ID', 'dwh-building')
        self.bucket_name = self.secrets.get('gcp', {}).get('bucket_name') or os.getenv('GCS_BUCKET', 'valsurtruck-dwh-bronze')
        
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
        
        # Try to load from local config/config.json first
        local_config_path = "config/config.json"
        if os.path.exists(local_config_path):
            self.logger.info(f"Loading configuration from: {local_config_path}")
            return self._load_config_file(local_config_path)
        
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
        required_keys = ['bigquery', 'gcs']
        for key in required_keys:
            if key not in config:
                raise KeyError(f"Required configuration key '{key}' not found")
    
    def _load_secrets(self) -> Dict:
        """Load secrets configuration for GCP credentials and connection info"""
        try:
            # Try multiple secret file locations
            possible_paths = [
                "config/secrets.json",
                os.path.join(os.path.dirname(__file__), "config", "secrets.json"),
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "secrets.json")
            ]
            
            for secrets_path in possible_paths:
                if os.path.exists(secrets_path):
                    with open(secrets_path, 'r') as f:
                        secrets = json.load(f)
                    self.logger.info(f"Secrets loaded from: {secrets_path}")
                    return secrets
            
            # If no secrets file found, return empty dict with warning
            self.logger.warning("No secrets.json file found, using environment variables and defaults")
            return {}
            
        except Exception as e:
            self.logger.error(f"Error loading secrets: {e}")
            return {}
    
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
        self.bq_location = bq_config.get('location', 'EU')
        
        # Load table strategies from config
        self.table_strategies = self.config.get('table_strategies', {})
        
        self.logger.info(f"Configured datasets - Bronze: {self.bronze_dataset}, Silver: {self.silver_dataset}, Gold: {self.gold_dataset}")
        self.logger.info(f"Source database: {self.source_database}")
        self.logger.info(f"Target tables: {len(self.target_tables)} tables configured")
        self.logger.info(f"Table strategies: {len(self.table_strategies)} strategies loaded")
    
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

    # =============== NEW ADVANCED INGESTION METHODS ===============
    
    def list_gcs_uris_for_day(self, table_name: str, day: datetime.date) -> List[str]:
        """List GCS URIs for a specific day partition"""
        prefix = (f"bronze/{self.source_database}/{table_name}/"
                  f"year={day:%Y}/month={day:%m}/day={day:%d}/")
        
        bucket = self.storage_client.bucket(self.bucket_name)
        uris = []
        
        try:
            for blob in bucket.list_blobs(prefix=prefix):
                if blob.name.endswith(".parquet"):
                    uris.append(f"gs://{self.bucket_name}/{blob.name}")
            
            self.logger.info(f"Found {len(uris)} files for {table_name} on {day}")
            return uris
            
        except Exception as e:
            self.logger.error(f"Error listing GCS files for {table_name} on {day}: {e}")
            return []
    
    def load_staging_from_uris(self, table_name: str, uris: List[str]) -> str:
        """Load data from GCS URIs into a staging table"""
        staging_table_name = f"{table_name}__stg"
        staging_table = f"{self.project_id}.{self.bronze_dataset}.{staging_table_name}"
        
        if not uris:
            raise ValueError(f"No URIs provided for {table_name}")
        
        try:
            # Use table reference object instead of string
            table_ref = self.bq_client.dataset(self.bronze_dataset).table(staging_table_name)
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
            )
            
            job = self.bq_client.load_table_from_uri(
                uris, table_ref, job_config=job_config, location=self.bq_location
            )
            job.result()
            
            # Get loaded row count
            table = self.bq_client.get_table(table_ref)
            self.logger.info(f"Loaded {table.num_rows:,} rows into staging table {staging_table}")
            
            return staging_table
            
        except Exception as e:
            self.logger.error(f"Failed to load staging table {staging_table}: {e}")
            raise
    
    def ensure_table_exists(self, table_id: str, ddl_sql: str):
        """Execute DDL to ensure table exists"""
        try:
            self.bq_client.query(ddl_sql, location=self.bq_location).result()
            self.logger.info(f"Ensured table exists: {table_id}")
        except Exception as e:
            self.logger.error(f"Failed to create table {table_id}: {e}")
            raise
    
    def execute_sql(self, sql: str, params: Optional[Dict] = None) -> int:
        """Execute SQL and return number of affected rows"""
        try:
            job_config = bigquery.QueryJobConfig()
            if params:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
                ]
            
            job = self.bq_client.query(sql, job_config=job_config, location=self.bq_location)
            result = job.result()
            
            # Get affected rows if available
            affected_rows = getattr(job, 'num_dml_affected_rows', 0) or 0
            self.logger.info(f"SQL executed successfully, affected rows: {affected_rows}")
            
            return affected_rows
            
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            raise
    
    def apply_incremental_merge(self, table_name: str, staging_table: str, strategy: Dict) -> int:
        """Apply incremental merge strategy for movement tables"""
        dest_table = f"{self.project_id}.{self.silver_dataset}.{table_name}"
        
        # Extract strategy configuration
        pk_cols = strategy.get('pk', ['id'])
        event_ts = strategy.get('event_ts', 'f_fecha')
        cluster_cols = strategy.get('cluster_by', [])
        
        # Create destination table if not exists
        cluster_clause = f"CLUSTER BY {', '.join(cluster_cols)}" if cluster_cols else ""
        
        ddl_sql = f"""
        CREATE TABLE IF NOT EXISTS `{dest_table}`
        PARTITION BY DATE({event_ts})
        {cluster_clause}
        AS SELECT * FROM `{staging_table}` WHERE 1=0
        """
        
        self.ensure_table_exists(dest_table, ddl_sql)
        
        # Build merge conditions
        pk_conditions = " AND ".join([f"T.{col} = S.{col}" for col in pk_cols])
        
        # Execute merge
        merge_sql = f"""
        MERGE `{dest_table}` T
        USING `{staging_table}` S
        ON {pk_conditions}
        WHEN MATCHED THEN UPDATE SET * EXCEPT({', '.join(pk_cols)})
        WHEN NOT MATCHED THEN INSERT ROW
        """
        
        affected_rows = self.execute_sql(merge_sql)
        self.logger.info(f"Incremental merge completed for {table_name}: {affected_rows} rows affected")
        
        return affected_rows
    
    def apply_replace_partition(self, table_name: str, staging_table: str, strategy: Dict, load_day: datetime.date) -> int:
        """Apply replace partition strategy for inventory tables"""
        dest_table = f"{self.project_id}.{self.silver_dataset}.{table_name}"
        
        # Extract strategy configuration
        partition_field = strategy.get('partition_field', 'snapshot_date')
        cluster_cols = strategy.get('cluster_by', [])
        
        # Create destination table if not exists
        cluster_clause = f"CLUSTER BY {', '.join(cluster_cols)}" if cluster_cols else ""
        
        ddl_sql = f"""
        CREATE TABLE IF NOT EXISTS `{dest_table}`
        PARTITION BY {partition_field}
        {cluster_clause}
        AS SELECT * FROM `{staging_table}` WHERE 1=0
        """
        
        self.ensure_table_exists(dest_table, ddl_sql)
        
        # Delete existing partition
        delete_sql = f"""
        DELETE FROM `{dest_table}` 
        WHERE {partition_field} = DATE('{load_day}')
        """
        
        self.execute_sql(delete_sql)
        
        # Insert new data
        insert_sql = f"""
        INSERT INTO `{dest_table}`
        SELECT 
            S.* EXCEPT(_load_day),
            DATE('{load_day}') AS {partition_field}
        FROM `{staging_table}` S
        """
        
        affected_rows = self.execute_sql(insert_sql)
        self.logger.info(f"Replace partition completed for {table_name}: {affected_rows} rows inserted")
        
        return affected_rows
    
    def apply_upsert_scd1(self, table_name: str, staging_table: str, strategy: Dict) -> int:
        """Apply SCD1 upsert strategy for catalog tables"""
        dest_table = f"{self.project_id}.{self.silver_dataset}.{table_name}"
        
        # Extract strategy configuration
        pk_cols = strategy.get('pk', ['cc_cod_pie'])
        updated_at = strategy.get('updated_at', 'f_fec_mod')
        cluster_cols = strategy.get('cluster_by', [])
        
        # Create destination table if not exists
        cluster_clause = f"CLUSTER BY {', '.join(cluster_cols)}" if cluster_cols else ""
        partition_clause = f"PARTITION BY DATE({updated_at})" if updated_at else ""
        
        ddl_sql = f"""
        CREATE TABLE IF NOT EXISTS `{dest_table}`
        {partition_clause}
        {cluster_clause}
        AS SELECT * FROM `{staging_table}` WHERE 1=0
        """
        
        self.ensure_table_exists(dest_table, ddl_sql)
        
        # Build merge conditions
        pk_conditions = " AND ".join([f"T.{col} = S.{col}" for col in pk_cols])
        
        # Execute merge
        merge_sql = f"""
        MERGE `{dest_table}` T
        USING `{staging_table}` S
        ON {pk_conditions}
        WHEN MATCHED THEN UPDATE SET * EXCEPT({', '.join(pk_cols)})
        WHEN NOT MATCHED THEN INSERT ROW
        """
        
        affected_rows = self.execute_sql(merge_sql)
        self.logger.info(f"SCD1 upsert completed for {table_name}: {affected_rows} rows affected")
        
        return affected_rows
    
    def run_advanced_ingestion(self, day: datetime.date, specific_tables: Optional[List[str]] = None) -> Dict[str, Dict]:
        """Run advanced ingestion with per-table strategies"""
        self.logger.info(f"Starting advanced ingestion for {day}")
        
        # Create datasets
        self.create_datasets()
        
        # Process tables
        tables_to_process = specific_tables or self.target_tables
        results = {}
        
        for table_name in tables_to_process:
            self.logger.info(f"Processing {table_name} with advanced strategy")
            
            try:
                # Get strategy for this table
                strategy = self.table_strategies.get(table_name)
                if not strategy:
                    self.logger.warning(f"No strategy defined for {table_name}, skipping")
                    results[table_name] = {'success': False, 'error': 'No strategy defined'}
                    continue
                
                # Get GCS files for the day
                uris = self.list_gcs_uris_for_day(table_name, day)
                if not uris:
                    self.logger.warning(f"No files found for {table_name} on {day}")
                    results[table_name] = {'success': False, 'error': 'No files found'}
                    continue
                
                # Load to staging
                staging_table = self.load_staging_from_uris(table_name, uris)
                
                # Apply strategy
                strategy_type = strategy.get('strategy')
                affected_rows = 0
                
                if strategy_type == 'incremental_merge':
                    affected_rows = self.apply_incremental_merge(table_name, staging_table, strategy)
                elif strategy_type == 'replace_partition':
                    affected_rows = self.apply_replace_partition(table_name, staging_table, strategy, day)
                elif strategy_type == 'upsert_scd1':
                    affected_rows = self.apply_upsert_scd1(table_name, staging_table, strategy)
                else:
                    raise ValueError(f"Unknown strategy: {strategy_type}")
                
                results[table_name] = {
                    'success': True, 
                    'strategy': strategy_type,
                    'affected_rows': affected_rows,
                    'files_processed': len(uris)
                }
                
            except Exception as e:
                self.logger.error(f"Failed to process {table_name}: {e}")
                results[table_name] = {'success': False, 'error': str(e)}
        
        # Log summary
        successful = sum(1 for r in results.values() if r.get('success'))
        total = len(results)
        self.logger.info(f"Advanced ingestion completed: {successful}/{total} tables successful")
        
        return results


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
        
        # Option 1: Run basic bronze ingestion (legacy mode)
        print("Starting bronze ingestion...")
        results = worker.run_bronze_ingestion()
        
        # Option 2: Run advanced ingestion with strategies (commented out for demo)
        # from datetime import date
        # today = date.today()
        # print(f"Starting advanced ingestion for {today}...")
        # advanced_results = worker.run_advanced_ingestion(today)
        
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