"""
Utility functions for the Data Warehouse Pipeline DAG
Handles initialization of extractors and workers with proper configuration
"""
import os
import sys
import logging
from typing import Any

# Add batch_only modules to path
batch_only_path = '/home/airflow/gcs/dags/batch_only'
sys.path.append(batch_only_path)

def get_batch_extractor():
    """Initialize and return BatchExtractor with Composer-specific configuration"""
    from batch_extractor import BatchExtractor
    
    try:
        # Use Composer-specific paths
        config_path = "/home/airflow/gcs/data/config.json"
        secrets_path = "/home/airflow/gcs/data/secrets.json"
        
        # Verify files exist
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        if not os.path.exists(secrets_path):
            raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
        
        extractor = BatchExtractor(
            config_path=config_path,
            secrets_path=secrets_path
        )
        
        logging.info("BatchExtractor initialized successfully")
        return extractor
        
    except Exception as e:
        logging.error(f"Failed to initialize BatchExtractor: {e}")
        raise

def get_gcs_to_bq_worker():
    """Initialize and return GCSToBigQueryWorker with proper credentials"""
    from main import GCSToBigQueryWorker
    from google.oauth2 import service_account
    
    try:
        # Load credentials from Composer data folder
        service_account_path = "/home/airflow/gcs/data/service-account.json"
        
        if os.path.exists(service_account_path):
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            worker = GCSToBigQueryWorker(
                gcs_credentials=credentials,
                bq_credentials=credentials,
                config_path="/home/airflow/gcs/data/secrets.json"
            )
        else:
            # Fallback to default authentication (if running on GCE with proper IAM)
            worker = GCSToBigQueryWorker(
                config_path="/home/airflow/gcs/data/secrets.json"
            )
        
        logging.info("GCSToBigQueryWorker initialized successfully")
        return worker
        
    except Exception as e:
        logging.error(f"Failed to initialize GCSToBigQueryWorker: {e}")
        raise

def get_pipeline_config() -> dict:
    """Get pipeline configuration"""
    return {
        'target_tables': [
            'alm_his_1', 'alm_his_2', 'alm_pie_2', 
            'alm_pie_1', 'piezas_1', 'piezas_2'
        ],
        'bronze_dataset': 'bronze1',
        'silver_dataset': 'silver1',
        'gold_dataset': 'gold1',
        'gcp_project_id': 'dwh-building',
        'gcs_bucket': 'valsurtruck-dwh-bronze',
        'mysql_database': 'pk_gest_xer'
    }
