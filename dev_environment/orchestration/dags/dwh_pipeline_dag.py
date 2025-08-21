"""
Data Warehouse Pipeline DAG for Google Cloud Composer
Orchestrates MySQL -> GCS -> BigQuery bronze layer pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import logging
import os
import sys

# Add project paths for imports
sys.path.append('/home/airflow/gcs/dags')

# DAG Configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'dwh_pipeline',
    default_args=default_args,
    description='Data Warehouse Pipeline: MySQL -> GCS -> BigQuery Bronze',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    max_active_runs=1,
    tags=['data-warehouse', 'etl', 'mysql', 'bigquery']
)

# Target tables configuration
TARGET_TABLES = [
    'alm_his_1', 'alm_his_2', 'alm_pie_2', 
    'alm_pie_1', 'piezas_1', 'piezas_2'
]

def mysql_to_gcs_extraction(**context):
    """Extract data from MySQL and upload to GCS"""
    from dwh_pipeline_utils import get_batch_extractor
    
    logging.info("Starting MySQL to GCS extraction")
    
    try:
        extractor = get_batch_extractor()
        results = extractor.extract_all_tables(limit_per_table=None)
        
        # Process results
        successful_tables = [name for name, result in results.items() if result.get('success', False)]
        failed_tables = [name for name, result in results.items() if not result.get('success', False)]
        total_records = sum(result.get('records', 0) for result in results.values() if result.get('success', False))
        
        logging.info(f"Extraction completed: {len(successful_tables)}/{len(results)} tables successful")
        logging.info(f"Total records extracted: {total_records:,}")
        
        if failed_tables:
            logging.warning(f"Failed tables: {failed_tables}")
        
        # Store results for next task
        context['task_instance'].xcom_push(
            key='extraction_results',
            value={
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'total_records': total_records,
                'extraction_timestamp': context['execution_date'].isoformat()
            }
        )
        
        if len(failed_tables) > 0:
            raise Exception(f"Some tables failed extraction: {failed_tables}")
            
        return f"Successfully extracted {len(successful_tables)} tables with {total_records:,} total records"
        
    except Exception as e:
        logging.error(f"MySQL to GCS extraction failed: {e}")
        raise

def gcs_to_bigquery_bronze(**context):
    """Load data from GCS to BigQuery bronze layer"""
    from dwh_pipeline_utils import get_gcs_to_bq_worker
    
    logging.info("Starting GCS to BigQuery bronze ingestion")
    
    try:
        # Get extraction results from previous task
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='mysql_to_gcs_extraction',
            key='extraction_results'
        )
        
        successful_tables = extraction_results.get('successful_tables', TARGET_TABLES)
        logging.info(f"Processing tables from extraction: {successful_tables}")
        
        # Initialize worker and run ingestion
        worker = get_gcs_to_bq_worker()
        results = worker.run_bronze_ingestion(specific_tables=successful_tables)
        validation = worker.validate_bronze_tables()
        
        # Calculate summary statistics
        successful_bronze = sum(1 for success in results.values() if success)
        total_bronze = len(results)
        total_rows = sum(info.get('num_rows', 0) for info in validation.values() if info.get('exists'))
        total_size_mb = sum(info.get('size_bytes', 0) for info in validation.values() if info.get('exists')) / (1024*1024)
        
        logging.info(f"Bronze ingestion completed: {successful_bronze}/{total_bronze} tables successful")
        logging.info(f"Total rows in bronze: {total_rows:,}")
        logging.info(f"Total size: {total_size_mb:.1f} MB")
        
        # Store results for validation
        context['task_instance'].xcom_push(
            key='bronze_results',
            value={
                'successful_tables': [name for name, success in results.items() if success],
                'failed_tables': [name for name, success in results.items() if not success],
                'total_rows': total_rows,
                'total_size_mb': total_size_mb,
                'validation': validation
            }
        )
        
        if successful_bronze < total_bronze:
            failed_tables = [name for name, success in results.items() if not success]
            raise Exception(f"Some tables failed bronze ingestion: {failed_tables}")
            
        return f"Successfully loaded {successful_bronze} tables to bronze layer with {total_rows:,} total rows"
        
    except Exception as e:
        logging.error(f"GCS to BigQuery bronze ingestion failed: {e}")
        raise

def validate_pipeline(**context):
    """Validate complete pipeline execution"""
    logging.info("Validating pipeline completion")
    
    try:
        extraction_results = context['task_instance'].xcom_pull(
            task_ids='mysql_to_gcs_extraction',
            key='extraction_results'
        )
        bronze_results = context['task_instance'].xcom_pull(
            task_ids='gcs_to_bigquery_bronze',
            key='bronze_results'
        )
        
        validation_summary = {
            'pipeline_date': context['execution_date'].isoformat(),
            'extraction_successful': len(extraction_results.get('successful_tables', [])),
            'bronze_successful': len(bronze_results.get('successful_tables', [])),
            'total_records': extraction_results.get('total_records', 0),
            'total_rows': bronze_results.get('total_rows', 0),
            'total_size_mb': bronze_results.get('total_size_mb', 0)
        }
        
        logging.info(f"Pipeline validation summary: {validation_summary}")
        
        context['task_instance'].xcom_push(
            key='pipeline_summary',
            value=validation_summary
        )
        
        return validation_summary
        
    except Exception as e:
        logging.error(f"Pipeline validation failed: {e}")
        raise

# Task definitions
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

mysql_extraction_task = PythonOperator(
    task_id='mysql_to_gcs_extraction',
    python_callable=mysql_to_gcs_extraction,
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(hours=2)
)

bronze_ingestion_task = PythonOperator(
    task_id='gcs_to_bigquery_bronze',
    python_callable=gcs_to_bigquery_bronze,
    dag=dag,
    provide_context=True,
    execution_timeout=timedelta(hours=1)
)

validation_task = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
    provide_context=True
)

# Placeholder tasks for future expansion
silver_layer_task = DummyOperator(
    task_id='silver_layer_transformation',
    dag=dag
)

gold_layer_task = DummyOperator(
    task_id='gold_layer_aggregation',
    dag=dag
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Task dependencies
start_task >> mysql_extraction_task >> bronze_ingestion_task >> validation_task >> silver_layer_task >> gold_layer_task >> end_task
