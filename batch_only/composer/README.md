# Data Warehouse Composer Pipeline

This directory contains the Google Cloud Composer (Airflow) DAG for orchestrating the complete data warehouse pipeline.

## 📁 Structure

```
composer/
├── dags/
│   ├── dwh_pipeline_dag.py      # Main DAG definition
│   └── dwh_pipeline_utils.py    # Utility functions
├── config/
│   └── composer_config.json     # Composer-specific configuration
├── requirements.txt             # Additional Python packages
├── deploy.sh                   # Deployment script
└── README.md                   # This file
```

## 🔄 Pipeline Overview

The DAG orchestrates the following workflow:

1. **MySQL to GCS** - Extract data from MySQL database and upload to Google Cloud Storage
2. **GCS to BigQuery Bronze** - Load raw data into BigQuery bronze layer
3. **Validation** - Verify pipeline completion and data quality
4. **Future Steps** - Placeholder tasks for silver and gold layer processing

## 🚀 Deployment

### Prerequisites

1. **Create Composer Environment**
```bash
gcloud composer environments create dwh-composer-env \
    --location us-central1 \
    --python-version 3 \
    --node-count 3 \
    --machine-type n1-standard-1
```

2. **Install Additional Packages**
```bash
gcloud composer environments update dwh-composer-env \
    --location us-central1 \
    --update-pypi-packages-from-file requirements.txt
```

### Deploy the DAG

```bash
# Make deployment script executable
chmod +x deploy.sh

# Deploy to Composer
./deploy.sh dwh-composer-env us-central1 dwh-building
```

### Upload Secrets (Important!)

```bash
# Upload secrets.json
gcloud composer environments storage data import \
    --environment dwh-composer-env \
    --location us-central1 \
    --source ../batch_only/config/secrets.json \
    --destination secrets.json

# Upload service account key
gcloud composer environments storage data import \
    --environment dwh-composer-env \
    --location us-central1 \
    --source ../batch_only/.keys/service-account.json \
    --destination service-account.json
```

## ⚙️ Configuration

### Airflow Variables

Set these in the Airflow UI (Admin > Variables):

| Variable | Value | Description |
|----------|-------|-------------|
| `GCP_PROJECT_ID` | `dwh-building` | Your GCP project ID |
| `GCS_BUCKET_NAME` | `valsurtruck-dwh-bronze` | GCS bucket for data storage |
| `MYSQL_DATABASE` | `pk_gest_xer` | Source MySQL database |
| `BIGQUERY_LOCATION` | `US` | BigQuery dataset location |

### Schedule

- **Default**: Daily at 2:00 AM UTC
- **Modify**: Edit `schedule_interval` in `dwh_pipeline_dag.py`

## 📊 Monitoring

### Airflow UI
- **URL**: Available in Cloud Console → Composer → Environments
- **Monitor**: Task execution, logs, and dependencies
- **Control**: Enable/disable DAG, trigger manual runs

### Logs
- **Task Logs**: Available in Airflow UI for each task
- **Cloud Logging**: Integrated with Google Cloud Logging
- **Error Alerts**: Email notifications on task failures

## 🔧 Task Details

### mysql_to_gcs_extraction
- **Timeout**: 2 hours
- **Function**: Extract all target tables from MySQL to GCS
- **Output**: Parquet files with date partitioning
- **Retry**: 2 attempts with 5-minute delay

### gcs_to_bigquery_bronze  
- **Timeout**: 1 hour
- **Function**: Load data from GCS to BigQuery bronze layer
- **Method**: Uses pandas-gbq for reliable data transfer
- **Validation**: Automatic table existence and row count checks

### validate_pipeline
- **Function**: Validate complete pipeline execution
- **Output**: Summary statistics and success metrics
- **Alerting**: Fails if critical issues detected

## 🔮 Future Expansion

The pipeline is designed for easy expansion:

### Silver Layer (Data Transformation)
```python
def silver_transformation(**context):
    """Transform bronze data to silver layer"""
    # Add business logic transformations
    # Data cleaning, type conversions
    # Standardization, deduplication
    pass
```

### Gold Layer (Business Intelligence)
```python
def gold_aggregation(**context):
    """Create business-ready aggregations"""
    # Create mart tables
    # Business KPIs and metrics
    # Optimized for reporting
    pass
```

### Data Quality Checks
```python
def data_quality_checks(**context):
    """Comprehensive data validation"""
    # Schema validation
    # Data freshness checks
    # Anomaly detection
    pass
```

## 🛡️ Security Best Practices

1. **Secrets Management**
   - Never commit `secrets.json` or service account keys
   - Use Composer's data folder for sensitive files
   - Rotate service account keys regularly

2. **IAM Permissions**
   - Grant minimum required permissions
   - Use separate service accounts for different services
   - Monitor access logs

3. **Network Security**
   - Use private IP for Composer environment
   - Restrict MySQL access to specific IPs
   - Enable VPC Service Controls if needed

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**
   ```
   ModuleNotFoundError: No module named 'batch_extractor'
   ```
   **Solution**: Ensure all modules are uploaded to the dags folder

2. **Credential Issues**
   ```
   403 Access Denied
   ```
   **Solution**: Verify service account permissions and file paths

3. **Task Timeouts**
   ```
   AirflowTaskTimeout
   ```
   **Solution**: Increase timeout values or optimize queries

4. **Memory Issues**
   ```
   Out of memory error
   ```
   **Solution**: Increase Composer node size or process data in chunks

### Debug Steps

1. **Check Task Logs**: Airflow UI → DAG → Task → View Log
2. **Verify File Paths**: Ensure all configuration files are uploaded
3. **Test Connections**: Use Airflow connections to test database/GCS access
4. **Resource Monitoring**: Check Composer environment resource usage

## 📈 Performance Optimization

### For Large Datasets
- Increase Composer node count and memory
- Use BigQuery streaming inserts for real-time data
- Implement parallel processing for multiple tables
- Add data partitioning and clustering in BigQuery

### Cost Optimization
- Schedule DAG during off-peak hours
- Use preemptible instances for Composer
- Implement incremental data loading
- Set BigQuery query cost controls

This pipeline provides a solid foundation for your data warehouse while maintaining flexibility for future enhancements!
