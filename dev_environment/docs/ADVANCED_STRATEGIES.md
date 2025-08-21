# Advanced Data Ingestion Pipeline

## Overview

This advanced data ingestion pipeline implements sophisticated per-table processing strategies for loading data from Google Cloud Storage (GCS) to BigQuery. The pipeline supports three distinct ingestion patterns optimized for different data types and use cases.

## Architecture

```
MySQL Database → GCS (Hive Partitioned) → BigQuery (Bronze/Silver/Gold)
```

### Data Flow
1. **Extract**: MySQL data extracted to GCS with hive-compatible partitioning
2. **Stage**: Data loaded from GCS to temporary staging tables
3. **Transform**: Per-table strategy applied (incremental merge, partition replacement, or SCD1 upsert)
4. **Clean**: Staging tables dropped after processing

## Ingestion Strategies

### 1. Incremental Merge (Movement Data)
**Tables**: `alm_his_1`, `alm_his_2`
**Use Case**: High-volume transactional data with frequent updates

**Features**:
- Event timestamp-based deduplication
- MERGE statement with WHEN MATCHED/NOT MATCHED logic
- Partitioned by event date for query performance
- Clustered by business keys

**Configuration**:
```json
"alm_his_1": {
  "strategy": "incremental_merge",
  "pk": ["id"],
  "event_ts": "f_fecha",
  "cluster_by": ["cc_cod_pie", "ic_cod_alm"]
}
```

### 2. Replace Partition (Snapshot Data)
**Tables**: `alm_pie_1`, `alm_pie_2`
**Use Case**: Daily inventory snapshots that completely replace previous data

**Features**:
- Full partition replacement (DELETE + INSERT)
- Date-based partitioning for efficient overwrites
- Clustered for optimal query performance
- No deduplication needed (complete snapshot)

**Configuration**:
```json
"alm_pie_1": {
  "strategy": "replace_partition",
  "partition_field": "snapshot_date",
  "cluster_by": ["cc_cod_pie", "ic_cod_alm"]
}
```

### 3. SCD1 Upsert (Master Data)
**Tables**: `piezas_1`, `piezas_2`
**Use Case**: Slowly changing dimensions where latest value wins

**Features**:
- Last-updated-wins logic
- MERGE with UPDATE when matched, INSERT when not matched
- Optimized for master data changes
- Maintains current state only (SCD Type 1)

**Configuration**:
```json
"piezas_1": {
  "strategy": "upsert_scd1",
  "pk": ["cc_cod_pie"],
  "updated_at": "f_fec_mod",
  "cluster_by": ["cc_cod_pie"]
}
```

## GCS Partitioning Structure

Data is stored in GCS using Hive-compatible partitioning:

```
gs://bucket/bronze/{database}/{table}/year=YYYY/month=MM/day=DD/
```

**Example**:
```
gs://valsur-data-lake/bronze/pk_gest_xer/alm_his_1/year=2025/month=08/day=21/
```

This structure enables:
- Efficient partition pruning in BigQuery
- Date-based data organization
- Easy backfill and reprocessing
- Compatible with various analytics tools

## Usage

### Basic Usage
```python
from main import GCSToBigQueryWorker
from datetime import date

# Initialize worker
worker = GCSToBigQueryWorker()

# Run advanced ingestion for today
today = date.today()
results = worker.run_advanced_ingestion(today)

# Check results
for table_name, success in results.items():
    status = 'SUCCESS' if success else 'FAILED'
    print(f'{table_name}: {status}')
```

### Configuration-Driven Processing
All table strategies are defined in `config/config.json`:

```json
{
  "table_strategies": {
    "table_name": {
      "strategy": "incremental_merge|replace_partition|upsert_scd1",
      "pk": ["primary_key_column"],
      "event_ts": "timestamp_column",
      "partition_field": "partition_column",
      "updated_at": "last_modified_column",
      "cluster_by": ["cluster_column1", "cluster_column2"]
    }
  }
}
```

### Advanced Methods

#### List GCS Files
```python
# List files for a specific table and date
uris = worker.list_gcs_uris_for_day('alm_his_1', date.today())
```

#### Load Staging Table
```python
# Load data from GCS to staging table
staging_table = worker.load_staging_from_uris('alm_his_1', uris)
```

#### Apply Strategy
```python
# Apply incremental merge
strategy = worker.table_strategies['alm_his_1']
rows_affected = worker.apply_incremental_merge('alm_his_1', staging_table, strategy)

# Apply partition replacement
strategy = worker.table_strategies['alm_pie_1']
rows_affected = worker.apply_replace_partition('alm_pie_1', staging_table, strategy, date.today())

# Apply SCD1 upsert
strategy = worker.table_strategies['piezas_1']
rows_affected = worker.apply_upsert_scd1('piezas_1', staging_table, strategy)
```

## SQL Generation

The pipeline automatically generates optimized SQL for each strategy:

### Incremental Merge SQL
```sql
MERGE `project.silver.table` AS target
USING (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY event_ts DESC) as rn
  FROM `staging_table`
) AS source
ON target.pk = source.pk AND source.rn = 1
WHEN MATCHED AND source.event_ts > target.event_ts THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

### Partition Replacement SQL
```sql
DELETE FROM `project.silver.table` 
WHERE partition_field = 'YYYY-MM-DD';

INSERT INTO `project.silver.table`
SELECT * FROM `staging_table`;
```

### SCD1 Upsert SQL
```sql
MERGE `project.silver.table` AS target
USING (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC) as rn
  FROM `staging_table`
) AS source
ON target.pk = source.pk AND source.rn = 1
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

## BigQuery Optimization

### Partitioning
- **Incremental tables**: Partitioned by event timestamp for efficient time-range queries
- **Snapshot tables**: Partitioned by snapshot date for efficient partition replacement
- **Master tables**: No partitioning (typically small datasets)

### Clustering
All tables are clustered by business keys to optimize:
- Join performance
- Filter performance
- Data skipping

### Table Structure
```sql
CREATE TABLE IF NOT EXISTS `project.silver.table`
PARTITION BY DATE(event_timestamp)
CLUSTER BY (business_key_1, business_key_2)
AS SELECT * FROM staging_table WHERE 1=0
```

## Error Handling

The pipeline includes comprehensive error handling:

1. **Configuration validation**: Ensures all required config keys exist
2. **GCS file checking**: Validates file existence before processing
3. **BigQuery error handling**: Captures and logs SQL execution errors
4. **Transaction management**: Uses staging tables to ensure atomicity
5. **Logging**: Detailed logging at each step for debugging

## Monitoring and Validation

### Built-in Validation
```python
# Validate all bronze tables
validation_results = worker.validate_bronze_tables()

for table_name, info in validation_results.items():
    if info['exists']:
        print(f"{table_name}: {info['num_rows']:,} rows")
    else:
        print(f"{table_name}: Table does not exist")
```

### Logging
All operations are logged with timestamps and detail levels:
- INFO: Normal operation progress
- WARNING: Non-critical issues (e.g., no files found)
- ERROR: Processing failures with full stack traces

### Metrics
The pipeline tracks:
- Tables processed successfully/failed
- Rows affected by each operation
- Processing duration
- File counts and sizes

## Production Deployment

### Prerequisites
1. Google Cloud service account with permissions:
   - BigQuery Data Editor
   - BigQuery Job User
   - Storage Object Viewer
2. Configured datasets: bronze1, silver1, gold1
3. GCS bucket with proper IAM permissions

### Environment Variables
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GCP_PROJECT_ID="your-project-id"
export GCS_BUCKET="your-bucket-name"
```

### Scheduling
The pipeline can be scheduled using:
- Cloud Scheduler + Cloud Functions
- Airflow DAGs
- Cron jobs with the provided scripts

### Example Production Script
```python
#!/usr/bin/env python3
"""Production ingestion script"""

import sys
from datetime import date, timedelta
from main import GCSToBigQueryWorker

def main():
    # Process yesterday's data (T-1)
    process_date = date.today() - timedelta(days=1)
    
    worker = GCSToBigQueryWorker()
    results = worker.run_advanced_ingestion(process_date)
    
    # Report results
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    
    if successful == total:
        print(f"SUCCESS: All {total} tables processed for {process_date}")
        sys.exit(0)
    else:
        print(f"PARTIAL FAILURE: {successful}/{total} tables processed for {process_date}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

## Performance Considerations

### GCS Organization
- Use consistent partitioning to enable partition pruning
- Store files in appropriate sizes (100MB - 1GB per file)
- Use Parquet format for optimal compression and query performance

### BigQuery Optimization
- Cluster tables by frequently filtered columns
- Partition by date columns used in WHERE clauses
- Use appropriate data types (prefer INT64 over STRING for IDs)

### Staging Strategy
- Staging tables are temporary and automatically cleaned
- Large datasets may benefit from manual staging table management
- Consider using BigQuery slots reservation for predictable performance

## Troubleshooting

### Common Issues

1. **"Bucket does not exist"**
   - Verify GCS bucket name in configuration
   - Check service account permissions

2. **"Table strategies: 0 strategies loaded"**
   - Verify config.json path and structure
   - Check that table_strategies section exists

3. **"No files found for table"**
   - Check GCS path structure matches expectation
   - Verify data extraction process completed

4. **BigQuery permission errors**
   - Ensure service account has BigQuery Data Editor role
   - Verify dataset exists and is in correct region

### Debug Mode
Enable debug logging by setting environment variable:
```bash
export LOG_LEVEL=DEBUG
```

## Testing

Run the test suite to validate functionality:

```bash
# Unit tests (no external dependencies)
python test_unit_ingestion.py

# Integration tests (requires GCS bucket and BigQuery access)
python test_advanced_ingestion.py

# Demo capabilities
python demo_advanced_ingestion.py
```

## Future Enhancements

Potential improvements for the pipeline:
- Support for schema evolution
- Data quality checks and validation
- Metrics collection and monitoring integration
- Support for additional file formats (CSV, JSON)
- Automatic backfill capabilities
- Data lineage tracking
