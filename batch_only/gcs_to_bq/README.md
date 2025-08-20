# GCS to BigQuery Worker

This module handles the automated transfer of data from Google Cloud Storage to BigQuery bronze layer.

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Authentication

#### Option A: Service Account Key File
1. Download your service account key JSON file from Google Cloud Console
2. Place it in the `.keys/` directory as `service-account.json`
3. Or update the path in `../config/secrets.json`

#### Option B: Environment Variable
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account.json"
```

### 3. Verify Configuration
```bash
python test_config.py
```

## Usage

### Quick Start
```bash
python run_bronze_ingestion.py
```

### Advanced Usage
```python
from main import GCSToBigQueryWorker

# Initialize worker
worker = GCSToBigQueryWorker()

# Run ingestion for specific tables
results = worker.run_bronze_ingestion(['alm_his_1', 'piezas_1'])

# Validate results
validation = worker.validate_bronze_tables()
```

## Target Tables

The following tables are automatically processed:
- `alm_his_1`
- `alm_his_2` 
- `alm_pie_1`
- `alm_pie_2`
- `piezas_1`
- `piezas_2`

## Data Flow

1. **Source**: GCS bucket (`valsurtruck-dwh-bronze`)
   - Path pattern: `bronze/{database}/{table_name}/{date}/`
   - Format: Parquet files

2. **Destination**: BigQuery (`dwh-building.bronze`)
   - Schema: Auto-detected from Parquet
   - Write mode: TRUNCATE (replaces existing data)

## Configuration

Configuration is loaded from `../config/secrets.json`:

```json
{
  "mysql": {
    "database": "pk_gest_xer"
  },
  "gcp": {
    "project_id": "dwh-building",
    "bucket_name": "valsurtruck-dwh-bronze",
    "service_account_key_path": ".keys/service-account.json"
  }
}
```

## Logging

Logs are written to:
- `logs/gcs_to_bq_worker_YYYYMMDD.log`
- Console output

## Troubleshooting

### Authentication Issues
- Ensure service account has required roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - Storage Admin
- Verify key file path and permissions

### Configuration Issues
Run the test script to diagnose:
```bash
python test_config.py
```

### Data Issues
- Check GCS bucket structure
- Verify Parquet file format
- Ensure latest files exist for all tables
