# Batch Only Data Loader

This folder contains a minimal, clean setup for batch data extraction only. No incremental logic is included.

## Features

- 🚀 Simple batch extraction from MySQL databases
- ☁️ **Google Cloud Storage upload** with proper partitioning
- 📊 Extracts all tables or specific tables
- 💾 Saves data in bronze layer structure (like BaseExtractor)
- 🔧 Easy configuration via JSON files (config + secrets)
- 📝 Built-in logging and error handling
- 📈 Metadata generation for each extraction
- 📅 Date-partitioned GCS storage

## Files

- `batch_extractor.py`: Main batch extraction class (based on BaseExtractor)
- `run_batch.py`: Example script to run batch extraction with GCS upload
- `config.json`: Non-sensitive configuration (logging, extraction settings)
- `secrets.json.template`: Template for database and GCS credentials
- `requirements.txt`: Required Python packages (includes GCS support)

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure credentials**:
   ```bash
   # Copy the template and edit with your credentials
   cp secrets.json.template secrets.json
   # Edit secrets.json with your MySQL and GCP details
   ```

3. **Setup GCS credentials**:
   ```bash
   # Create .keys directory and place your service account JSON file
   mkdir .keys
   # Place your service-account.json file in .keys/
   ```

4. **Run batch extraction**:
   ```bash
   python run_batch.py
   ```

## Configuration

### secrets.json (Private - NOT committed to git)
```json
{
  "mysql": {
    "host": "your_mysql_host",
    "port": 3306,
    "database": "your_database_name",
    "username": "your_username",
    "password": "your_password"
  },
  "gcp": {
    "project_id": "your-gcp-project-id",
    "bucket_name": "your-dwh-bucket",
    "service_account_key_path": ".keys/service-account.json"
  }
}
```

### config.json (Public - can be committed)
```json
{
  "extraction": {
    "enable_gcs_upload": true,
    "output_directory": "extracted_data",
    "bronze_layer_path": "bronze",
    "metadata_path": "metadata"
  },
  "logging": {
    "level": "INFO",
    "log_to_file": true,
    "log_file_path": "logs/batch_pipeline.log"
  }
}
```

## Usage Examples

### Extract All Tables with GCS Upload
```python
from batch_extractor import BatchExtractor

extractor = BatchExtractor()
results = extractor.extract_all_tables(limit_per_table=1000)

# Check results
for table_name, result in results.items():
    if result['success']:
        print(f"✅ {table_name}: {result['records']} records")
        if result['gcs_path']:
            print(f"   GCS: {result['gcs_path']}")
```

### Extract Specific Table
```python
extractor = BatchExtractor()
result = extractor.extract_table_with_upload("your_table_name", limit=5000)
```

## Output Structure

### Local Files (Bronze Layer)
```
extracted_data/
├── bronze/
│   └── {database_name}/
│       └── {table_name}/
│           └── {table_name}_batch_{timestamp}.parquet
└── metadata/
    └── {table_name}_batch_{timestamp}.json
```

### GCS Structure (Date Partitioned)
```
gs://your-bucket/bronze/{database_name}/{table_name}/date=2025/08/19/{filename}
```

## GCS Features

- **Date Partitioning**: Files organized by extraction date
- **Bronze Layer**: Raw data storage following data lake patterns  
- **Metadata**: Rich metadata including schema, record counts, and paths
- **Error Handling**: Graceful fallback to local-only if GCS fails

## Logging

Logs are written to:
- Console (always)
- `logs/batch_pipeline.log` (if enabled in config)

## Security

- Database credentials in `secrets.json` (gitignored)
- GCS service account key in `.keys/` directory (gitignored)
- Public configuration in `config.json` (can be committed)
