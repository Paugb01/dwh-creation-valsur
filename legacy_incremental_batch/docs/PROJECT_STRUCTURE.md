# Project Structure

```
pruebas-dwh/
├── .venv/                          # Python virtual environment
├── .keys/                          # Service account keys (excluded from git)
│   └── dwh-building-gcp.json      # GCP service account key
├── config/                         # Configuration files
│   ├── config.json                # Application configuration
│   ├── secrets.json               # Sensitive credentials (excluded from git)
│   └── secrets.json.template      # Template for secrets configuration
├── extractors/                     # Data extraction modules
│   ├── __init__.py                # Package initialization
│   ├── base_extractor.py          # Base extractor class
│   ├── simple_extractor.py        # Simple table extractor
│   ├── incremental_extractor.py   # Incremental loading extractor
│   ├── mysql_to_gcs_extractor.py  # Legacy GCS extractor
│   └── batch_extractor.py         # Batch processing extractor
├── scripts/                        # Utility and setup scripts
│   ├── setup.py                   # Interactive configuration setup
│   ├── daily_pipeline.py          # Daily automation script
│   ├── table_discovery.py         # Database analysis tools
│   ├── test_incremental.py        # Incremental loading tests
│   └── test_batch_limited.py      # Batch processing tests
├── docs/                          # Project documentation
│   ├── PROJECT_STRUCTURE.md       # This file
│   ├── GCS_SETUP.md              # Google Cloud Storage setup
│   ├── GIT_SETUP.md              # Git configuration guide
│   └── README_NEW.md             # Updated project documentation
├── tests/                         # Unit tests (future)
├── extracted_data/                # Local data storage (excluded from git)
│   ├── bronze/                    # Raw extracted data
│   └── metadata/                  # Extraction metadata and watermarks
├── logs/                          # Application logs (excluded from git)
├── .git/                          # Git repository
├── .gitignore                     # Git exclusion rules
├── config_manager.py              # Legacy configuration manager
├── project_summary.py             # Project status summary
├── requirements.txt               # Python dependencies
└── README.md                      # Main project documentation
```

## Directory Descriptions

### `/config`
Contains all configuration files:
- **config.json**: Non-sensitive application settings (extraction limits, paths, logging)
- **secrets.json**: Sensitive credentials (MySQL connection, GCP service account)
- **secrets.json.template**: Template for setting up secrets

### `/extractors`
Core data extraction modules:
- **base_extractor.py**: Common functionality for all extractors (connections, storage, logging)
- **simple_extractor.py**: Basic table extraction for testing and development
- **incremental_extractor.py**: Smart incremental loading with watermark tracking
- **batch_extractor.py**: Production batch processing of multiple tables
- **mysql_to_gcs_extractor.py**: Legacy extractor (deprecated but kept for compatibility)

### `/scripts`
Utility and automation scripts:
- **setup.py**: Interactive configuration wizard
- **daily_pipeline.py**: Automated daily data pipeline with scheduling
- **table_discovery.py**: Database analysis and table structure discovery
- **test_*.py**: Testing scripts for different extraction modes

### `/docs`
Project documentation and guides:
- **PROJECT_STRUCTURE.md**: This file - complete project organization
- **GCS_SETUP.md**: Google Cloud Storage configuration guide
- **GIT_SETUP.md**: Version control setup instructions
- **README_NEW.md**: Comprehensive project documentation

### `/extracted_data`
Local data storage with bronze layer structure:
```
bronze/
└── table_name/
    ├── table_name_simple_20250818_104637.parquet
    ├── table_name_full_20250818_110422.parquet
    └── table_name_incremental_20250818_112100.parquet

metadata/
├── watermarks.json
├── table_name_simple_20250818_104637.json
└── table_name_incremental_20250818_112100.json
```

### `/.keys`
Service account keys and certificates (secured, excluded from git):
- **dwh-building-gcp.json**: Google Cloud Platform service account key

### `/logs`
Application execution logs organized by date:
- **pipeline_YYYYMMDD.log**: Daily pipeline execution logs

## Migration from Legacy Structure

The project has been reorganized from a flat structure to a modular architecture:

### Old Structure → New Structure
```
config.json → config/config.json
secrets.json → config/secrets.json
simple_extractor.py → extractors/simple_extractor.py
incremental_extractor.py → extractors/incremental_extractor.py
production_batch_extractor.py → extractors/batch_extractor.py
daily_pipeline.py → scripts/daily_pipeline.py
setup.py → scripts/setup.py
GCS_SETUP.md → docs/GCS_SETUP.md
```

### Backward Compatibility
- Legacy `config_manager.py` maintained for compatibility
- All new extractors inherit from `BaseExtractor`
- Old function-based extractors still work but are deprecated

## Configuration Structure

### config/config.json (Safe to commit)
```json
{
  "database": {
    "host": "192.168.1.204",
    "port": 3306,
    "database": "pk_gest_xer",
    "connection_timeout": 5
  },
  "extraction": {
    "output_directory": "extracted_data",
    "bronze_layer_path": "bronze",
    "metadata_path": "metadata",
    "batch_size": 10000,
    "test_limit": 100,
    "large_table_limit": 50000,
    "max_workers": 3
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(levelname)s - %(message)s",
    "log_to_file": true,
    "log_to_console": true
  }
}
```

### config/secrets.json (Never commit)
```json
{
  "database": {
    "username": "your_username",
    "password": "your_password"
  },
  "gcp": {
    "project_id": "dwh-building",
    "bucket_name": "valsurtruck-dwh-bronze",
    "service_account_key_path": ".keys/dwh-building-gcp.json"
  }
}
```

## Usage Examples

### Using New Modular Extractors
```python
# Simple extraction
from extractors import SimpleExtractor
extractor = SimpleExtractor()
result = extractor.extract_and_save("table_name", 1000)

# Incremental extraction
from extractors import IncrementalExtractor
extractor = IncrementalExtractor()
extractor.load_watermarks()
extractor.extract_table_incremental("table_name")

# Batch processing
from extractors import BatchExtractor
extractor = BatchExtractor()
extractor.run_batch_extraction(plan)
```

### Running Scripts
```bash
# Interactive setup
python scripts/setup.py

# Table analysis
python scripts/table_discovery.py

# Daily pipeline
python scripts/daily_pipeline.py manual

# Testing
python scripts/test_incremental.py
```

## Security Notes

- All sensitive files are properly excluded via `.gitignore`
- Credentials are separated from code in `/config`
- Service account keys are stored securely in `/.keys`
- Configuration uses template system for easy setup
- Base extractor handles secure credential loading

## Development Guidelines

1. **New Extractors**: Inherit from `BaseExtractor` for consistency
2. **Configuration**: Use `self.config` and `self.secrets` from base class
3. **Logging**: Use `self.logger` for consistent logging
4. **Testing**: Add test scripts to `/scripts` directory
5. **Documentation**: Update relevant docs when adding features

## Future Enhancements

- Move all legacy code to `/legacy` folder
- Add comprehensive unit tests in `/tests`
- Create API documentation in `/docs`
- Implement plugin system for custom extractors
- Add configuration validation and schema
