# Data Warehouse Ingestion Pipeline

A production-ready data pipeline for extracting data from MySQL databases and loading it into BigQuery with advanced processing strategies.

## 🏗️ Architecture

This pipeline implements a modern data architecture with:
- **Bronze Layer**: Raw data storage in BigQuery
- **Silver Layer**: Processed and cleaned data
- **Gold Layer**: Analytics-ready datasets
- **Advanced Strategies**: Per-table processing optimization

## 📁 Project Structure

```
batch_ingestion/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .env.example                       # Environment template
├── .gitignore                         # Git ignore rules
│
├── core/                              # Core pipeline modules
│   ├── __init__.py
│   ├── batch_extractor.py             # MySQL extraction logic
│   └── utils.py                       # Shared utilities
│
├── bigquery/                          # BigQuery ingestion components
│   ├── __init__.py
│   └── main.py                        # Advanced BigQuery worker
│
├── config/                            # Configuration files
│   ├── config.json                    # Main pipeline configuration
│   ├── secrets.json                   # Credentials (gitignored)
│   └── secrets.json.template          # Secrets template
│
├── scripts/                           # Executable scripts
│   ├── run_extraction.py              # Run data extraction
│   ├── run_bigquery_ingestion.py      # Run BigQuery ingestion
│   └── migrate_region.py              # Region migration utility
│
├── tests/                             # Test suite
│   ├── __init__.py
│   ├── test_pipeline_integration.py   # End-to-end pipeline tests
│   └── test_advanced_strategies.py    # Advanced strategy tests
│
├── orchestration/                     # Airflow DAGs and deployment
│   ├── dags/                          # Airflow DAG definitions
│   ├── config/                        # Airflow configuration
│   └── README.md                      # Orchestration documentation
│
├── docs/                              # Documentation
│   ├── IMPLEMENTATION.md              # Technical implementation details
│   └── ADVANCED_STRATEGIES.md         # Advanced ingestion strategies
│
└── data/                              # Data storage (gitignored)
    ├── extracted/                     # Local extracted data
    ├── logs/                          # Pipeline logs
    └── temp/                          # Temporary files
```

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd batch_ingestion

# Create virtual environment
python -m venv .venv
.venv/Scripts/activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

```bash
# Copy environment template
cp .env.example .env

# Copy secrets template
cp config/secrets.json.template config/secrets.json

# Edit configuration files
# - Update .env with your environment variables
# - Update config/secrets.json with your credentials
# - Modify config/config.json for your tables and strategies
```

### 3. Run the Pipeline

```bash
# Extract data from MySQL
python scripts/run_extraction.py

# Load data to BigQuery with advanced strategies
python scripts/run_bigquery_ingestion.py

# Run full integration test
python tests/test_pipeline_integration.py
```

## 🔧 Configuration

### Main Configuration (`config/config.json`)

```json
{
  "bigquery": {
    "datasets": {
      "bronze": "bronze1",
      "silver": "silver1", 
      "gold": "gold1"
    },
    "location": "europe-southwest1",
    "source_database": "your_database",
    "target_tables": ["table1", "table2"]
  },
  "table_strategies": {
    "table1": {
      "strategy": "incremental_merge",
      "partition_field": "created_date",
      "merge_keys": ["id"]
    },
    "table2": {
      "strategy": "replace_partition", 
      "partition_field": "snapshot_date"
    }
  }
}
```

### Secrets Configuration (`config/secrets.json`)

```json
{
  "mysql": {
    "host": "your-mysql-host",
    "port": 3306,
    "database": "your_database",
    "username": "your_username",
    "password": "your_password"
  },
  "gcp": {
    "project_id": "your-gcp-project",
    "bucket_name": "your-gcs-bucket"
  }
}
```

## 📊 Advanced Processing Strategies

The pipeline supports multiple processing strategies optimized for different data types:

### 1. Incremental Merge
- **Use Case**: Transactional data with updates
- **How it Works**: Merges new/changed records based on merge keys
- **Best For**: Order history, transaction logs

### 2. Replace Partition
- **Use Case**: Daily snapshots or time-series data  
- **How it Works**: Replaces entire partition for the processing date
- **Best For**: Inventory snapshots, daily aggregations

### 3. SCD Type 1 Upsert
- **Use Case**: Master data that changes over time
- **How it Works**: Updates existing records or inserts new ones
- **Best For**: Customer data, product catalogs

## 🧪 Testing

```bash
# Run all tests
python -m pytest tests/

# Run integration tests
python tests/test_pipeline_integration.py

# Run advanced strategy tests
python tests/test_advanced_strategies.py

# Test specific components
python -m pytest tests/test_extraction.py
```

## 📈 Performance

The pipeline is optimized for high-volume data processing:

- **Parallel Processing**: Multi-threaded extraction and loading
- **Batch Operations**: Optimized batch sizes for MySQL and BigQuery
- **Hive-Compatible Partitioning**: Efficient GCS storage layout
- **Strategy-Based Processing**: Per-table optimization reduces processing time

### Typical Performance Metrics
- **Extraction**: ~100K records/minute per table
- **BigQuery Loading**: ~500K records/minute
- **End-to-End**: 15-20 minutes for ~220 tables (~1.6M records)

## 🔄 Orchestration

The pipeline includes Airflow DAGs for production orchestration:

```bash
# Deploy to Google Cloud Composer
cd orchestration
./deploy.sh

# Local Airflow development
./setup.sh
```

## 🌍 Multi-Region Support

Use the migration utility to move resources between regions:

```bash
python scripts/migrate_region.py
```

## 📚 Documentation

- [Implementation Details](docs/IMPLEMENTATION.md)
- [Advanced Strategies](docs/ADVANCED_STRATEGIES.md)
- [Orchestration Guide](orchestration/README.md)

## 🤝 Contributing

1. Follow the established project structure
2. Add tests for new features
3. Update documentation
4. Ensure all tests pass before submitting

## 📝 License

This project is proprietary software for Valsur Truck data warehouse operations.

## 🆘 Support

For issues and questions:
1. Check the documentation in the `docs/` folder
2. Run the integration tests to validate your setup
3. Review the logs in the `data/logs/` directory
