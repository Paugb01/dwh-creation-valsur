# MySQL to GCS Data Pipeline

A production-ready data pipeline that extracts data from MySQL database `pk_gest_xer` and loads it into Google Cloud Storage (GCS) bronze layer with intelligent incremental loading capabilities.

## 🚀 Features

- **Incremental Loading**: Automatically detects timestamp columns and extracts only new/changed data
- **Cloud Integration**: Uploads data to Google Cloud Storage with date partitioning
- **Security**: Separates configuration from secrets, prevents sensitive data in Git
- **Scalability**: Handles 220+ tables efficiently with multi-threading
- **Monitoring**: Comprehensive logging and metadata tracking
- **Flexible**: Supports both full and incremental extraction strategies

## 📁 Project Structure

```
pruebas-dwh/
├── config.json              # Non-sensitive configuration
├── secrets.json             # Database credentials and GCP keys (gitignored)
├── config_manager.py        # Configuration management
├── simple_extractor.py      # Basic local extraction
├── mysql_to_gcs_extractor.py # Advanced GCS extractor
├── incremental_extractor.py # Incremental loading logic
├── production_batch_extractor.py # Production batch processor
├── daily_pipeline.py        # Automated daily pipeline
├── extracted_data/          # Local bronze layer
│   ├── bronze/              # Parquet files organized by table
│   └── metadata/            # Extraction metadata and watermarks
├── logs/                    # Pipeline execution logs
└── .keys/                   # GCP service account keys (gitignored)
```

## ⚙️ Setup

### 1. Environment Setup
```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
Run the setup script to configure credentials:
```bash
python setup.py
```

This will create:
- `secrets.json` with database and GCP credentials
- `.keys/` directory for GCP service account key

### 3. GCS Setup
Follow [GCS_SETUP.md](GCS_SETUP.md) for Google Cloud Storage configuration.

### 4. Git Setup
Follow [GIT_SETUP.md](GIT_SETUP.md) for version control setup.

## 🔧 Usage

### Quick Test
```bash
# Test database connection
python simple_extractor.py

# Test incremental loading
python test_incremental.py

# Test batch processing (limited)
python test_batch_limited.py
```

### Production Extraction

#### Single Table (Incremental)
```bash
python incremental_extractor.py
```

#### Batch Processing (All 220 Tables)
```bash
python production_batch_extractor.py
```

#### Daily Pipeline
```bash
# Run once manually
python daily_pipeline.py manual

# Run test extraction
python daily_pipeline.py test

# Start scheduled daily runs
python daily_pipeline.py schedule
```

## 📊 Data Strategy

### Incremental Loading
- **Automatic Detection**: Finds timestamp columns (`created_at`, `updated_at`, `fecha`, etc.)
- **Watermark Tracking**: Stores last extraction timestamp per table
- **Change Detection**: Extracts only records newer than watermark
- **Fallback**: Uses full extraction if no timestamp columns found

### Table Categorization
1. **Incremental Tables**: Have timestamp columns, support change detection
2. **Small Full Tables**: <100K rows, full extraction acceptable
3. **Large Limited Tables**: >100K rows, limited to 50K records per extraction
4. **Empty Tables**: Skipped automatically

### Data Flow
```
MySQL Database (pk_gest_xer)
    ↓ (Incremental/Full Extraction)
Local Bronze Layer (Parquet)
    ↓ (Upload)
GCS Bronze Layer (gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/)
    ↓ (Future: Load to BigQuery)
Data Warehouse
```

## 📈 Monitoring

### Extraction Metadata
Each extraction generates metadata including:
- Record count and extraction type (incremental/full)
- Table analysis (columns, timestamps, row counts)
- Watermark information
- Local and GCS file paths
- Execution timing

### Watermarks
Stored in `extracted_data/metadata/watermarks.json`:
```json
{
  "table_name": {
    "last_timestamp": "2025-03-13 07:44:01",
    "last_extraction": "2025-08-18T11:21:00.145742",
    "timestamp_column": "fecha",
    "extraction_type": "incremental"
  }
}
```

### Logging
- Daily log files in `logs/pipeline_YYYYMMDD.log`
- Console and file output
- Error tracking and performance metrics

## 🎯 Performance

### Current Results (Test Run - 10 Tables)
- **Duration**: ~24 seconds
- **Success Rate**: 100% (10/10 tables)
- **Incremental**: 2 tables (11,081 records)
- **Full**: 8 tables (250,336 records)
- **Total**: 261,417 records processed

### Production Scaling
- **Multi-threading**: 3 concurrent workers for database safety
- **Memory Efficient**: Streams large tables with row limits
- **Batch Processing**: Handles all 220 tables automatically
- **GCS Upload**: Parallel upload with local fallback

## 🔒 Security

- **Credential Separation**: `secrets.json` and `.keys/` in `.gitignore`
- **Service Account**: GCP authentication via service account keys
- **Config Management**: Centralized configuration with sensitive data isolation
- **Git Safety**: Prevents accidental credential commits

## 📋 Configuration Files

### config.json (Safe to commit)
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
    "large_table_limit": 50000,
    "max_workers": 3
  }
}
```

### secrets.json (Never commit)
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

## 🛣️ Roadmap

### Phase 1: Foundation ✅
- [x] Database connectivity
- [x] Local extraction
- [x] GCS integration
- [x] Security implementation

### Phase 2: Intelligence ✅
- [x] Incremental loading
- [x] Automatic table analysis
- [x] Watermark tracking
- [x] Batch processing

### Phase 3: Production ✅
- [x] Daily scheduling
- [x] Monitoring and logging
- [ ] BigQuery integration
- [ ] Data quality checks
- [ ] Error recovery mechanisms

### Phase 4: Enhancement 📋
- [ ] Delta loading optimization
- [ ] Schema change detection
- [ ] Data lineage tracking
- [ ] Performance monitoring dashboard

## 🔧 Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Verify database host and port
   - Check network connectivity
   - Increase `connection_timeout` in config

2. **GCS Upload Failures**
   - Verify service account key path
   - Check GCS bucket permissions
   - Ensure project ID is correct

3. **Large Table Processing**
   - Adjust `large_table_limit` in config
   - Monitor memory usage
   - Consider table-specific limits

### Debug Commands
```bash
# Test database connection
python -c "from config_manager import config_manager; print('Config loaded successfully')"

# Check table structure
python table_discovery.py

# Verify GCS connection
python -c "from production_batch_extractor import ProductionBatchExtractor; e=ProductionBatchExtractor(); e.initialize_gcs()"
```

## 📞 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review extraction metadata in `extracted_data/metadata/`
3. Verify configuration files are properly set up
4. Test individual components before running full pipeline

---

**Built with Python 3.13+, PyMySQL, Pandas, and Google Cloud Storage**
