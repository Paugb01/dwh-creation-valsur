# Data Warehouse Creation - Valsur

This project has been reorganized into two main approaches:

## Project Structure

- **`batch_only/`**: Clean, minimal batch-only data loader (recommended for new development)
- **`legacy_incremental_batch/`**: Original full-featured loader supporting both incremental and batch extraction

## Quick Start

### For Batch-Only Loading (Recommended)
```bash
cd batch_only
python run_batch.py
```

### For Legacy Full-Featured Loading
```bash
cd legacy_incremental_batch
python scripts/daily_pipeline.py
```

## Which Version to Use?

- **Use `batch_only/`** if you only need batch data extraction (simpler, cleaner)
- **Use `legacy_incremental_batch/`** if you need incremental loading or want to reference the original implementation

## Configuration

Each folder has its own configuration:
- `batch_only/config.json`: Simple batch configuration
- `legacy_incremental_batch/config/`: Full configuration with secrets

See the README files in each folder for detailed setup instructions.
   ```

2. Create and activate virtual environment:
   ```bash
   python -m venv .venv
   # On Windows:
   .venv\Scripts\activate
   # On Linux/Mac:
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure secrets** (Interactive setup):
   ```bash
   python setup.py
   ```
   
   Or manually copy and edit:
   ```bash
   cp secrets.json.template secrets.json
   # Edit secrets.json with your database credentials
   ```

### Configuration

The project uses a secure configuration system:

- **`config.json`**: Non-sensitive configuration (committed to git)
  - Database host, port, connection settings
  - Extraction parameters
  - Logging configuration

- **`secrets.json`**: Sensitive data (NOT committed to git)
  - Database username and password
  - GCP credentials and project details

- **`secrets.json.template`**: Template for secrets (committed to git)

## Usage

### Basic Data Extraction (Local Only)

Run the simple extractor to test with a single table:

```bash
python simple_extractor.py
```

### Advanced Extraction with GCS Upload

Run the GCS-enabled extractor:

```bash
python mysql_to_gcs_extractor.py
```

This will:
1. Connect to the MySQL database
2. Extract 100 rows from the first table (Pie_Fac)
3. Save data as Parquet file locally
4. Upload to Google Cloud Storage (if configured)
5. Generate extraction metadata

### Setting up Google Cloud Storage

For cloud storage integration, follow the [GCS Setup Guide](GCS_SETUP.md).

### Output Structure

Data is organized in a bronze layer structure:
```
extracted_data/
└── bronze/
    └── {table_name}/
        └── {table_name}_{timestamp}.parquet
```

## Database Schema

The `pk_gest_xer` database contains **220 tables** including:
- `Pie_Fac` - Parts/Pieces Factory data
- `alb_cli` - Customer delivery notes
- `fac_cli` - Customer invoices
- `avisos` - Service notices
- And many more...

## Development Roadmap

### Next Steps
1. **Multi-table extraction** - Handle all 220 tables
2. **Incremental loading** - Only extract new/changed data
3. **Scheduling** - Daily automation
4. **GCP integration** - Upload to Cloud Storage
5. **BigQuery integration** - Load to data warehouse
6. **Metadata tracking** - Data governance and lineage

### Planned Architecture

```
MySQL (192.168.1.204) → Cloud Storage (Bronze) → BigQuery (Silver/Gold)
```

## Contributing

1. Create a feature branch
2. Make changes
3. Test thoroughly
4. Create pull request

## Dependencies

- `pymysql>=1.0.0` - MySQL database connector
- `pandas>=1.5.0` - Data manipulation and analysis
- `pyarrow>=10.0.0` - Parquet file format support
- `google-cloud-storage>=2.10.0` - Google Cloud Storage client
- `google-auth>=2.22.0` - Google Cloud authentication

## License

[Add license information]

## Contact

[Add contact information]
