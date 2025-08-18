# MySQL to GCP Data Pipeline - Version 2.0

A daily data pipeline that extracts data from a MySQL database (pk_gest_xer) and prepares it for Google Cloud Platform (GCP) as a bronze layer for data governance, with eventual BigQuery integration.

## Project Structure

```
pruebas-dwh/
├── .venv/                          # Virtual environment (not in git)
├── .gitignore                      # Git ignore rules
├── config.json                     # Non-sensitive configuration
├── config_manager.py               # Configuration management
├── secrets.json                    # Sensitive data (not in git)
├── secrets.json.template           # Template for secrets
├── setup.py                        # Interactive setup script
├── simple_extractor.py            # Basic local data extractor
├── mysql_to_gcs_extractor.py      # Advanced extractor with GCS integration
├── requirements.txt                # Python dependencies
├── extracted_data/                # Output directory (not in git)
│   └── bronze/                     # Bronze layer data
├── GIT_SETUP.md                   # Git setup instructions
├── GCS_SETUP.md                   # Google Cloud Storage setup guide
└── README.md                       # This file
```

## Features

- ✅ **Secure Configuration**: Separates configuration from secrets
- ✅ **Database Connection**: PyMySQL connection to MySQL database
- ✅ **Data Extraction**: Pandas-based extraction to DataFrame
- ✅ **Local Storage**: Parquet format in organized directory structure
- ✅ **Cloud Storage**: Google Cloud Storage integration with secure authentication
- ✅ **Bronze Layer**: Organized data lake structure with date partitioning
- ✅ **Metadata Tracking**: JSON metadata for each extraction
- ✅ **Version Control Ready**: Proper .gitignore and documentation

## Setup

### Prerequisites

- Python 3.7+
- Access to MySQL database at 192.168.1.204
- Virtual environment recommended

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd pruebas-dwh
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
