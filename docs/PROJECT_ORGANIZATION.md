# Data Warehouse Project Organization

## 📁 Project Structure

```
pruebas-dwh/
├── 📁 config/                          # Configuration files
│   ├── config.json                     # Main configuration
│   ├── secrets.json                    # Database credentials (gitignored)
│   └── secrets.json.template           # Template for secrets
│
├── 📁 extractors/                      # Core extraction modules
│   ├── __init__.py                     # Package initialization
│   ├── base_extractor.py              # Base class with DB/GCS connectivity
│   ├── batch_extractor.py             # Batch/full load extractor
│   ├── incremental_extractor.py       # Incremental load extractor
│   ├── mysql_to_gcs_extractor.py      # MySQL to GCS pipeline
│   └── simple_extractor.py            # Simple extraction utilities
│
├── 📁 scripts/                         # Execution and utility scripts
│   ├── 🔧 daily_pipeline.py           # Main daily execution pipeline
│   ├── 📊 database_documenter.py      # Comprehensive DB documentation
│   ├── ⚡ quick_database_documenter.py # Incremental loading analyzer
│   ├── 🔍 table_discovery.py          # Table discovery utilities
│   ├── ⚙️ setup.py                    # Environment setup
│   ├── 🧪 test_batch_limited.py       # Limited batch testing
│   ├── 🧪 test_incremental.py         # Incremental loading tests
│   ├── 🧹 cleanup_gcs.py              # GCS cleanup utilities
│   ├── 🧹 cleanup_gcs_commands.ps1    # PowerShell cleanup commands
│   └── 🧹 cleanup_gcs_commands.sh     # Bash cleanup commands
│
├── 📁 docs/                            # Documentation
│   ├── 📁 database_documentation/      # Database analysis and documentation
│   │   ├── 📊 incremental_loading_analysis_*.xlsx    # Incremental strategy analysis
│   │   ├── 📋 incremental_loading_guide_*.md         # Implementation guide
│   │   ├── 📊 pk_gest_xer_documentation_*.xlsx      # Full DB documentation
│   │   ├── 📋 DATABASE_QUICK_GUIDE_*.md              # Quick reference guide
│   │   ├── 📁 incremental_analysis_*_csv/            # CSV analysis data
│   │   └── 📁 csv_quick_*/                           # Quick analysis CSV
│   ├── GCS_SETUP.md                   # Google Cloud Storage setup guide
│   ├── GIT_SETUP.md                   # Git configuration guide
│   ├── PROJECT_STRUCTURE.md           # Legacy project structure
│   ├── PROJECT_ORGANIZATION.md        # This file - current organization
│   └── README_NEW.md                  # Updated project README
│
├── 📁 extracted_data/                  # Data output directory
│   ├── 📁 bronze/                      # Raw extracted data (by table)
│   │   ├── 📁 accesos_presencia/       # Individual table data
│   │   ├── 📁 agenda/
│   │   ├── 📁 alb_cli/
│   │   └── ... (220 tables)
│   └── 📁 metadata/                    # Extraction metadata and logs
│       ├── *_full_*.json              # Full extraction metadata
│       └── *_incremental_*.json       # Incremental extraction metadata
│
├── 📁 logs/                            # Application logs
│   ├── pipeline.log                   # Current pipeline logs
│   └── pipeline_*.log                 # Historical logs by date
│
├── 📁 tests/                           # Test files
│
├── 📁 .venv/                           # Python virtual environment
├── 📁 .git/                            # Git repository
├── 📁 .keys/                           # GCS service account keys (gitignored)
├── 📁 __pycache__/                     # Python cache files
│
├── 📄 config_manager.py               # Configuration management utility
├── 📄 organization_summary.py         # Project organization utilities
├── 📄 project_summary.py              # Project summary generator
├── 📄 requirements.txt                # Python dependencies
├── 📄 README.md                       # Main project README
├── 📄 GITHUB_READY.md                 # GitHub publication guide
└── 📄 .gitignore                      # Git ignore patterns
```

## 🎯 Key Components

### 🔥 Core Extractors
- **`base_extractor.py`**: Foundation class with MySQL and GCS connectivity
- **`incremental_extractor.py`**: Smart incremental loading with watermark management
- **`batch_extractor.py`**: Full table replacement extraction
- **`mysql_to_gcs_extractor.py`**: Complete MySQL to GCS pipeline

### 📊 Database Analysis Tools
- **`quick_database_documenter.py`**: **⭐ NEW** - Incremental loading strategy analyzer
  - Analyzes 220 tables for loading patterns
  - Identifies optimal watermark columns
  - Provides implementation recommendations
  - Outputs: Excel, CSV, Markdown guides

- **`database_documenter.py`**: Comprehensive database documentation
  - Full schema analysis
  - Table and column metadata
  - Relationship mapping

### 🚀 Execution Scripts
- **`daily_pipeline.py`**: Main orchestration script
- **`test_incremental.py`**: Incremental loading testing
- **`test_batch_limited.py`**: Limited batch testing

### 🧹 Maintenance Tools
- **`cleanup_gcs.py`**: GCS cleanup automation
- **`table_discovery.py`**: Dynamic table discovery

## 📋 Recent Additions (August 19, 2025)

### 🆕 Incremental Loading Analysis System
1. **Analysis Engine**: `quick_database_documenter.py` 
   - Converted from general documenter to incremental loading specialist
   - Analyzes timestamp patterns, primary keys, table types
   - Scoring system for loading strategy recommendations

2. **Generated Documentation** (in `docs/database_documentation/`):
   - `incremental_loading_analysis_20250819_082934.xlsx` - Complete analysis
   - `incremental_loading_guide_20250819_082938.md` - Implementation guide
   - `incremental_analysis_20250819_082938_csv/` - CSV data exports

3. **Key Insights**:
   - 63 tables suitable for incremental loading (29%)
   - 26 high-confidence incremental candidates
   - Identified optimal watermark columns
   - Generated SQL implementation examples

## 🏗️ Architecture Patterns

### 🔄 Data Flow
```
MySQL Database → Base Extractor → Strategy Decision → GCS Storage
                      ↓
               [Incremental | Batch]
                      ↓
              Metadata Tracking → Monitoring
```

### 📊 Loading Strategy Matrix
- **INCREMENTAL_PREFERRED**: Tables with clear timestamp patterns
- **INCREMENTAL_POSSIBLE**: Tables with potential for incremental
- **INCREMENTAL_CHALLENGING**: Complex but technically feasible
- **FULL_REPLACE**: Simple, reliable full table replacement

## 🛠️ Development Workflow

1. **Analysis Phase**: Use `quick_database_documenter.py` for strategy planning
2. **Implementation Phase**: Configure extractors based on analysis
3. **Testing Phase**: Use test scripts for validation
4. **Production Phase**: Execute via `daily_pipeline.py`
5. **Monitoring Phase**: Review logs and metadata

## 📈 Performance Optimization

- **Incremental Loading**: Reduces data transfer by 70-90%
- **Watermark Management**: Ensures no data loss or duplication
- **Timeout Handling**: Manages large table extractions
- **Parallel Processing**: Concurrent table extractions

## 🔐 Security & Configuration

- **Secrets Management**: `config/secrets.json` (gitignored)
- **Service Account Keys**: `.keys/` directory (gitignored)
- **Environment Isolation**: Virtual environment in `.venv/`
- **Access Control**: GCS IAM and MySQL user permissions

## 📝 Documentation Standards

- **Code Documentation**: Docstrings and inline comments
- **Analysis Reports**: Auto-generated Excel and Markdown
- **Setup Guides**: Step-by-step instructions in `docs/`
- **Change Tracking**: Git history and markdown logs

---

*Last Updated: August 19, 2025*
*Project Status: Production-ready with incremental loading optimization*
