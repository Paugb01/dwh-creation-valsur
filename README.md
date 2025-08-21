# Data Warehouse Creation - Valsur

This repository contains the complete data warehouse solution for Valsur Truck operations.

## 📁 Project Structure

- **[`batch_ingestion/`](./batch_ingestion/README.md)** - Production-ready data pipeline for MySQL → BigQuery ingestion with advanced processing strategies
- **`tests/`** - Legacy test files (deprecated)
- **`PIPELINE_VALIDATION_RESULTS.md`** - Historical validation results

## 🚀 Quick Start

Navigate to the main project directory:

```bash
cd batch_ingestion
```

Follow the setup instructions in the [batch_ingestion README](./batch_ingestion/README.md).

## 🏗️ Architecture Overview

The data warehouse implements a modern lakehouse architecture:

1. **Bronze Layer**: Raw data from MySQL sources
2. **Silver Layer**: Cleaned and processed data
3. **Gold Layer**: Analytics-ready datasets

## 🔧 Advanced Features

- **Per-table Processing Strategies**: Incremental merge, partition replacement, SCD Type 1
- **Hive-compatible Partitioning**: Optimized storage layout
- **Multi-region Support**: Deployable across GCP regions
- **Production Orchestration**: Airflow DAGs for automated workflows

## 📊 Performance

Validated for high-volume processing:
- ~220 tables
- ~1.6M daily records
- 15-20 minute end-to-end runtime

## 📚 Documentation

- [Main Pipeline Documentation](./batch_ingestion/README.md)
- [Implementation Details](./batch_ingestion/docs/IMPLEMENTATION.md)
- [Advanced Strategies](./batch_ingestion/docs/ADVANCED_STRATEGIES.md)

## 🎯 Status

✅ **Production Ready** - All validation tests passed, advanced strategies implemented
