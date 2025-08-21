#!/usr/bin/env python3
"""
Full Pipeline Runner
Runs the complete end-to-end pipeline in the reorganized structure
"""

import sys
import os
import json
import logging
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.batch_extractor import BatchExtractor
from bigquery.main import GCSToBigQueryWorker
from core.utils import setup_logging

def load_config():
    """Load configuration from config.json"""
    config_path = project_root / "config" / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)

def load_secrets():
    """Load secrets from secrets.json"""
    secrets_path = project_root / "config" / "secrets.json"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
    
    with open(secrets_path, 'r') as f:
        return json.load(f)

def run_extraction_phase(config, secrets):
    """Run the data extraction phase"""
    print("\n" + "="*60)
    print("🚀 PHASE 1: DATA EXTRACTION")
    print("="*60)
    
    try:
        # Initialize extractor
        extractor = BatchExtractor(config, secrets)
        
        # Run extraction
        results = extractor.extract_all_tables()
        
        print(f"✅ Extraction completed successfully!")
        print(f"📊 Extracted {len(results)} tables")
        
        # Print summary - results is already a summary string
        print(f"📈 Extraction summary: {results}")
        
        return True
        
    except Exception as e:
        print(f"❌ Extraction failed: {str(e)}")
        logging.error(f"Extraction failed: {str(e)}", exc_info=True)
        return False

def run_bigquery_phase(config, secrets):
    """Run the BigQuery ingestion phase"""
    print("\n" + "="*60)
    print("🚀 PHASE 2: BIGQUERY INGESTION")
    print("="*60)
    
    try:
        # Initialize and run BigQuery worker
        worker = GCSToBigQueryWorker(config, secrets)
        results = worker.run_bronze_ingestion()
        
        # Print results summary
        successful_tables = sum(1 for result in results.values() if result)
        failed_tables = len(results) - successful_tables
        
        print(f"✅ BigQuery ingestion completed successfully!")
        print(f"📊 Processed {len(results)} tables")
        print(f"✅ Successful: {successful_tables}")
        print(f"❌ Failed: {failed_tables}")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery ingestion failed: {str(e)}")
        logging.error(f"BigQuery ingestion failed: {str(e)}", exc_info=True)
        return False

def validate_environment():
    """Validate that all required files and configurations exist"""
    print("\n" + "="*60)
    print("🔍 ENVIRONMENT VALIDATION")
    print("="*60)
    
    required_files = [
        "config/config.json",
        "config/secrets.json",
        "core/batch_extractor.py",
        "bigquery/main.py"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = project_root / file_path
        if full_path.exists():
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path}")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n❌ Missing required files: {', '.join(missing_files)}")
        return False
    
    print("✅ Environment validation passed!")
    return True

def main():
    """Main pipeline execution"""
    print("🏗️  DWH CREATION PIPELINE - REORGANIZED STRUCTURE")
    print("=" * 80)
    
    # Setup logging
    setup_logging(project_root / "logs")
    
    # Validate environment
    if not validate_environment():
        print("\n❌ Environment validation failed. Please check missing files.")
        sys.exit(1)
    
    try:
        # Load configuration
        print("\n📋 Loading configuration...")
        config = load_config()
        secrets = load_secrets()
        print("✅ Configuration loaded successfully!")
        
        # Print pipeline info
        print(f"📂 Project root: {project_root}")
        print(f"🗂️  Data output: {config['gcs']['bucket_name']}")
        print(f"📊 BigQuery project: {config['bigquery']['project_id']}")
        print(f"🌍 Location: {config['bigquery']['location']}")
        
        # Run extraction phase
        extraction_success = run_extraction_phase(config, secrets)
        
        if not extraction_success:
            print("\n❌ Pipeline failed at extraction phase")
            sys.exit(1)
        
        # Ask user if they want to continue to BigQuery
        print("\n" + "="*60)
        response = input("Continue to BigQuery ingestion? (y/n): ").lower().strip()
        
        if response != 'y':
            print("⏸️  Pipeline stopped after extraction phase")
            sys.exit(0)
        
        # Run BigQuery phase
        bigquery_success = run_bigquery_phase(config, secrets)
        
        if not bigquery_success:
            print("\n❌ Pipeline failed at BigQuery ingestion phase")
            sys.exit(1)
        
        # Success
        print("\n" + "="*60)
        print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("✅ Data extraction: Complete")
        print("✅ BigQuery ingestion: Complete")
        print("🚀 Your data warehouse is ready!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
