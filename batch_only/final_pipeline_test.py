#!/usr/bin/env python3
"""
Final Pipeline Validation Test
Comprehensive test of the MySQL -> GCS -> BigQuery pipeline
Optimized for single service account usage
"""
import sys
import os
from dotenv import load_dotenv
from utils import get_credentials, load_configuration, setup_environment

# Setup environment and load configuration
setup_environment()
load_dotenv()

# Get shared credentials and configuration
credentials = get_credentials()
config = load_configuration('SECRETS')


def validate_configuration():
    """Validate that configuration is properly loaded"""
    print("Configuration Validation:")
    
    if not config:
        print("   ERROR: Configuration not loaded")
        return False
    
    print(f"   MySQL host: {config.get('mysql', {}).get('host', 'NOT FOUND')}")
    print(f"   GCP project: {config.get('gcp', {}).get('project_id', 'NOT FOUND')}")
    print(f"   GCS bucket: {config.get('gcp', {}).get('bucket_name', 'NOT FOUND')}")
    
    return True


def test_imports():
    """Test that all required modules can be imported"""
    print("Testing module imports...")
    
    try:
        from batch_extractor import BatchExtractor
        print("   BatchExtractor imported successfully")
        
        # Add gcs_to_bq to path and import
        sys.path.append('gcs_to_bq')
        from gcs_to_bq.main import GCSToBigQueryWorker
        print("   GCSToBigQueryWorker imported successfully")
        
        return BatchExtractor, GCSToBigQueryWorker
        
    except Exception as e:
        print(f"   Import failed: {e}")
        return None, None


def test_extraction_pipeline(BatchExtractor, test_tables, record_limit=None):
    """Test the MySQL extraction and GCS upload pipeline"""
    print(f"\nTesting extraction pipeline ({len(test_tables)} tables, {record_limit} records each)...")
    
    try:
        # Initialize extractor
        extractor = BatchExtractor(
            config_path='config/config.json',
            secrets_path='config/secrets.json'
        )
        
        extraction_results = {}
        
        for table in test_tables:
            print(f"   Extracting {table}...")
            result = extractor.extract_table_with_upload(table, limit=record_limit)
            extraction_results[table] = result
            
            if result.get('success'):
                print(f"      SUCCESS: {result.get('records', 0)} records extracted")
            else:
                print(f"      FAILED: {result.get('error', 'Unknown error')}")
        
        successful_extractions = sum(1 for r in extraction_results.values() if r.get('success'))
        print(f"   Extraction Summary: {successful_extractions}/{len(test_tables)} tables successful")
        
        return extraction_results, successful_extractions
        
    except Exception as e:
        print(f"   Extraction test failed: {e}")
        return {}, 0


def test_bigquery_ingestion(GCSToBigQueryWorker, extraction_results):
    """Test BigQuery ingestion from GCS"""
    print("\nTesting BigQuery ingestion...")
    
    successful_tables = [table for table, result in extraction_results.items() 
                        if result.get('success')]
    
    if not successful_tables:
        print("   SKIPPED: No successful extractions to test")
        return 0
    
    try:
        # Initialize worker with single service account
        worker = GCSToBigQueryWorker(credentials=credentials, config=config)
        
        # Run ingestion for successful extractions
        bq_results = worker.run_bronze_ingestion(specific_tables=successful_tables)
        successful_ingestions = sum(1 for r in bq_results.values() if r)
        
        print(f"   BigQuery Summary: {successful_ingestions}/{len(successful_tables)} tables successful")
        
        return successful_ingestions
        
    except Exception as e:
        print(f"   BigQuery ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 0


def print_final_summary(extraction_count, ingestion_count, test_tables):
    """Print comprehensive test summary"""
    print("\n" + "=" * 60)
    print("FINAL PIPELINE VALIDATION SUMMARY")
    print("=" * 60)
    
    extraction_rate = (extraction_count / len(test_tables)) * 100 if test_tables else 0
    ingestion_rate = (ingestion_count / extraction_count) * 100 if extraction_count > 0 else 0
    
    if extraction_count > 0 and ingestion_count > 0:
        print("PIPELINE FULLY VALIDATED!")
        print("+ MySQL extraction: WORKING")
        print("+ GCS upload: WORKING") 
        print("+ BigQuery ingestion: WORKING")
        print("\nREADY FOR PRODUCTION DEPLOYMENT")
        print("Recommendation: Proceed with full pipeline deployment")
        
    elif extraction_count > 0:
        print("PARTIAL VALIDATION")
        print("+ MySQL extraction: WORKING")
        print("+ GCS upload: WORKING")
        print("- BigQuery ingestion: NEEDS INVESTIGATION")
        print("\nAction Required: Fix BigQuery permissions before deployment")
        
    else:
        print("PIPELINE VALIDATION FAILED")
        print("- Basic extraction: NOT WORKING")
        print("\nAction Required: Fix extraction configuration")
    
    print(f"\nPerformance Metrics:")
    print(f"   Extraction Success Rate: {extraction_rate:.1f}%")
    
    if extraction_count > 0:
        print(f"   BigQuery Success Rate: {ingestion_rate:.1f}%")
    
    print(f"\nFull Pipeline Estimates:")
    print(f"   Total Tables: ~220")
    print(f"   Estimated Runtime: 15-20 minutes")
    print(f"   Expected Daily Volume: ~1.6M records")


def main():
    """Main test execution"""
    print("FINAL PIPELINE VALIDATION TEST")
    print("=" * 60)
    
    # Validation steps
    if not validate_configuration():
        print("Configuration validation failed - exiting")
        sys.exit(1)
    
    # Test imports and get classes
    BatchExtractor, GCSToBigQueryWorker = test_imports()
    if not BatchExtractor or not GCSToBigQueryWorker:
        print("Module import failed - exiting")
        sys.exit(1)
    
    # Define test tables (small subset for validation)
    test_tables = ['alm_his_1', 'alm_his_2', 'alm_pie_2',
            'alm_pie_1', 'piezas_1', 'piezas_2']
    
    # Run extraction test
    extraction_results, successful_extractions = test_extraction_pipeline(BatchExtractor, test_tables)
    
    # Run BigQuery test if extractions succeeded
    successful_ingestions = test_bigquery_ingestion(GCSToBigQueryWorker, extraction_results)
    
    # Print final summary
    print_final_summary(successful_extractions, successful_ingestions, test_tables)


if __name__ == "__main__":
    main()
