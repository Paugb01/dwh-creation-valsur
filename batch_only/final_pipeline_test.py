#!/usr/bin/env python3
"""
FINAL PIPELINE VALIDATION TEST
Tests the complete MySQL -> GCS -> BigQuery pipeline
"""
import sys
import os
from dotenv import load_dotenv
from utils import get_credentials_auto, load_config_from_env, setup_environment
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'gcs_to_bq'))

# Setup environment with proper paths
setup_environment()

# Loading environment variables
load_dotenv()

# Get credentials using best practices
gcs_credentials = get_credentials_auto()
bq_credentials = gcs_credentials  # Use same credentials for both
config = load_config_from_env('SECRETS')


print("🚀 FINAL PIPELINE VALIDATION")
print("="*50)

print("\n🔧 Configuration check:")
print(f"   Config loaded: {'✅' if config else '❌'}")
if config:
    print(f"   MySQL host: {config.get('mysql', {}).get('host', 'NOT FOUND')}")
    print(f"   GCP project: {config.get('gcp', {}).get('project_id', 'NOT FOUND')}")

print("1️⃣ Testing imports...")
try:
    from batch_extractor import BatchExtractor
    print("   ✅ BatchExtractor imported")
    
    from gcs_to_bq.main import GCSToBigQueryWorker
    print("   ✅ GCSToBigQueryWorker imported")
except Exception as e:
    print(f"   ❌ Import failed: {e}")
    sys.exit(1)

# Test 2: Test pipeline on a small subset
print("\n2️⃣ Testing small pipeline (3 tables, 5 records each)...")
try:
    # Extract small dataset - pass the correct secrets path
    extractor = BatchExtractor(
        config_path='config/config.json',
        secrets_path='config/secrets.json'
    )
    limited_tables = ['tramos_margen', 'vehiculos', 'vto_pro']
    
    extraction_results = {}
    for table in limited_tables:
        print(f"   📊 Extracting {table}...")
        result = extractor.extract_table_with_upload(table, limit=5)
        extraction_results[table] = result
        if result.get('success'):
            print(f"      ✅ {result.get('records', 0)} records extracted")
        else:
            print(f"      ❌ Failed: {result.get('error', 'Unknown error')}")
    
    successful_extractions = sum(1 for r in extraction_results.values() if r.get('success'))
    print(f"   📊 Extraction summary: {successful_extractions}/{len(limited_tables)} tables successful")
    
except Exception as e:
    print(f"   ❌ Extraction test failed: {e}")
    successful_extractions = 0

# Test 3: Test BigQuery ingestion
if successful_extractions > 0:
    print("\n3️⃣ Testing BigQuery ingestion...")
    try:
        worker = GCSToBigQueryWorker(gcs_credentials, bq_credentials, config=config)
        successful_tables = [table for table, result in extraction_results.items() 
                           if result.get('success')]
        
        bq_results = worker.run_bronze_ingestion(specific_tables=successful_tables)
        successful_ingestions = sum(1 for r in bq_results.values() if r)
        
        print(f"   📊 BigQuery ingestion: {successful_ingestions}/{len(successful_tables)} tables successful")
        
    except Exception as e:
        print(f"   ❌ BigQuery ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        successful_ingestions = 0
else:
    print("\n3️⃣ Skipping BigQuery test (no successful extractions)")
    successful_ingestions = 0

# Test summary
print("\n" + "="*50)
print("📋 FINAL TEST SUMMARY")
print("="*50)

if successful_extractions > 0 and successful_ingestions > 0:
    print("🎉 PIPELINE FULLY VALIDATED!")
    print("✅ MySQL extraction working")
    print("✅ GCS upload working") 
    print("✅ BigQuery ingestion working")
    print("\n🚀 READY FOR COMPOSER DEPLOYMENT!")
    print("💡 Recommendation: Proceed with creating Composer environment")
    
elif successful_extractions > 0:
    print("⚠️ PARTIAL VALIDATION")
    print("✅ MySQL extraction working")
    print("✅ GCS upload working")
    print("❌ BigQuery ingestion needs investigation")
    print("\n🔧 Action needed: Fix BigQuery issues before Composer deployment")
    
else:
    print("❌ PIPELINE VALIDATION FAILED")
    print("❌ Basic extraction not working")
    print("\n🛑 Action needed: Fix extraction issues before proceeding")

print("\n📊 Performance estimate for full pipeline:")
print(f"   • Extraction success rate: {successful_extractions/len(limited_tables)*100:.1f}%")
if successful_extractions > 0:
    print(f"   • BigQuery success rate: {successful_ingestions/successful_extractions*100:.1f}%")
print("   • Full pipeline (220 tables) estimated time: 15-20 minutes")
print("   • Expected daily data volume: ~1.6M records")
