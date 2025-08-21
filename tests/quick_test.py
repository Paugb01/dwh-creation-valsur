"""
Quick 5-minute pipeline test
Fast validation before running comprehensive tests
"""
import sys
import time
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "batch_only"))
sys.path.append(str(project_root / "batch_only" / "gcs_to_bq"))

def quick_test():
    print("⚡ QUICK PIPELINE TEST (5 minutes max)")
    print("="*50)
    
    # Test 1: Connections (30 seconds)
    print("1️⃣ Testing connections...")
    try:
        from batch_extractor import BatchExtractor
        from main import GCSToBigQueryWorker
        
        print("   📊 MySQL connection...")
        extractor = BatchExtractor()
        print("   ✅ MySQL OK")
        
        print("   ☁️ GCS/BigQuery connection...")
        worker = GCSToBigQueryWorker()
        print("   ✅ GCS/BigQuery OK")
        
        print("   ✅ All connections successful")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return False
    
    # Test 2: Small extraction (2 minutes)
    print("\n2️⃣ Testing small extraction (10 records per table)...")
    start = time.time()
    try:
        results = extractor.extract_all_tables(limit_per_table=10)
        successful = sum(1 for r in results.values() if r.get('success'))
        total_records = sum(r.get('records', 0) for r in results.values() if r.get('success'))
        print(f"   ✅ {successful}/{len(results)} tables extracted in {time.time()-start:.1f}s")
        print(f"   📊 {total_records} total records extracted")
        
        if successful == 0:
            print("   ❌ No tables extracted successfully")
            return False
            
    except Exception as e:
        print(f"   ❌ Extraction failed: {e}")
        return False
    
    # Test 3: BigQuery ingestion (2 minutes)
    print("\n3️⃣ Testing BigQuery ingestion...")
    start = time.time()
    try:
        # Only test tables that were successfully extracted
        successful_tables = [name for name, result in results.items() if result.get('success')]
        bq_results = worker.run_bronze_ingestion(specific_tables=successful_tables)
        successful_bq = sum(1 for r in bq_results.values() if r)
        print(f"   ✅ {successful_bq}/{len(bq_results)} tables ingested in {time.time()-start:.1f}s")
        
        if successful_bq == 0:
            print("   ❌ No tables ingested successfully")
            return False
            
    except Exception as e:
        print(f"   ❌ BigQuery ingestion failed: {e}")
        return False
    
    print("\n🎉 QUICK TEST PASSED - Ready for comprehensive testing!")
    return True

def main():
    start_time = time.time()
    
    if quick_test():
        total_time = time.time() - start_time
        print(f"\n✅ Quick test completed in {total_time:.1f} seconds")
        print("🚀 You can now run the comprehensive test: python test_pipeline.py")
        return True
    else:
        total_time = time.time() - start_time
        print(f"\n❌ Quick test failed after {total_time:.1f} seconds")
        print("⚠️ Fix issues before proceeding with Composer deployment")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
