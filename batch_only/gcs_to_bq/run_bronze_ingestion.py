"""
Simple runner script for GCS to BigQuery bronze ingestion
"""
import os
import sys
from datetime import datetime
from google.oauth2 import service_account
from dotenv import load_dotenv

# Add the current directory to Python path
sys.path.append(os.path.dirname(__file__))

# Load dotenv
load_dotenv()

def load_credentials():
    """Load GCS and BigQuery credentials from environment variables or config"""
    gcs_creds_path = os.getenv("GCS_APPLICATION_CREDENTIALS")
    bq_creds_path = os.getenv("BQ_APPLICATION_CREDENTIALS")
    
    gcs_credentials = None
    bq_credentials = None
    
    if gcs_creds_path and os.path.exists(gcs_creds_path):
        try:
            gcs_credentials = service_account.Credentials.from_service_account_file(gcs_creds_path)
            print(f"✅ GCS credentials loaded from: {gcs_creds_path}")
        except Exception as e:
            print(f"⚠️  Failed to load GCS credentials: {e}")
    else:
        print(f"ℹ️  GCS credentials not found in environment, will use config/default")
    
    if bq_creds_path and os.path.exists(bq_creds_path):
        try:
            bq_credentials = service_account.Credentials.from_service_account_file(bq_creds_path)
            print(f"✅ BigQuery credentials loaded from: {bq_creds_path}")
        except Exception as e:
            print(f"⚠️  Failed to load BigQuery credentials: {e}")
    else:
        print(f"ℹ️  BigQuery credentials not found in environment, will use config/default")
    
    return gcs_credentials, bq_credentials

def main():
    """Main execution function"""
    print("="*70)
    print("GCS TO BIGQUERY BRONZE INGESTION")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Load credentials
    print("Loading credentials...")
    gcs_credentials, bq_credentials = load_credentials()
    print()
    
    try:
        from main import GCSToBigQueryWorker
        
        # Initialize worker with explicit credentials
        print("Initializing GCS to BigQuery worker...")
        worker = GCSToBigQueryWorker(
            gcs_credentials=gcs_credentials,
            bq_credentials=bq_credentials
        )
        
        # Show configuration
        print(f"Project ID: {worker.project_id}")
        print(f"Bucket: {worker.bucket_name}")
        print(f"Source Database: {worker.source_database}")
        print(f"Target Tables: {', '.join(worker.target_tables)}")
        print()
        
        # Run bronze ingestion
        print("Starting bronze ingestion process...")
        results = worker.run_bronze_ingestion()
        
        # Validate results
        print("\nValidating bronze tables...")
        validation = worker.validate_bronze_tables()
        
        # Summary
        print("\n" + "="*70)
        print("BRONZE INGESTION SUMMARY")
        print("="*70)
        
        for table_name in worker.target_tables:
            success = results.get(table_name, False)
            info = validation.get(table_name, {})
            
            status = "✅ SUCCESS" if success else "❌ FAILED"
            rows = info.get('num_rows', 0) if info.get('exists') else 0
            size_mb = info.get('size_bytes', 0) / (1024*1024) if info.get('size_bytes') else 0
            
            print(f"{table_name:<15} {status:<12} {rows:>10,} rows  {size_mb:>8.1f} MB")
        
        # Overall statistics
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        success_rate = (successful / total * 100) if total > 0 else 0
        
        print("-" * 70)
        print(f"Total Tables: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {total - successful}")
        print(f"Success Rate: {success_rate:.1f}%")
        
        if successful == total:
            print("\n🎉 All tables successfully ingested to bronze layer!")
        else:
            print(f"\n⚠️  {total - successful} tables failed. Check logs for details.")
            
    except Exception as e:
        print(f"\n❌ Error during execution: {e}")
        print("Run test_config.py to diagnose configuration issues.")
        return 1
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
