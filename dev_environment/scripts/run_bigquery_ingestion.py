#!/usr/bin/env python3
"""
Main BigQuery ingestion script
"""

import sys
import os
from datetime import date

# Add the bigquery directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'bigquery'))

from main import GCSToBigQueryWorker

def main():
    """Run BigQuery ingestion with advanced strategies"""
    print("🚀 Running BigQuery Ingestion")
    print("="*40)
    
    try:
        # Initialize worker with config
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
        worker = GCSToBigQueryWorker(config_path=config_path)
        
        print("✅ BigQuery worker initialized")
        
        # Run basic bronze ingestion
        print("\n📊 Running bronze ingestion...")
        bronze_results = worker.run_bronze_ingestion()
        
        bronze_success = sum(1 for result in bronze_results.values() if result)
        print(f"Bronze ingestion: {bronze_success}/{len(bronze_results)} tables successful")
        
        # Run advanced ingestion for today
        today = date.today()
        print(f"\n🔧 Running advanced ingestion for {today}...")
        
        advanced_results = worker.run_advanced_ingestion(today)
        
        advanced_success = sum(1 for table, result in advanced_results.items() if result.get('success', False))
        print(f"Advanced ingestion: {advanced_success}/{len(advanced_results)} tables successful")
        
        if bronze_success > 0 or advanced_success > 0:
            print("\n✅ BigQuery ingestion completed successfully!")
            return True
        else:
            print("\n❌ No tables processed successfully")
            return False
            
    except Exception as e:
        print(f"❌ BigQuery ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
