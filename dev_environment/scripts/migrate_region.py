#!/usr/bin/env python3
"""
Migrate BigQuery resources from US to europe-southwest1
"""

import sys
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Add the gcs_to_bq directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'gcs_to_bq'))

from main import GCSToBigQueryWorker

def migrate_to_europe_southwest1():
    """Migrate all BigQuery resources to europe-southwest1"""
    print("🚀 Migrating BigQuery Resources to europe-southwest1")
    print("="*60)
    
    try:
        # Initialize worker with new config
        config_path = "gcs_to_bq/config/config.json"
        worker = GCSToBigQueryWorker(config_path=config_path)
        
        print(f"✅ Worker initialized")
        print(f"   Project: {worker.project_id}")
        print(f"   New Location: {worker.bq_location}")
        print(f"   Datasets: {worker.bronze_dataset}, {worker.silver_dataset}, {worker.gold_dataset}")
        
        # Step 1: Create new datasets in europe-southwest1
        print("\n📊 Step 1: Creating datasets in europe-southwest1...")
        
        datasets_to_create = [
            (worker.bronze_dataset, "Bronze layer for raw data"),
            (worker.silver_dataset, "Silver layer for processed data"), 
            (worker.gold_dataset, "Gold layer for analytics-ready data")
        ]
        
        for dataset_name, description in datasets_to_create:
            dataset_id = f"{worker.project_id}.{dataset_name}"
            
            try:
                # Check if dataset already exists in europe-southwest1
                existing_dataset = worker.bq_client.get_dataset(dataset_id)
                if existing_dataset.location.lower() == worker.bq_location.lower():
                    print(f"   ✅ Dataset {dataset_name} already exists in {worker.bq_location}")
                    continue
                else:
                    print(f"   ⚠️  Dataset {dataset_name} exists in {existing_dataset.location}, will create new one")
                    # We'll need to use a different approach to handle this
                    
            except NotFound:
                print(f"   📝 Creating dataset {dataset_name} in {worker.bq_location}...")
                
            # Create dataset in europe-southwest1
            try:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = worker.bq_location
                dataset.description = description
                
                created_dataset = worker.bq_client.create_dataset(dataset, exists_ok=True)
                print(f"   ✅ Created {dataset_name} in {created_dataset.location}")
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"   ✅ Dataset {dataset_name} already exists")
                else:
                    print(f"   ❌ Failed to create {dataset_name}: {e}")
                    raise
        
        # Step 2: Check what tables exist in US region
        print("\n📋 Step 2: Checking existing tables in US region...")
        
        us_tables = []
        for dataset_name in [worker.bronze_dataset, worker.silver_dataset, worker.gold_dataset]:
            try:
                us_dataset_id = f"{worker.project_id}.{dataset_name}"
                tables = list(worker.bq_client.list_tables(us_dataset_id))
                
                for table in tables:
                    table_info = worker.bq_client.get_table(table.reference)
                    if table_info.location == "US":
                        us_tables.append({
                            'dataset': dataset_name,
                            'table': table.table_id,
                            'rows': table_info.num_rows,
                            'size_mb': round(table_info.num_bytes / 1024 / 1024, 2) if table_info.num_bytes else 0
                        })
                        print(f"   📊 Found: {dataset_name}.{table.table_id} ({table_info.num_rows:,} rows, {round(table_info.num_bytes / 1024 / 1024, 2) if table_info.num_bytes else 0} MB)")
                        
            except Exception as e:
                print(f"   ⚠️  Could not check {dataset_name}: {e}")
        
        if us_tables:
            print(f"\n💾 Found {len(us_tables)} tables in US region")
            
            # Step 3: Offer options for data migration
            print("\n🔄 Step 3: Data Migration Options")
            print("   Option 1: Re-extract data from MySQL (recommended)")
            print("   Option 2: Copy data from US to europe-southwest1 (cross-region transfer)")
            print("   Option 3: Proceed without data migration (empty datasets)")
            
            choice = input("\nSelect option (1/2/3): ").strip()
            
            if choice == "1":
                print("\n🔄 Re-extracting data from MySQL...")
                return re_extract_data(worker)
                
            elif choice == "2":
                print("\n🔄 Copying data from US to europe-southwest1...")
                return copy_data_cross_region(worker, us_tables)
                
            elif choice == "3":
                print("\n✅ Migration completed - datasets created in europe-southwest1")
                print("   Note: You'll need to re-run data extraction to populate the datasets")
                return True
                
            else:
                print("❌ Invalid choice")
                return False
        else:
            print("\n✅ No existing data found - datasets ready in europe-southwest1")
            return True
            
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def re_extract_data(worker):
    """Re-extract data from MySQL to populate europe-southwest1 datasets"""
    print("\n🔄 Re-extracting data from MySQL...")
    
    try:
        # Run basic bronze ingestion
        print("   📊 Running bronze ingestion...")
        results = worker.run_bronze_ingestion()
        
        success_count = sum(1 for result in results if result.get('success', False))
        total_count = len(results)
        
        if success_count > 0:
            print(f"   ✅ Bronze ingestion: {success_count}/{total_count} tables successful")
            
            # Also run advanced ingestion if we have data
            try:
                from datetime import date
                today = date.today()
                print(f"   🚀 Running advanced ingestion for {today}...")
                
                advanced_results = worker.run_advanced_ingestion(today)
                print(f"   📊 Advanced ingestion completed")
                
            except Exception as e:
                print(f"   ⚠️  Advanced ingestion skipped: {e}")
            
            print("\n✅ Data migration completed successfully!")
            return True
        else:
            print(f"   ❌ Bronze ingestion failed: {success_count}/{total_count} tables successful")
            return False
            
    except Exception as e:
        print(f"   ❌ Re-extraction failed: {e}")
        return False

def copy_data_cross_region(worker, us_tables):
    """Copy data from US to europe-southwest1 (cross-region transfer)"""
    print("\n🔄 Cross-region data copy...")
    print("   ⚠️  Note: Cross-region transfers incur additional costs")
    
    confirm = input("   Continue with cross-region copy? (y/N): ").strip().lower()
    if confirm != 'y':
        print("   ❌ Cross-region copy cancelled")
        return False
    
    copied_count = 0
    
    for table_info in us_tables:
        try:
            source_table = f"{worker.project_id}.{table_info['dataset']}.{table_info['table']}"
            dest_table = source_table  # Same name, different region
            
            print(f"   📋 Copying {source_table}...")
            
            # Create copy job
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            
            job = worker.bq_client.copy_table(
                source_table, 
                dest_table, 
                job_config=job_config,
                location=worker.bq_location  # Destination location
            )
            
            job.result()  # Wait for completion
            
            # Verify copy
            dest_table_obj = worker.bq_client.get_table(dest_table)
            print(f"   ✅ Copied {dest_table_obj.num_rows:,} rows to {worker.bq_location}")
            copied_count += 1
            
        except Exception as e:
            print(f"   ❌ Failed to copy {table_info['table']}: {e}")
    
    print(f"\n📊 Copy completed: {copied_count}/{len(us_tables)} tables")
    
    if copied_count > 0:
        print("\n🗑️  Clean up old US datasets? (recommended)")
        cleanup = input("   Delete US datasets? (y/N): ").strip().lower()
        
        if cleanup == 'y':
            cleanup_us_datasets(worker)
    
    return copied_count > 0

def cleanup_us_datasets(worker):
    """Clean up old US datasets"""
    print("\n🗑️  Cleaning up US datasets...")
    
    for dataset_name in [worker.bronze_dataset, worker.silver_dataset, worker.gold_dataset]:
        try:
            dataset_id = f"{worker.project_id}.{dataset_name}"
            dataset = worker.bq_client.get_dataset(dataset_id)
            
            if dataset.location == "US":
                print(f"   🗑️  Deleting US dataset: {dataset_name}")
                worker.bq_client.delete_dataset(dataset_id, delete_contents=True)
                print(f"   ✅ Deleted US dataset: {dataset_name}")
            else:
                print(f"   ℹ️  Dataset {dataset_name} is not in US region")
                
        except NotFound:
            print(f"   ℹ️  Dataset {dataset_name} not found in US")
        except Exception as e:
            print(f"   ❌ Failed to delete {dataset_name}: {e}")

if __name__ == "__main__":
    success = migrate_to_europe_southwest1()
    
    if success:
        print("\n🎉 Migration to europe-southwest1 completed!")
        print("   Next steps:")
        print("   1. Test the pipeline with the new location")
        print("   2. Update any scripts/configs that reference the old location")
        print("   3. Verify all data is accessible in the new region")
    else:
        print("\n❌ Migration failed - please check errors above")
    
    sys.exit(0 if success else 1)
