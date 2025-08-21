#!/usr/bin/env python3
"""
Script to check BigQuery service account identity and permissions
"""

import os
import sys
import json
from google.cloud import bigquery
from google.oauth2 import service_account

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from batch_only.utils import setup_environment, get_credentials_auto

def check_bq_identity_and_permissions():
    """
    Check what service account BigQuery is using and its permissions
    """
    print("🔍 CHECKING BIGQUERY SERVICE ACCOUNT IDENTITY AND PERMISSIONS")
    print("=" * 70)
    
    try:
        # Setup environment
        setup_environment()
        
        # Get credentials using automatic detection
        credentials = get_credentials_auto()
        
        if credentials:
            print(f"✅ Using auto-detected credentials")
            if hasattr(credentials, 'service_account_email'):
                print(f"📧 Service Account Email: {credentials.service_account_email}")
            if hasattr(credentials, 'project_id'): 
                print(f"🏗️  Project ID from credentials: {credentials.project_id}")
                
            # Also check the actual credential source
            if hasattr(credentials, '_service_account_email'):
                print(f"📧 Service Account Email: {credentials._service_account_email}")
        else:
            print("⚠️  Using default application credentials")
        
        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials)
        
        print(f"🏗️  BigQuery Client Project: {client.project}")
        print("-" * 70)
        
        # Test 1: Check if we can list datasets
        print("📋 TEST 1: Listing datasets...")
        try:
            datasets = list(client.list_datasets())
            print(f"✅ Found {len(datasets)} datasets:")
            for dataset in datasets:
                print(f"   - {dataset.dataset_id}")
        except Exception as e:
            print(f"❌ Failed to list datasets: {e}")
        
        print("-" * 70)
        
        # Test 2: Check specific dataset access
        target_datasets = ['bronze1', 'silver1', 'gold1']
        
        for dataset_name in target_datasets:
            print(f"🔍 TEST 2: Checking access to dataset '{dataset_name}'...")
            
            try:
                dataset_id = f"{client.project}.{dataset_name}"
                dataset = client.get_dataset(dataset_id)
                print(f"✅ Can read dataset: {dataset_name}")
                print(f"   Location: {dataset.location}")
                print(f"   Created: {dataset.created}")
                
                # Try to list tables
                tables = list(client.list_tables(dataset))
                print(f"   Tables: {len(tables)} found")
                
                # Test write permissions by trying to create a temporary table
                print(f"🔍 Testing WRITE permissions to {dataset_name}...")
                
                # Create a simple test table
                test_table_id = f"{dataset_id}.permission_test_table"
                
                # Define schema for test table
                schema = [
                    bigquery.SchemaField("test_field", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                ]
                
                # Create test table
                table = bigquery.Table(test_table_id, schema=schema)
                table = client.create_table(table, exists_ok=True)
                print(f"✅ WRITE permission confirmed - created test table")
                
                # Insert test data
                test_data = [
                    {"test_field": "permission_test", "timestamp": "2025-08-21T08:00:00"}
                ]
                
                errors = client.insert_rows_json(table, test_data)
                if not errors:
                    print(f"✅ INSERT permission confirmed - added test data")
                else:
                    print(f"❌ INSERT failed: {errors}")
                
                # Clean up - delete test table
                client.delete_table(test_table_id)
                print(f"✅ DELETE permission confirmed - cleaned up test table")
                
            except Exception as e:
                print(f"❌ Access denied to dataset {dataset_name}: {e}")
                print(f"   Error type: {type(e).__name__}")
                
                # More detailed error analysis
                if "403" in str(e):
                    print(f"   🚨 HTTP 403 - Permission denied")
                    print(f"   💡 The service account may not have sufficient IAM roles")
                elif "404" in str(e):
                    print(f"   🚨 HTTP 404 - Dataset not found")
                
            print()
        
        print("-" * 70)
        
        # Test 3: Check IAM roles (if possible)
        print("🔍 TEST 3: Attempting to check IAM permissions...")
        
        try:
            # This requires additional permissions, but let's try
            from google.cloud import resource_manager
            
            # Try to get IAM policy for the project
            # Note: This might fail if the service account doesn't have the right permissions
            print("   Checking project-level IAM roles...")
            
        except ImportError:
            print("   ⚠️  google-cloud-resource-manager not installed")
        except Exception as e:
            print(f"   ❌ Cannot check IAM roles: {e}")
        
        print("-" * 70)
        
        # Summary
        print("📊 SUMMARY:")
        print(f"Service Account: mysql-to-gcs-extractor@dwh-building.iam.gserviceaccount.com")
        print(f"Project: {client.project}")
        print("Required BigQuery IAM Roles:")
        print("  - BigQuery Data Editor (bigquery.dataEditor)")
        print("  - BigQuery Job User (bigquery.jobUser)")
        print("  - Storage Object Viewer (storage.objectViewer) for GCS access")
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_bq_identity_and_permissions()
