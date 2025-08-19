#!/usr/bin/env python3
"""
GCS Bronze Layer Cleanup Script
This script helps clean up duplicate extractions in the bronze layer
"""

import subprocess
import re
from datetime import datetime
from collections import defaultdict

def list_gcs_files(bucket_path):
    """List all files in a GCS path"""
    result = subprocess.run(
        ['gcloud', 'storage', 'ls', bucket_path, '--recursive'],
        capture_output=True, text=True
    )
    return result.stdout.strip().split('\n')

def parse_file_info(file_path):
    """Extract table name and timestamp from file path"""
    # Extract table name and timestamp from path like:
    # gs://bucket/bronze/pk_gest_xer/table_name/date=2025/08/18/table_name_full_20250818_112905.parquet
    match = re.search(r'/([^/]+)/date=\d{4}/\d{2}/\d{2}/([^/]+)\.parquet$', file_path)
    if match:
        table_name = match.group(1)
        filename = match.group(2)
        
        # Extract timestamp from filename
        timestamp_match = re.search(r'(\d{8}_\d{6})', filename)
        if timestamp_match:
            timestamp_str = timestamp_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            return table_name, timestamp, filename
    return None, None, None

def find_duplicates():
    """Find duplicate files in bronze layer"""
    bucket_path = 'gs://valsurtruck-dwh-bronze/bronze/pk_gest_xer/'
    files = list_gcs_files(bucket_path)
    
    # Group files by table and type (full/incremental)
    table_files = defaultdict(lambda: defaultdict(list))
    
    for file_path in files:
        if file_path.endswith('.parquet'):
            table_name, timestamp, filename = parse_file_info(file_path)
            if table_name and timestamp:
                file_type = 'full' if '_full_' in filename else 'incremental'
                table_files[table_name][file_type].append({
                    'path': file_path,
                    'timestamp': timestamp,
                    'filename': filename
                })
    
    return table_files

def generate_cleanup_commands():
    """Generate cleanup commands"""
    table_files = find_duplicates()
    
    print("=== GCS BRONZE LAYER CLEANUP ANALYSIS ===\n")
    
    files_to_delete = []
    files_to_keep = []
    
    for table_name, file_types in table_files.items():
        print(f"📊 Table: {table_name}")
        
        for file_type, files in file_types.items():
            if len(files) > 1:
                # Sort by timestamp, keep the latest
                files.sort(key=lambda x: x['timestamp'], reverse=True)
                latest = files[0]
                older_files = files[1:]
                
                print(f"  ✅ {file_type.upper()} - Keep latest: {latest['filename']}")
                files_to_keep.append(latest['path'])
                
                for old_file in older_files:
                    print(f"  🗑️  {file_type.upper()} - DELETE older: {old_file['filename']}")
                    files_to_delete.append(old_file['path'])
            else:
                print(f"  ✅ {file_type.upper()} - Only one file: {files[0]['filename']}")
                files_to_keep.append(files[0]['path'])
        print()
    
    print(f"\n=== SUMMARY ===")
    print(f"Files to keep: {len(files_to_keep)}")
    print(f"Files to delete: {len(files_to_delete)}")
    
    if files_to_delete:
        print(f"\n=== CLEANUP COMMANDS ===")
        print("# Run these commands to delete duplicate files:")
        for file_path in files_to_delete:
            print(f"gcloud storage rm {file_path}")
        
        print(f"\n# Or delete all at once (CAREFUL!):")
        files_list = ' '.join(f'"{f}"' for f in files_to_delete)
        print(f"gcloud storage rm {files_list}")
    
    return files_to_delete, files_to_keep

if __name__ == "__main__":
    try:
        files_to_delete, files_to_keep = generate_cleanup_commands()
        
        if files_to_delete:
            print(f"\n⚠️  WARNING: This will delete {len(files_to_delete)} files!")
            print("Please review the commands above before executing them.")
        else:
            print("✅ No duplicate files found - your GCS environment is clean!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure gcloud CLI is installed and authenticated.")
