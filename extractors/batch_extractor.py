"""
Production Batch Extractor
Efficiently extracts all tables with intelligent incremental/full loading strategy
"""
import pymysql
import pandas as pd
import os
import json
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config_manager import config_manager
from incremental_extractor import IncrementalExtractor

class ProductionBatchExtractor(IncrementalExtractor):
    def __init__(self, max_workers: int = 4):
        super().__init__()
        self.max_workers = max_workers
        self.extraction_stats = {
            'total_tables': 0,
            'incremental_tables': 0,
            'full_extraction_tables': 0,
            'failed_tables': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }
        
    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        connection = self.get_mysql_connection()
        try:
            cursor = connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            return sorted(tables)
        finally:
            connection.close()
    
    def categorize_tables(self, tables: List[str]) -> Tuple[List[Dict], List[Dict]]:
        """Categorize tables into incremental-capable and full-extraction-only"""
        print(f"Analyzing {len(tables)} tables for incremental loading capability...")
        
        incremental_tables = []
        full_extraction_tables = []
        
        connection = self.get_mysql_connection()
        try:
            cursor = connection.cursor()
            
            for i, table_name in enumerate(tables, 1):
                if i % 50 == 0:
                    print(f"  Analyzed {i}/{len(tables)} tables...")
                
                try:
                    # Quick analysis
                    cursor.execute(f"DESCRIBE {table_name}")
                    columns = cursor.fetchall()
                    
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    
                    # Find timestamp columns
                    timestamp_cols = []
                    for col in columns:
                        col_name, col_type = col[0], col[1]
                        if any(time_type in col_type.lower() for time_type in ['timestamp', 'datetime', 'date']):
                            timestamp_cols.append(col_name)
                    
                    table_info = {
                        'name': table_name,
                        'rows': row_count,
                        'timestamp_columns': timestamp_cols
                    }
                    
                    if timestamp_cols and row_count > 0:
                        incremental_tables.append(table_info)
                    else:
                        full_extraction_tables.append(table_info)
                        
                except Exception as e:
                    print(f"    Warning: Could not analyze {table_name}: {e}")
                    full_extraction_tables.append({
                        'name': table_name,
                        'rows': 0,
                        'timestamp_columns': []
                    })
        
        finally:
            connection.close()
        
        # Sort by row count (largest first)
        incremental_tables.sort(key=lambda x: x['rows'], reverse=True)
        full_extraction_tables.sort(key=lambda x: x['rows'], reverse=True)
        
        return incremental_tables, full_extraction_tables
    
    def create_extraction_plan(self, incremental_tables: List[Dict], full_extraction_tables: List[Dict], 
                             max_full_table_size: int = 100000) -> Dict:
        """Create an intelligent extraction plan"""
        extraction_config = self.config_manager.get_extraction_config()
        
        plan = {
            'strategy': 'mixed',
            'incremental_extraction': [],
            'full_extraction_small': [],
            'full_extraction_large': [],
            'skipped_tables': [],
            'total_estimated_records': 0
        }
        
        # Incremental tables (always include)
        for table in incremental_tables:
            plan['incremental_extraction'].append(table)
            # Estimate records (assume 10% new data for incremental)
            plan['total_estimated_records'] += min(table['rows'] * 0.1, 10000)
        
        # Full extraction tables - categorize by size
        for table in full_extraction_tables:
            if table['rows'] == 0:
                plan['skipped_tables'].append(table)
            elif table['rows'] <= max_full_table_size:
                plan['full_extraction_small'].append(table)
                plan['total_estimated_records'] += table['rows']
            else:
                plan['full_extraction_large'].append(table)
                # For large tables, we'll limit extraction
                plan['total_estimated_records'] += min(table['rows'], extraction_config.get('large_table_limit', 50000))
        
        return plan
    
    def print_extraction_plan(self, plan: Dict):
        """Print the extraction plan summary"""
        print(f"\n{'='*80}")
        print("EXTRACTION PLAN")
        print(f"{'='*80}")
        
        print(f"📊 Incremental Loading: {len(plan['incremental_extraction'])} tables")
        if plan['incremental_extraction']:
            for table in plan['incremental_extraction'][:5]:
                print(f"  • {table['name']:<30} {table['rows']:>10,} rows  {table['timestamp_columns']}")
            if len(plan['incremental_extraction']) > 5:
                print(f"  • ... and {len(plan['incremental_extraction']) - 5} more tables")
        
        print(f"\n📋 Full Extraction (Small): {len(plan['full_extraction_small'])} tables")
        if plan['full_extraction_small']:
            for table in plan['full_extraction_small'][:5]:
                print(f"  • {table['name']:<30} {table['rows']:>10,} rows")
            if len(plan['full_extraction_small']) > 5:
                print(f"  • ... and {len(plan['full_extraction_small']) - 5} more tables")
        
        print(f"\n🏗️  Full Extraction (Large): {len(plan['full_extraction_large'])} tables")
        if plan['full_extraction_large']:
            for table in plan['full_extraction_large'][:5]:
                print(f"  • {table['name']:<30} {table['rows']:>10,} rows  (limited extraction)")
            if len(plan['full_extraction_large']) > 5:
                print(f"  • ... and {len(plan['full_extraction_large']) - 5} more tables")
        
        print(f"\n⏭️  Skipped (Empty): {len(plan['skipped_tables'])} tables")
        
        print(f"\n🎯 Estimated Total Records: {plan['total_estimated_records']:,}")
        print(f"{'='*80}")
    
    def extract_table_safe(self, table_info: Dict, extraction_type: str = 'auto') -> Dict:
        """Safely extract a single table with error handling"""
        table_name = table_info['name']
        
        try:
            if extraction_type == 'incremental' or (extraction_type == 'auto' and table_info['timestamp_columns']):
                # Try incremental extraction
                success = self.extract_table_incremental(table_name, limit=None)
                return {
                    'table': table_name,
                    'success': success,
                    'extraction_type': 'incremental',
                    'error': None
                }
            else:
                # Full extraction with size limits
                extraction_config = self.config_manager.get_extraction_config()
                
                if table_info['rows'] > 100000:
                    # Large table - limit extraction
                    limit = extraction_config.get('large_table_limit', 50000)
                    print(f"⚠️  Large table {table_name} ({table_info['rows']:,} rows) - limiting to {limit:,} rows")
                else:
                    limit = None
                
                success = self.extract_table_incremental(table_name, limit=limit)
                return {
                    'table': table_name,
                    'success': success,
                    'extraction_type': 'full',
                    'error': None
                }
                
        except Exception as e:
            print(f"❌ Failed to extract {table_name}: {e}")
            return {
                'table': table_name,
                'success': False,
                'extraction_type': extraction_type,
                'error': str(e)
            }
    
    def run_batch_extraction(self, plan: Dict, dry_run: bool = False):
        """Run the complete batch extraction"""
        if dry_run:
            print("\n🔍 DRY RUN MODE - No actual extraction will be performed")
            return
        
        self.extraction_stats['start_time'] = datetime.now()
        self.extraction_stats['total_tables'] = (
            len(plan['incremental_extraction']) + 
            len(plan['full_extraction_small']) + 
            len(plan['full_extraction_large'])
        )
        
        results = []
        
        print(f"\n🚀 Starting batch extraction of {self.extraction_stats['total_tables']} tables...")
        print(f"Using {self.max_workers} worker threads")
        
        # Extract incremental tables first (usually faster)
        if plan['incremental_extraction']:
            print(f"\n📊 Processing {len(plan['incremental_extraction'])} incremental tables...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_table = {
                    executor.submit(self.extract_table_safe, table, 'incremental'): table 
                    for table in plan['incremental_extraction']
                }
                
                for future in concurrent.futures.as_completed(future_to_table):
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        self.extraction_stats['incremental_tables'] += 1
                        print(f"✅ {result['table']} (incremental)")
                    else:
                        self.extraction_stats['failed_tables'] += 1
                        print(f"❌ {result['table']} - {result['error']}")
        
        # Extract small full tables
        if plan['full_extraction_small']:
            print(f"\n📋 Processing {len(plan['full_extraction_small'])} small tables (full extraction)...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_table = {
                    executor.submit(self.extract_table_safe, table, 'full'): table 
                    for table in plan['full_extraction_small']
                }
                
                for future in concurrent.futures.as_completed(future_to_table):
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        self.extraction_stats['full_extraction_tables'] += 1
                        print(f"✅ {result['table']} (full)")
                    else:
                        self.extraction_stats['failed_tables'] += 1
                        print(f"❌ {result['table']} - {result['error']}")
        
        # Extract large tables (one by one to avoid memory issues)
        if plan['full_extraction_large']:
            print(f"\n🏗️  Processing {len(plan['full_extraction_large'])} large tables (limited extraction)...")
            for table in plan['full_extraction_large']:
                result = self.extract_table_safe(table, 'full')
                results.append(result)
                
                if result['success']:
                    self.extraction_stats['full_extraction_tables'] += 1
                    print(f"✅ {result['table']} (limited)")
                else:
                    self.extraction_stats['failed_tables'] += 1
                    print(f"❌ {result['table']} - {result['error']}")
        
        self.extraction_stats['end_time'] = datetime.now()
        
        # Save final watermarks
        self.save_watermarks()
        
        # Print final summary
        self.print_final_summary(results)
        
        return results
    
    def print_final_summary(self, results: List[Dict]):
        """Print final extraction summary"""
        duration = self.extraction_stats['end_time'] - self.extraction_stats['start_time']
        
        print(f"\n{'='*80}")
        print("EXTRACTION COMPLETE")
        print(f"{'='*80}")
        print(f"⏱️  Duration: {duration}")
        print(f"📊 Total Tables: {self.extraction_stats['total_tables']}")
        print(f"✅ Successful: {self.extraction_stats['incremental_tables'] + self.extraction_stats['full_extraction_tables']}")
        print(f"   - Incremental: {self.extraction_stats['incremental_tables']}")
        print(f"   - Full: {self.extraction_stats['full_extraction_tables']}")
        print(f"❌ Failed: {self.extraction_stats['failed_tables']}")
        
        # Show failed tables
        failed_tables = [r['table'] for r in results if not r['success']]
        if failed_tables:
            print(f"\n❌ Failed tables:")
            for table in failed_tables[:10]:
                print(f"   • {table}")
            if len(failed_tables) > 10:
                print(f"   • ... and {len(failed_tables) - 10} more")
        
        # Show watermark summary
        if self.watermarks:
            print(f"\n💾 Watermarks saved for {len(self.watermarks)} tables")
        
        print(f"{'='*80}")

def main():
    """Run production batch extraction"""
    print("Production Batch MySQL to GCS Extractor")
    print("=" * 80)
    
    # Initialize extractor
    extractor = ProductionBatchExtractor(max_workers=3)  # Conservative for database
    
    # Load existing watermarks
    extractor.load_watermarks()
    
    # Initialize GCS
    gcs_available = extractor.initialize_gcs()
    if not gcs_available:
        print("⚠️  GCS not available. Will save locally only.")
    
    # Get all tables
    all_tables = extractor.get_all_tables()
    print(f"Found {len(all_tables)} tables in database")
    
    # Categorize tables
    incremental_tables, full_extraction_tables = extractor.categorize_tables(all_tables)
    
    # Create extraction plan
    plan = extractor.create_extraction_plan(incremental_tables, full_extraction_tables)
    
    # Show plan
    extractor.print_extraction_plan(plan)
    
    # Ask for confirmation
    response = input(f"\nProceed with extraction? (y/N): ").strip().lower()
    
    if response == 'y':
        # Run extraction
        results = extractor.run_batch_extraction(plan, dry_run=False)
        print(f"\n🎉 Batch extraction completed!")
    else:
        print("Extraction cancelled.")

if __name__ == "__main__":
    main()
