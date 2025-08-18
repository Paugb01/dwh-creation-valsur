"""
Incremental MySQL to GCS Data Extractor
Extracts only new/changed data based on timestamp columns and watermarks
"""
from .base_extractor import BaseExtractor
import pandas as pd
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

class IncrementalExtractor(BaseExtractor):
    def __init__(self):
        super().__init__()
        self.watermarks = {}  # Track last extraction timestamps per table
        
    def initialize_gcs(self):
        """Initialize Google Cloud Storage client and bucket (deprecated - now handled by base class)"""
        # This method is now handled by the base class setup_gcs()
        return self.storage_client is not None

    def get_mysql_connection(self):
        """Get MySQL connection using base class method"""
        return super().get_mysql_connection()

    def analyze_table_structure(self, table_name: str) -> Dict:
        """Analyze table structure to identify timestamp columns and primary keys"""
        connection = self.get_mysql_connection()
        
        try:
            cursor = connection.cursor()
            
            # Get table structure
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]
            
            # Analyze columns
            timestamp_columns = []
            primary_keys = []
            all_columns = []
            
            for col in columns:
                col_name, col_type, null, key, default, extra = col
                all_columns.append(col_name)
                
                if key == 'PRI':
                    primary_keys.append(col_name)
                
                # Look for timestamp/datetime columns
                if any(time_type in col_type.lower() for time_type in ['timestamp', 'datetime', 'date']):
                    timestamp_columns.append({
                        'name': col_name,
                        'type': col_type,
                        'nullable': null == 'YES'
                    })
            
            # Try to find the best timestamp column for incremental loading
            best_timestamp_col = self._find_best_timestamp_column(cursor, table_name, timestamp_columns)
            
            analysis = {
                'table_name': table_name,
                'total_rows': total_rows,
                'columns': all_columns,
                'primary_keys': primary_keys,
                'timestamp_columns': timestamp_columns,
                'best_timestamp_column': best_timestamp_col,
                'supports_incremental': best_timestamp_col is not None,
                'analysis_time': datetime.now().isoformat()
            }
            
            return analysis
            
        finally:
            connection.close()

    def _find_best_timestamp_column(self, cursor, table_name: str, timestamp_columns: List[Dict]) -> Optional[str]:
        """Find the best timestamp column for incremental loading"""
        if not timestamp_columns:
            return None
        
        # Priority order for common timestamp column names
        preferred_names = [
            'updated_at', 'modified_at', 'last_modified', 'updated_date',
            'created_at', 'created_date', 'insert_date', 'date_created',
            'timestamp', 'last_update', 'fecha_modificacion', 'fecha_creacion'
        ]
        
        # First, try to find preferred column names
        for preferred in preferred_names:
            for col in timestamp_columns:
                if preferred.lower() in col['name'].lower():
                    # Check if this column has recent data
                    try:
                        cursor.execute(f"SELECT MAX({col['name']}) FROM {table_name}")
                        max_date = cursor.fetchone()[0]
                        if max_date:
                            print(f"  Found good timestamp column: {col['name']} (max date: {max_date})")
                            return col['name']
                    except:
                        continue
        
        # If no preferred names found, use the first timestamp column with data
        for col in timestamp_columns:
            try:
                cursor.execute(f"SELECT MAX({col['name']}) FROM {table_name}")
                max_date = cursor.fetchone()[0]
                if max_date:
                    print(f"  Using timestamp column: {col['name']} (max date: {max_date})")
                    return col['name']
            except:
                continue
        
        return None

    def load_watermarks(self) -> Dict:
        """Load watermarks from local file"""
        extraction_config = self.config.get('extraction', {})
        watermark_file = os.path.join(
            extraction_config.get('output_directory', 'extracted_data'),
            extraction_config.get('metadata_path', 'metadata'),
            'watermarks.json'
        )
        
        if os.path.exists(watermark_file):
            try:
                with open(watermark_file, 'r') as f:
                    self.watermarks = json.load(f)
                print(f"Loaded watermarks for {len(self.watermarks)} tables")
            except Exception as e:
                print(f"Warning: Could not load watermarks: {e}")
                self.watermarks = {}
        else:
            print("No existing watermarks found - will perform full extraction")
            self.watermarks = {}
        
        return self.watermarks

    def save_watermarks(self):
        """Save watermarks to local file"""
        extraction_config = self.config.get('extraction', {})
        metadata_dir = os.path.join(
            extraction_config.get('output_directory', 'extracted_data'),
            extraction_config.get('metadata_path', 'metadata')
        )
        os.makedirs(metadata_dir, exist_ok=True)
        
        watermark_file = os.path.join(metadata_dir, 'watermarks.json')
        
        try:
            with open(watermark_file, 'w') as f:
                json.dump(self.watermarks, f, indent=2, default=str)
            print(f"Saved watermarks for {len(self.watermarks)} tables")
        except Exception as e:
            print(f"Warning: Could not save watermarks: {e}")

    def get_incremental_data(self, table_name: str, analysis: Dict, limit: Optional[int] = None) -> Tuple[pd.DataFrame, bool]:
        """Extract incremental data from a table"""
        connection = self.get_mysql_connection()
        
        try:
            timestamp_col = analysis['best_timestamp_column']
            last_watermark = self.watermarks.get(table_name, {}).get('last_timestamp')
            
            if not timestamp_col:
                print(f"  No suitable timestamp column found - performing full extraction")
                query = f"SELECT * FROM {table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                is_incremental = False
            elif not last_watermark:
                print(f"  No previous watermark - performing full extraction")
                query = f"SELECT * FROM {table_name}"
                if limit:
                    query += f" LIMIT {limit}"
                is_incremental = False
            else:
                print(f"  Incremental extraction from {timestamp_col} > '{last_watermark}'")
                query = f"""
                    SELECT * FROM {table_name} 
                    WHERE {timestamp_col} > %s 
                    ORDER BY {timestamp_col}
                """
                if limit:
                    query += f" LIMIT {limit}"
                is_incremental = True
            
            # Execute query
            if is_incremental and last_watermark:
                df = pd.read_sql(query, connection, params=[last_watermark])
            else:
                df = pd.read_sql(query, connection)
            
            print(f"  Extracted {len(df)} rows ({'incremental' if is_incremental else 'full'})")
            
            # Update watermark if we have a timestamp column and data
            if timestamp_col and len(df) > 0 and timestamp_col in df.columns:
                # Get the maximum timestamp from this extraction
                max_timestamp = df[timestamp_col].max()
                
                # Update watermark
                if table_name not in self.watermarks:
                    self.watermarks[table_name] = {}
                
                self.watermarks[table_name].update({
                    'last_timestamp': max_timestamp,
                    'last_extraction': datetime.now().isoformat(),
                    'timestamp_column': timestamp_col,
                    'extraction_type': 'incremental' if is_incremental else 'full'
                })
                
                print(f"  Updated watermark to: {max_timestamp}")
            
            return df, is_incremental
            
        finally:
            connection.close()

    def save_to_local_bronze(self, df: pd.DataFrame, table_name: str, is_incremental: bool):
        """Save DataFrame to local bronze layer"""
        if df.empty:
            print(f"  No data to save for {table_name}")
            return None
            
        extraction_config = self.config_manager.get_extraction_config()
        
        # Create output directory
        output_dir = os.path.join(
            extraction_config['output_directory'], 
            extraction_config['bronze_layer_path'], 
            table_name
        )
        os.makedirs(output_dir, exist_ok=True)
        
        # Create filename with extraction type
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extraction_type = "incremental" if is_incremental else "full"
        filename = f"{table_name}_{extraction_type}_{timestamp}.parquet"
        filepath = os.path.join(output_dir, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"  Saved locally to: {filepath}")
        return filepath

    def upload_to_gcs(self, local_filepath: str, table_name: str, is_incremental: bool):
        """Upload parquet file to Google Cloud Storage"""
        if not self.storage_client or not self.bucket:
            print("  GCS not initialized. Skipping upload.")
            return None
            
        try:
            # Create GCS path with date partitioning
            date_str = datetime.now().strftime("%Y/%m/%d")
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            extraction_type = "incremental" if is_incremental else "full"
            
            gcs_path = f"bronze/pk_gest_xer/{table_name}/date={date_str}/{table_name}_{extraction_type}_{timestamp_str}.parquet"
            
            # Upload file
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_filepath)
            
            full_gcs_path = f"gs://{self.bucket.name}/{gcs_path}"
            print(f"  ✓ Uploaded to GCS: {full_gcs_path}")
            return full_gcs_path
            
        except Exception as e:
            print(f"  ❌ Failed to upload to GCS: {e}")
            return None

    def save_extraction_metadata(self, table_name: str, analysis: Dict, local_path: str, gcs_path: str, 
                                record_count: int, is_incremental: bool, watermark_info: Dict):
        """Save detailed extraction metadata"""
        metadata = {
            'table_name': table_name,
            'extraction_timestamp': datetime.now().isoformat(),
            'extraction_type': 'incremental' if is_incremental else 'full',
            'record_count': record_count,
            'local_path': local_path,
            'gcs_path': gcs_path,
            'table_analysis': analysis,
            'watermark_info': watermark_info,
            'status': 'success' if gcs_path else 'local_only'
        }
        
        extraction_config = self.config_manager.get_extraction_config()
        metadata_dir = os.path.join(
            extraction_config['output_directory'], 
            extraction_config['metadata_path']
        )
        os.makedirs(metadata_dir, exist_ok=True)
        
        # Save metadata
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extraction_type = "incremental" if is_incremental else "full"
        metadata_file = os.path.join(metadata_dir, f"{table_name}_{extraction_type}_{timestamp}.json")
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        print(f"  Metadata saved to: {metadata_file}")
        return metadata_file

    def extract_table_incremental(self, table_name: str, limit: Optional[int] = None):
        """Extract a single table with incremental loading"""
        print(f"\n{'='*60}")
        print(f"Processing table: {table_name}")
        print(f"{'='*60}")
        
        try:
            # 1. Analyze table structure
            print("1. Analyzing table structure...")
            analysis = self.analyze_table_structure(table_name)
            
            print(f"  - Total rows: {analysis['total_rows']:,}")
            print(f"  - Primary keys: {analysis['primary_keys']}")
            print(f"  - Timestamp columns: {[col['name'] for col in analysis['timestamp_columns']]}")
            print(f"  - Supports incremental: {analysis['supports_incremental']}")
            
            # 2. Extract data (incremental if possible)
            print("2. Extracting data...")
            df, is_incremental = self.get_incremental_data(table_name, analysis, limit)
            
            if df.empty:
                print("  No new data to process")
                return True
            
            # 3. Save locally
            print("3. Saving data locally...")
            local_path = self.save_to_local_bronze(df, table_name, is_incremental)
            
            # 4. Upload to GCS (if available)
            print("4. Uploading to GCS...")
            gcs_path = None
            if self.storage_client and self.bucket:
                gcs_path = self.upload_to_gcs(local_path, table_name, is_incremental)
            else:
                print("  GCS not available - skipping upload")
            
            # 5. Save metadata
            print("5. Saving metadata...")
            watermark_info = self.watermarks.get(table_name, {})
            self.save_extraction_metadata(table_name, analysis, local_path, gcs_path, 
                                        len(df), is_incremental, watermark_info)
            
            print(f"✅ Successfully processed {table_name}: {len(df)} records ({('incremental' if is_incremental else 'full')} extraction)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to process {table_name}: {e}")
            return False

def main():
    """Test incremental extraction"""
    print("Incremental MySQL to GCS Data Extractor")
    print("=" * 60)
    
    # Initialize extractor
    extractor = IncrementalExtractor()
    
    # Load existing watermarks
    extractor.load_watermarks()
    
    # Initialize GCS (optional)
    gcs_available = extractor.initialize_gcs()
    if not gcs_available:
        print("⚠️  GCS not available. Will save locally only.")
    
    # Get test table
    try:
        connection = extractor.get_mysql_connection()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        test_table = tables[0][0]  # Use Pie_Fac
        connection.close()
        
        print(f"\nTesting incremental extraction with table: {test_table}")
        
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    # Get test limit from config
    extraction_config = extractor.config_manager.get_extraction_config()
    test_limit = extraction_config.get('test_limit', 100)
    
    # Extract table
    success = extractor.extract_table_incremental(test_table, limit=test_limit)
    
    # Save watermarks
    extractor.save_watermarks()
    
    if success:
        print(f"\n🎉 Incremental extraction completed successfully!")
        print("\nNext run will only extract records newer than the current watermark.")
        
        # Show current watermark
        if test_table in extractor.watermarks:
            watermark = extractor.watermarks[test_table]
            print(f"\nCurrent watermark for {test_table}:")
            print(f"  - Last timestamp: {watermark.get('last_timestamp')}")
            print(f"  - Timestamp column: {watermark.get('timestamp_column')}")
            print(f"  - Last extraction: {watermark.get('last_extraction')}")
    else:
        print(f"\n❌ Incremental extraction failed!")

if __name__ == "__main__":
    main()
