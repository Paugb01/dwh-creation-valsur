"""
Table Discovery Script
Analyzes all tables in the database to find candidates for incremental loading
"""
import pymysql
from config_manager import config_manager

def discover_tables():
    """Discover all tables and analyze their structures"""
    print("Table Discovery and Analysis")
    print("=" * 60)
    
    try:
        # Get connection
        db_config = config_manager.get_database_config()
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print(f"Found {len(tables)} tables in database")
        print("\nAnalyzing tables for timestamp columns...\n")
        
        tables_with_timestamps = []
        tables_without_timestamps = []
        
        for i, (table_name,) in enumerate(tables[:20], 1):  # Analyze first 20 tables
            print(f"{i:2d}. {table_name:<30}", end=" ")
            
            try:
                # Get table structure
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                # Find timestamp columns
                timestamp_cols = []
                primary_keys = []
                
                for col in columns:
                    col_name, col_type, null, key, default, extra = col
                    if key == 'PRI':
                        primary_keys.append(col_name)
                    
                    if any(time_type in col_type.lower() for time_type in ['timestamp', 'datetime', 'date']):
                        timestamp_cols.append(f"{col_name}({col_type})")
                
                # Check for common timestamp column names even if not detected by type
                common_timestamp_names = [
                    'created_at', 'updated_at', 'modified_at', 'last_modified',
                    'created_date', 'updated_date', 'fecha_modificacion', 'fecha_creacion',
                    'timestamp', 'last_update', 'date_created', 'date_modified'
                ]
                
                for col in columns:
                    col_name = col[0].lower()
                    if any(name in col_name for name in common_timestamp_names):
                        if f"{col[0]}({col[1]})" not in timestamp_cols:
                            timestamp_cols.append(f"{col[0]}({col[1]})*")  # * means found by name
                
                if timestamp_cols:
                    tables_with_timestamps.append({
                        'name': table_name,
                        'rows': row_count,
                        'timestamp_cols': timestamp_cols,
                        'primary_keys': primary_keys
                    })
                    print(f"[{row_count:>8,} rows] ✓ {', '.join(timestamp_cols)}")
                else:
                    tables_without_timestamps.append({
                        'name': table_name,
                        'rows': row_count,
                        'primary_keys': primary_keys
                    })
                    print(f"[{row_count:>8,} rows] ✗ No timestamp columns")
                    
            except Exception as e:
                print(f"[ERROR] {e}")
        
        connection.close()
        
        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Tables with timestamp columns: {len(tables_with_timestamps)}")
        print(f"Tables without timestamp columns: {len(tables_without_timestamps)}")
        
        if tables_with_timestamps:
            print(f"\n📊 GOOD CANDIDATES FOR INCREMENTAL LOADING:")
            print("-" * 60)
            for table in sorted(tables_with_timestamps, key=lambda x: x['rows'], reverse=True):
                print(f"{table['name']:<30} {table['rows']:>8,} rows  {', '.join(table['timestamp_cols'])}")
        
        if tables_without_timestamps:
            print(f"\n📋 TABLES REQUIRING FULL EXTRACTION:")
            print("-" * 60)
            for table in sorted(tables_without_timestamps, key=lambda x: x['rows'], reverse=True)[:10]:
                print(f"{table['name']:<30} {table['rows']:>8,} rows")
            
            if len(tables_without_timestamps) > 10:
                print(f"... and {len(tables_without_timestamps) - 10} more tables")
        
        return tables_with_timestamps, tables_without_timestamps
        
    except Exception as e:
        print(f"❌ Failed to discover tables: {e}")
        return [], []

if __name__ == "__main__":
    discover_tables()
