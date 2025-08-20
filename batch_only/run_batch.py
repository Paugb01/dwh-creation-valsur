"""
Example script for running batch extraction with GCS upload
Demonstrates how to use the BatchExtractor for data extraction and cloud upload
"""
from batch_extractor import BatchExtractor

def main():
    """Main function to run batch extraction with GCS upload"""
    print("🚀 Starting Batch Data Extraction with GCS Upload")
    print("=" * 60)
    
    # Initialize the extractor (will load config.json and secrets.json)
    try:
        extractor = BatchExtractor(
            config_path="config/config.json",  # Fixed: was "config.json"
            secrets_path="config/secrets.json"  # Fixed: was "secrets.json"
        ) 
        print("✅ Extractor initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize extractor: {e}")
        print("📝 Make sure to create secrets.json from secrets.json.template")
        return
    
    # Option 1: Extract all tables (limited to 1000 rows per table for demo)
    print(f"\n📊 Extracting all tables...")
    print("   - Saving locally to bronze layer structure")
    print("   - Uploading to GCS with date partitioning")
    print("   - Generating metadata files")
    
    results = extractor.extract_all_tables(limit_per_table=None)
    
    # Summary report
    successful_tables = [name for name, result in results.items() if result.get('success', False)]
    failed_tables = [name for name, result in results.items() if not result.get('success', False)]
    total_records = sum(result.get('records', 0) for result in results.values() if result.get('success', False))
    
    print(f"\n📈 EXTRACTION SUMMARY")
    print(f"{'='*50}")
    print(f"✅ Successful tables: {len(successful_tables)}")
    print(f"❌ Failed tables: {len(failed_tables)}")
    print(f"📊 Total records extracted: {total_records:,}")
    
    if successful_tables:
        print(f"\n✅ Successfully processed tables:")
        for table_name in successful_tables[:10]:  # Show first 10
            result = results[table_name]
            gcs_status = "✓ GCS" if result.get('gcs_path') else "⚠ Local only"
            print(f"  • {table_name:<30} {result.get('records', 0):>6,} rows  {gcs_status}")
        
        if len(successful_tables) > 10:
            print(f"  ... and {len(successful_tables) - 10} more tables")
    
    if failed_tables:
        print(f"\n❌ Failed tables:")
        for table_name in failed_tables[:5]:  # Show first 5 failures
            error = results[table_name].get('error', 'Unknown error')
            print(f"  • {table_name:<30} Error: {error}")
    
    # Option 2: Extract a specific table (uncomment to use)
    # print(f"\n📋 Extracting specific table...")
    # result = extractor.extract_table_with_upload("your_table_name", limit=5000)
    # if result.get('success'):
    #     print(f"✅ Single table extraction completed: {result['records']} records")
    #     if result.get('gcs_path'):
    #         print(f"   GCS Path: {result['gcs_path']}")
    # else:
    #     print(f"❌ Single table extraction failed: {result.get('error')}")

    print(f"\n🎉 Batch extraction process completed!")

if __name__ == "__main__":
    main()
