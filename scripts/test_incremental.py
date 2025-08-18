"""
Test Incremental Loading with accesos_presencia table
"""
from incremental_extractor import IncrementalExtractor

def test_incremental_loading():
    """Test incremental loading with a table that has timestamp columns"""
    print("Testing Incremental Loading")
    print("=" * 60)
    
    # Initialize extractor
    extractor = IncrementalExtractor()
    
    # Load existing watermarks
    extractor.load_watermarks()
    
    # Initialize GCS
    gcs_available = extractor.initialize_gcs()
    if not gcs_available:
        print("⚠️  GCS not available. Will save locally only.")
    
    # Test with accesos_presencia table (has timestamp columns)
    table_name = "accesos_presencia"
    
    print(f"\n🧪 Testing incremental extraction with: {table_name}")
    
    # First extraction (should be full)
    print("\n--- FIRST EXTRACTION (should be full) ---")
    success1 = extractor.extract_table_incremental(table_name, limit=50)
    
    if success1:
        # Save watermarks after first extraction
        extractor.save_watermarks()
        
        # Second extraction (should be incremental with no new data)
        print("\n--- SECOND EXTRACTION (should be incremental) ---")
        success2 = extractor.extract_table_incremental(table_name, limit=50)
        
        if success2:
            extractor.save_watermarks()
            print("\n🎉 Incremental loading test completed!")
            
            # Show watermark status
            if table_name in extractor.watermarks:
                watermark = extractor.watermarks[table_name]
                print(f"\nFinal watermark for {table_name}:")
                print(f"  - Timestamp column: {watermark.get('timestamp_column')}")
                print(f"  - Last timestamp: {watermark.get('last_timestamp')}")
                print(f"  - Last extraction: {watermark.get('last_extraction')}")
                print(f"  - Extraction type: {watermark.get('extraction_type')}")
        else:
            print("❌ Second extraction failed")
    else:
        print("❌ First extraction failed")

if __name__ == "__main__":
    test_incremental_loading()
