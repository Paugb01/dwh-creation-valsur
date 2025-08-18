"""
Test Production Batch Extractor with Limited Scope
Tests with first 10 tables to verify functionality
"""
from production_batch_extractor import ProductionBatchExtractor

def test_limited_batch():
    """Test batch extractor with limited scope"""
    print("Testing Production Batch Extractor (Limited)")
    print("=" * 60)
    
    # Initialize extractor
    extractor = ProductionBatchExtractor(max_workers=2)
    
    # Load existing watermarks
    extractor.load_watermarks()
    
    # Initialize GCS
    gcs_available = extractor.initialize_gcs()
    if not gcs_available:
        print("⚠️  GCS not available. Will save locally only.")
    
    # Get first 10 tables for testing
    all_tables = extractor.get_all_tables()[:10]
    print(f"Testing with first {len(all_tables)} tables: {', '.join(all_tables)}")
    
    # Categorize tables
    incremental_tables, full_extraction_tables = extractor.categorize_tables(all_tables)
    
    # Create extraction plan
    plan = extractor.create_extraction_plan(incremental_tables, full_extraction_tables, max_full_table_size=50000)
    
    # Show plan
    extractor.print_extraction_plan(plan)
    
    # Run with dry run first
    print(f"\n🔍 DRY RUN - Testing extraction plan")
    extractor.run_batch_extraction(plan, dry_run=True)
    
    # Ask for actual run
    response = input(f"\nRun actual extraction on these {len(all_tables)} tables? (y/N): ").strip().lower()
    
    if response == 'y':
        results = extractor.run_batch_extraction(plan, dry_run=False)
        return results
    else:
        print("Test cancelled.")
        return None

if __name__ == "__main__":
    test_limited_batch()
