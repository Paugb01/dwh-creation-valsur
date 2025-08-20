"""
Daily Data Pipeline Scheduler
Automates daily incremental data extraction with proper scheduling and monitoring
"""
import schedule
import time
import logging
import os
from datetime import datetime
from production_batch_extractor import ProductionBatchExtractor
from config_manager import config_manager

class DailyPipeline:
    def __init__(self):
        self.setup_logging()
        self.extractor = ProductionBatchExtractor(max_workers=3)
        
    def setup_logging(self):
        """Setup logging for the pipeline"""
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Create a unique log file for each day
        log_filename = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def run_daily_extraction(self):
        """Run the daily data extraction pipeline"""
        start_time = datetime.now()
        self.logger.info("="*80)
        self.logger.info(f"Starting daily data pipeline at {start_time}")
        self.logger.info("="*80)
        
        try:
            # Load existing watermarks
            self.extractor.load_watermarks()
            self.logger.info(f"Loaded watermarks for {len(self.extractor.watermarks)} tables")
            
            # Initialize GCS
            gcs_available = self.extractor.initialize_gcs()
            if not gcs_available:
                self.logger.warning("GCS not available. Will save locally only.")
            
            # Get all tables
            all_tables = self.extractor.get_all_tables()
            self.logger.info(f"Found {len(all_tables)} tables in database")
            
            # Categorize tables
            incremental_tables, full_extraction_tables = self.extractor.categorize_tables(all_tables)
            
            # Create extraction plan
            plan = self.extractor.create_extraction_plan(incremental_tables, full_extraction_tables)
            
            # Log extraction plan
            self.logger.info(f"Extraction plan: {len(plan['incremental_extraction'])} incremental, "
                           f"{len(plan['full_extraction_small'])} small full, "
                           f"{len(plan['full_extraction_large'])} large limited")
            
            # Run extraction
            results = self.extractor.run_batch_extraction(plan, dry_run=False)
            
            # Log summary
            successful = sum(1 for r in results if r['success'])
            failed = sum(1 for r in results if not r['success'])
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info("="*80)
            self.logger.info(f"Daily pipeline completed in {duration}")
            self.logger.info(f"Successful: {successful}, Failed: {failed}")
            self.logger.info("="*80)
            
            # Log any failures
            failed_tables = [r['table'] for r in results if not r['success']]
            if failed_tables:
                self.logger.error(f"Failed tables: {', '.join(failed_tables)}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            raise
    
    def run_test_extraction(self):
        """Run a test extraction with limited tables"""
        self.logger.info("Running test extraction (first 20 tables)")
        
        try:
            # Load existing watermarks
            self.extractor.load_watermarks()
            
            # Initialize GCS
            gcs_available = self.extractor.initialize_gcs()
            if not gcs_available:
                self.logger.warning("GCS not available. Will save locally only.")
            
            # Get first 20 tables for testing
            all_tables = self.extractor.get_all_tables()[:20]
            self.logger.info(f"Testing with {len(all_tables)} tables")
            
            # Categorize and extract
            incremental_tables, full_extraction_tables = self.extractor.categorize_tables(all_tables)
            plan = self.extractor.create_extraction_plan(incremental_tables, full_extraction_tables)
            results = self.extractor.run_batch_extraction(plan, dry_run=False)
            
            successful = sum(1 for r in results if r['success'])
            self.logger.info(f"Test extraction completed: {successful}/{len(results)} successful")
            
        except Exception as e:
            self.logger.error(f"Test extraction failed: {e}", exc_info=True)
    
    def schedule_daily_runs(self):
        """Schedule daily pipeline runs"""
        # Schedule daily run at 2:00 AM
        schedule.every().day.at("02:00").do(self.run_daily_extraction)
        
        # Schedule a test run every hour during business hours for monitoring
        schedule.every().hour.at(":00").do(self.run_test_extraction).tag('test')
        
        self.logger.info("Pipeline scheduled:")
        self.logger.info("- Daily full extraction: 02:00")
        self.logger.info("- Hourly test extraction: Every hour")
        
        # Run scheduler
        self.logger.info("Starting scheduler...")
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

def run_manual():
    """Run pipeline manually (for testing)"""
    pipeline = DailyPipeline()
    pipeline.run_daily_extraction()

def run_test():
    """Run test extraction manually"""
    pipeline = DailyPipeline()
    pipeline.run_test_extraction()

def run_scheduler():
    """Run the scheduler"""
    pipeline = DailyPipeline()
    pipeline.schedule_daily_runs()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            run_test()
        elif sys.argv[1] == "schedule":
            run_scheduler()
        elif sys.argv[1] == "manual":
            run_manual()
        else:
            print("Usage: python daily_pipeline.py [test|schedule|manual]")
    else:
        print("Daily Data Pipeline")
        print("Usage:")
        print("  python daily_pipeline.py test      - Run test extraction")
        print("  python daily_pipeline.py manual    - Run full extraction once")
        print("  python daily_pipeline.py schedule  - Start scheduler")
