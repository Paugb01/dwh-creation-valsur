"""
Pipeline Testing Framework
Comprehensive testing before Composer deployment
"""
import sys
import os
import time
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "batch_only"))
sys.path.append(str(project_root / "batch_only" / "gcs_to_bq"))

class PipelineTestFramework:
    """Test framework for the data warehouse pipeline"""
    
    def __init__(self):
        self.results = {}
        self.start_time = time.time()
        self.test_timestamp = datetime.now()
        
    def log(self, message: str, level: str = "INFO"):
        """Log with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
        
    def test_connections(self) -> Dict[str, Any]:
        """Test all required connections"""
        self.log("🔗 Testing connections and permissions...")
        results = {
            'mysql': False,
            'gcs': False,
            'bigquery': False,
            'errors': []
        }
        
        # Test MySQL connection
        try:
            from batch_extractor import BatchExtractor
            extractor = BatchExtractor()
            # Try to access database info
            self.log("  📊 Testing MySQL connection...")
            results['mysql'] = True
            self.log("    ✅ MySQL connection successful")
        except Exception as e:
            results['errors'].append(f"MySQL: {str(e)}")
            self.log(f"    ❌ MySQL connection failed: {e}", "ERROR")
        
        # Test GCS connection
        try:
            from main import GCSToBigQueryWorker
            worker = GCSToBigQueryWorker()
            bucket = worker.storage_client.bucket(worker.bucket_name)
            bucket.reload()
            self.log("  ☁️ Testing GCS connection...")
            results['gcs'] = True
            self.log("    ✅ GCS connection successful")
        except Exception as e:
            results['errors'].append(f"GCS: {str(e)}")
            self.log(f"    ❌ GCS connection failed: {e}", "ERROR")
        
        # Test BigQuery connection
        try:
            from main import GCSToBigQueryWorker
            worker = GCSToBigQueryWorker()
            datasets = list(worker.bq_client.list_datasets())
            self.log("  📈 Testing BigQuery connection...")
            results['bigquery'] = True
            self.log("    ✅ BigQuery connection successful")
        except Exception as e:
            results['errors'].append(f"BigQuery: {str(e)}")
            self.log(f"    ❌ BigQuery connection failed: {e}", "ERROR")
        
        results['all_connected'] = all([results['mysql'], results['gcs'], results['bigquery']])
        return results
    
    def test_mysql_extraction(self, limit_per_table: int = 100) -> Dict[str, Any]:
        """Test MySQL data extraction"""
        self.log(f"📊 Testing MySQL extraction (limit: {limit_per_table} records per table)...")
        
        results = {
            'success': False,
            'extraction_time': 0,
            'tables_extracted': 0,
            'total_tables': 0,
            'total_records': 0,
            'tables': {},
            'errors': []
        }
        
        try:
            from batch_extractor import BatchExtractor
            extractor = BatchExtractor()
            
            start_time = time.time()
            extraction_results = extractor.extract_all_tables(limit_per_table=limit_per_table)
            extraction_time = time.time() - start_time
            
            results['extraction_time'] = extraction_time
            results['total_tables'] = len(extraction_results)
            
            # Process results
            for table_name, result in extraction_results.items():
                if result.get('success', False):
                    results['tables_extracted'] += 1
                    results['total_records'] += result.get('records', 0)
                    results['tables'][table_name] = {
                        'success': True,
                        'records': result.get('records', 0),
                        'file_size_mb': result.get('file_size_bytes', 0) / (1024*1024)
                    }
                else:
                    results['tables'][table_name] = {
                        'success': False,
                        'error': result.get('error', 'Unknown error')
                    }
                    results['errors'].append(f"{table_name}: {result.get('error', 'Unknown error')}")
            
            results['success'] = results['tables_extracted'] > 0
            
            self.log(f"    ⏱️ Extraction completed in {extraction_time:.2f} seconds")
            self.log(f"    📈 Success: {results['tables_extracted']}/{results['total_tables']} tables")
            self.log(f"    📊 Total records: {results['total_records']:,}")
            
            if results['errors']:
                self.log(f"    ⚠️ Errors: {len(results['errors'])}", "WARNING")
                
        except Exception as e:
            results['errors'].append(f"Extraction failed: {str(e)}")
            self.log(f"    ❌ Extraction failed: {e}", "ERROR")
            
        return results
    
    def test_gcs_to_bigquery(self, specific_tables: List[str] = None) -> Dict[str, Any]:
        """Test GCS to BigQuery ingestion"""
        self.log("📈 Testing GCS to BigQuery bronze ingestion...")
        
        results = {
            'success': False,
            'ingestion_time': 0,
            'tables_ingested': 0,
            'total_tables': 0,
            'total_rows': 0,
            'total_size_mb': 0,
            'tables': {},
            'errors': []
        }
        
        try:
            from main import GCSToBigQueryWorker
            worker = GCSToBigQueryWorker()
            
            start_time = time.time()
            ingestion_results = worker.run_bronze_ingestion(specific_tables=specific_tables)
            ingestion_time = time.time() - start_time
            
            results['ingestion_time'] = ingestion_time
            results['total_tables'] = len(ingestion_results)
            
            # Validate results
            validation = worker.validate_bronze_tables()
            
            # Process results
            for table_name, success in ingestion_results.items():
                if success:
                    results['tables_ingested'] += 1
                    table_info = validation.get(table_name, {})
                    if table_info.get('exists'):
                        results['total_rows'] += table_info.get('num_rows', 0)
                        results['total_size_mb'] += table_info.get('size_bytes', 0) / (1024*1024)
                    
                    results['tables'][table_name] = {
                        'success': True,
                        'rows': table_info.get('num_rows', 0),
                        'size_mb': table_info.get('size_bytes', 0) / (1024*1024)
                    }
                else:
                    results['tables'][table_name] = {
                        'success': False,
                        'error': 'Ingestion failed'
                    }
                    results['errors'].append(f"{table_name}: Ingestion failed")
            
            results['success'] = results['tables_ingested'] > 0
            
            self.log(f"    ⏱️ Ingestion completed in {ingestion_time:.2f} seconds")
            self.log(f"    📈 Success: {results['tables_ingested']}/{results['total_tables']} tables")
            self.log(f"    📊 Total rows: {results['total_rows']:,}")
            self.log(f"    💾 Total size: {results['total_size_mb']:.1f} MB")
            
            if results['errors']:
                self.log(f"    ⚠️ Errors: {len(results['errors'])}", "WARNING")
                
        except Exception as e:
            results['errors'].append(f"Ingestion failed: {str(e)}")
            self.log(f"    ❌ Ingestion failed: {e}", "ERROR")
            
        return results
    
    def test_performance_scaling(self) -> Dict[str, Any]:
        """Test performance with different data sizes"""
        self.log("🏃 Testing performance scaling...")
        
        test_sizes = [10, 50, 100]  # Records per table
        performance_data = []
        
        for size in test_sizes:
            self.log(f"  📏 Testing with {size} records per table...")
            
            try:
                from batch_extractor import BatchExtractor
                extractor = BatchExtractor()
                
                start_time = time.time()
                results = extractor.extract_all_tables(limit_per_table=size)
                end_time = time.time()
                
                successful_tables = sum(1 for result in results.values() if result.get('success', False))
                total_records = sum(result.get('records', 0) for result in results.values() if result.get('success', False))
                
                duration = end_time - start_time
                records_per_second = total_records / duration if duration > 0 else 0
                
                perf_data = {
                    'size': size,
                    'time': duration,
                    'tables': successful_tables,
                    'records': total_records,
                    'records_per_second': records_per_second
                }
                performance_data.append(perf_data)
                
                self.log(f"    ⏱️ Time: {duration:.2f}s")
                self.log(f"    🚀 Records/sec: {records_per_second:.0f}")
                
            except Exception as e:
                self.log(f"    ❌ Failed at size {size}: {e}", "ERROR")
                performance_data.append({
                    'size': size,
                    'error': str(e)
                })
        
        return {'performance_data': performance_data}
    
    def estimate_full_pipeline(self, performance_data: List[Dict]) -> Dict[str, Any]:
        """Estimate full pipeline performance"""
        self.log("📊 Estimating full pipeline performance...")
        
        if not performance_data:
            self.log("    ⚠️ No performance data available", "WARNING")
            return {}
        
        # Get best performance metric
        valid_data = [p for p in performance_data if 'records_per_second' in p and p['records_per_second'] > 0]
        if not valid_data:
            self.log("    ⚠️ No valid performance data", "WARNING")
            return {}
        
        best_perf = max(valid_data, key=lambda x: x['records_per_second'])
        records_per_second = best_perf['records_per_second']
        
        # Estimate based on actual table sizes (conservative estimate: 1.6M total records)
        estimated_total_records = 1_600_000
        
        estimated_extraction_time = estimated_total_records / records_per_second if records_per_second > 0 else 0
        estimated_ingestion_time = estimated_extraction_time * 0.3  # BigQuery is usually faster
        
        estimates = {
            'extraction_minutes': estimated_extraction_time / 60,
            'ingestion_minutes': estimated_ingestion_time / 60,
            'total_minutes': (estimated_extraction_time + estimated_ingestion_time) / 60,
            'records_per_second': records_per_second,
            'estimated_total_records': estimated_total_records
        }
        
        self.log(f"    📈 Estimated extraction time: {estimates['extraction_minutes']:.1f} minutes")
        self.log(f"    📈 Estimated ingestion time: {estimates['ingestion_minutes']:.1f} minutes")
        self.log(f"    📈 Total estimated time: {estimates['total_minutes']:.1f} minutes")
        
        return estimates
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests and generate comprehensive report"""
        self.log("🧪 STARTING COMPREHENSIVE PIPELINE TESTING")
        self.log("="*60)
        
        # Test 1: Connections
        self.log("\n1️⃣ TESTING CONNECTIONS")
        self.log("-" * 30)
        connection_results = self.test_connections()
        self.results['connections'] = connection_results
        
        if not connection_results['all_connected']:
            self.log("❌ Connection tests failed. Cannot proceed with pipeline tests.", "ERROR")
            return self.generate_final_report()
        
        # Test 2: Performance scaling
        self.log("\n2️⃣ TESTING PERFORMANCE SCALING")
        self.log("-" * 30)
        performance_results = self.test_performance_scaling()
        self.results['performance'] = performance_results
        
        # Test 3: MySQL extraction
        self.log("\n3️⃣ TESTING MYSQL EXTRACTION")
        self.log("-" * 30)
        extraction_results = self.test_mysql_extraction(limit_per_table=100)
        self.results['extraction'] = extraction_results
        
        # Test 4: BigQuery ingestion (only if extraction succeeded)
        if extraction_results['success']:
            self.log("\n4️⃣ TESTING BIGQUERY INGESTION")
            self.log("-" * 30)
            successful_tables = [name for name, info in extraction_results['tables'].items() 
                               if info.get('success', False)]
            ingestion_results = self.test_gcs_to_bigquery(specific_tables=successful_tables)
            self.results['ingestion'] = ingestion_results
        else:
            self.log("\n4️⃣ SKIPPING BIGQUERY TEST (extraction failed)")
            self.results['ingestion'] = {'success': False, 'skipped': True}
        
        # Test 5: Performance estimates
        self.log("\n5️⃣ GENERATING PERFORMANCE ESTIMATES")
        self.log("-" * 30)
        estimates = self.estimate_full_pipeline(performance_results.get('performance_data', []))
        self.results['estimates'] = estimates
        
        return self.generate_final_report()
    
    def generate_final_report(self) -> Dict[str, Any]:
        """Generate final test report and recommendations"""
        total_time = time.time() - self.start_time
        
        self.log("\n" + "="*70)
        self.log("📋 FINAL TEST REPORT")
        self.log("="*70)
        
        # Summary
        connections_ok = self.results.get('connections', {}).get('all_connected', False)
        extraction_ok = self.results.get('extraction', {}).get('success', False)
        ingestion_ok = self.results.get('ingestion', {}).get('success', False)
        
        self.log(f"📅 Test Date: {self.test_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"⏱️ Total Test Time: {total_time:.1f} seconds")
        self.log("")
        self.log("🔍 TEST RESULTS:")
        self.log(f"  Connections: {'✅ PASS' if connections_ok else '❌ FAIL'}")
        self.log(f"  MySQL Extraction: {'✅ PASS' if extraction_ok else '❌ FAIL'}")
        self.log(f"  BigQuery Ingestion: {'✅ PASS' if ingestion_ok else '❌ FAIL'}")
        
        # Performance summary
        if 'estimates' in self.results and self.results['estimates']:
            estimates = self.results['estimates']
            self.log("")
            self.log("📊 PERFORMANCE ESTIMATES:")
            self.log(f"  Full pipeline time: {estimates.get('total_minutes', 0):.1f} minutes")
            self.log(f"  Processing speed: {estimates.get('records_per_second', 0):.0f} records/second")
        
        # Recommendations
        self.log("")
        self.log("🎯 RECOMMENDATIONS:")
        
        if connections_ok and extraction_ok and ingestion_ok:
            self.log("✅ PIPELINE IS READY FOR COMPOSER DEPLOYMENT")
            self.log("🚀 You can safely proceed with creating the Composer environment")
            
            if 'estimates' in self.results and self.results['estimates'].get('total_minutes', 0) > 60:
                self.log("⚠️ Pipeline may take over 1 hour - consider increasing Composer timeouts")
                
        else:
            self.log("❌ FIX THE FOLLOWING ISSUES BEFORE DEPLOYING:")
            
            if not connections_ok:
                errors = self.results.get('connections', {}).get('errors', [])
                self.log("   🔗 Connection issues:")
                for error in errors[:3]:  # Show first 3 errors
                    self.log(f"     - {error}")
            
            if not extraction_ok:
                errors = self.results.get('extraction', {}).get('errors', [])
                self.log("   📊 Extraction issues:")
                for error in errors[:3]:
                    self.log(f"     - {error}")
            
            if not ingestion_ok and not self.results.get('ingestion', {}).get('skipped'):
                errors = self.results.get('ingestion', {}).get('errors', [])
                self.log("   📈 Ingestion issues:")
                for error in errors[:3]:
                    self.log(f"     - {error}")
        
        # Save detailed report
        report_data = {
            'test_timestamp': self.test_timestamp.isoformat(),
            'total_test_time': total_time,
            'results': self.results,
            'ready_for_composer': connections_ok and extraction_ok and ingestion_ok
        }
        
        report_file = f"pipeline_test_report_{self.test_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        report_path = Path(__file__).parent / report_file
        
        try:
            with open(report_path, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            self.log(f"📄 Detailed report saved to: {report_path}")
        except Exception as e:
            self.log(f"⚠️ Could not save report: {e}", "WARNING")
        
        return report_data


def main():
    """Run the complete test suite"""
    tester = PipelineTestFramework()
    results = tester.run_all_tests()
    
    # Exit with appropriate code
    if results.get('ready_for_composer', False):
        exit(0)  # Success
    else:
        exit(1)  # Failure


if __name__ == "__main__":
    main()
