#!/usr/bin/env python3
"""
Incremental Loading Documentation Generator
Analyzes database tables and columns to determine the best incremental loading strategy
Focus: Identify timestamp columns, primary keys, and loading patterns
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pymysql
from pathlib import Path
from datetime import datetime
import json
import logging
import re
from extractors.base_extractor import BaseExtractor

class IncrementalLoadingDocumenter(BaseExtractor):
    """Analyze tables for incremental loading patterns"""
    
    def __init__(self):
        super().__init__()
        self.tables_analysis = []
        self.incremental_candidates = []
        self.full_load_only = []
        self.output_dir = "docs/database_documentation"
        self.timestamp_patterns = [
            'created_at', 'updated_at', 'modified_at', 'last_modified', 'last_update',
            'created_date', 'updated_date', 'modification_date', 'modify_date',
            'fecha_modificacion', 'fecha_creacion', 'fecha_actualizacion',
            'timestamp', 'date_created', 'date_modified', 'date_updated',
            'create_time', 'update_time', 'mod_time', 'change_date',
            'insert_date', 'insert_time', 'changed_on', 'modified_on'
        ]
        
    def discover_all_tables(self):
        """Discover all tables in the database"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                self.logger.info(f"Found {len(tables)} tables in database")
                return tables
        finally:
            connection.close()
    
    def analyze_table_for_incremental_loading(self, table_name: str) -> dict:
        """Analyze a table to determine incremental loading strategy"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                # Get table basic info
                cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
                status = cursor.fetchone()
                
                # Get estimated row count
                estimated_rows = status[4] if status and status[4] is not None else 0
                
                # Get exact count for smaller tables only
                if estimated_rows < 50000:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        exact_rows = cursor.fetchone()[0]
                    except:
                        exact_rows = estimated_rows
                else:
                    exact_rows = estimated_rows
                
                # Get column information with detailed analysis
                cursor.execute(f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        COLUMN_TYPE,
                        COLUMN_KEY,
                        EXTRA,
                        COLUMN_COMMENT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                
                columns = cursor.fetchall()
                
                # Analyze columns for incremental loading patterns
                analysis = self._analyze_columns_for_incremental(table_name, columns)
                
                # Add table metadata
                analysis.update({
                    'table_name': table_name,
                    'engine': status[1] if status and status[1] else 'Unknown',
                    'row_count': exact_rows,
                    'estimated_rows': estimated_rows,
                    'data_size_mb': round((status[6] or 0) / 1024 / 1024, 2) if status else 0,
                    'create_time': status[11] if status and status[11] is not None else None,
                    'update_time': status[12] if status and status[12] is not None else None,
                    'table_comment': status[17] if status and status[17] is not None else '',
                    'total_columns': len(columns)
                })
                
                return analysis
                
        except Exception as e:
            self.logger.error(f"Error analyzing table {table_name}: {e}")
            return {
                'table_name': table_name,
                'loading_strategy': 'ERROR',
                'confidence': 'LOW',
                'reason': f'Analysis failed: {str(e)}',
                'row_count': 0,
                'timestamp_columns': [],
                'primary_keys': [],
                'auto_increment_columns': []
            }
        finally:
            connection.close()
    
    def _analyze_columns_for_incremental(self, table_name: str, columns: list) -> dict:
        """Analyze columns to determine incremental loading strategy"""
        
        timestamp_columns = []
        primary_keys = []
        auto_increment_columns = []
        unique_columns = []
        indexed_columns = []
        
        # Analyze each column
        for col in columns:
            col_name, data_type, is_nullable, default, col_type, col_key, extra, comment = col
            
            # Check for primary keys
            if col_key == 'PRI':
                primary_keys.append({
                    'column': col_name,
                    'type': col_type,
                    'auto_increment': 'auto_increment' in extra.lower()
                })
            
            # Check for auto increment
            if 'auto_increment' in extra.lower():
                auto_increment_columns.append(col_name)
            
            # Check for unique keys
            if col_key == 'UNI':
                unique_columns.append(col_name)
            
            # Check for indexed columns
            if col_key in ['PRI', 'UNI', 'MUL']:
                indexed_columns.append(col_name)
            
            # Check for timestamp columns (by type)
            if data_type.lower() in ['timestamp', 'datetime', 'date']:
                timestamp_columns.append({
                    'column': col_name,
                    'type': col_type,
                    'nullable': is_nullable == 'YES',
                    'default': default,
                    'detection_method': 'type_based'
                })
            
            # Check for timestamp columns (by name pattern)
            col_lower = col_name.lower()
            for pattern in self.timestamp_patterns:
                if pattern in col_lower:
                    # Avoid duplicates
                    if not any(tc['column'] == col_name for tc in timestamp_columns):
                        timestamp_columns.append({
                            'column': col_name,
                            'type': col_type,
                            'nullable': is_nullable == 'YES',
                            'default': default,
                            'detection_method': 'name_pattern',
                            'matched_pattern': pattern
                        })
                    break
        
        # Determine loading strategy
        strategy_analysis = self._determine_loading_strategy(
            table_name, timestamp_columns, primary_keys, auto_increment_columns
        )
        
        return {
            'timestamp_columns': timestamp_columns,
            'primary_keys': primary_keys,
            'auto_increment_columns': auto_increment_columns,
            'unique_columns': unique_columns,
            'indexed_columns': indexed_columns,
            **strategy_analysis
        }
    
    def _determine_loading_strategy(self, table_name: str, timestamp_cols: list, 
                                  primary_keys: list, auto_increment_cols: list) -> dict:
        """Determine the best loading strategy for a table"""
        
        # Score different strategies
        incremental_score = 0
        reasons = []
        recommended_watermark = None
        loading_strategy = "FULL_REPLACE"
        confidence = "LOW"
        
        # Check for timestamp columns
        if timestamp_cols:
            incremental_score += 50
            reasons.append(f"Found {len(timestamp_cols)} timestamp column(s)")
            
            # Find best watermark column
            for ts_col in timestamp_cols:
                if any(keyword in ts_col['column'].lower() 
                      for keyword in ['updated', 'modified', 'last_modified', 'update_time']):
                    recommended_watermark = ts_col['column']
                    incremental_score += 30
                    reasons.append(f"'{ts_col['column']}' indicates modification tracking")
                    break
            
            # If no modification column, use creation column
            if not recommended_watermark:
                for ts_col in timestamp_cols:
                    if any(keyword in ts_col['column'].lower() 
                          for keyword in ['created', 'insert', 'create_time']):
                        recommended_watermark = ts_col['column']
                        incremental_score += 20
                        reasons.append(f"'{ts_col['column']}' can track new records")
                        break
            
            # If still no watermark, use first timestamp column
            if not recommended_watermark and timestamp_cols:
                recommended_watermark = timestamp_cols[0]['column']
                incremental_score += 10
                reasons.append(f"Using '{recommended_watermark}' as fallback watermark")
        
        # Check for auto-increment primary key
        if auto_increment_cols:
            incremental_score += 25
            reasons.append("Auto-increment column available for incremental loading")
        
        # Check primary key structure
        if len(primary_keys) == 1:
            incremental_score += 15
            reasons.append("Single primary key simplifies incremental logic")
        elif len(primary_keys) > 1:
            incremental_score += 5
            reasons.append("Composite primary key (manageable for incremental)")
        else:
            incremental_score -= 20
            reasons.append("No primary key found (complicates incremental loading)")
        
        # Special table type analysis
        table_lower = table_name.lower()
        
        # Configuration/lookup tables (usually small, full replace is fine)
        if any(keyword in table_lower for keyword in ['config', 'tipos', 'estados', 'categorias']):
            incremental_score -= 15
            reasons.append("Configuration/lookup table - full replace recommended")
        
        # Historical/audit tables (good for incremental)
        if any(keyword in table_lower for keyword in ['his', 'hist', 'log', 'audit']):
            incremental_score += 20
            reasons.append("Historical/audit table - good incremental candidate")
        
        # Transaction tables (good for incremental)
        if any(keyword in table_lower for keyword in ['fac', 'alb', 'ped', 'mov']):
            incremental_score += 15
            reasons.append("Transaction table - likely benefits from incremental loading")
        
        # Determine final strategy
        if incremental_score >= 70:
            loading_strategy = "INCREMENTAL_PREFERRED"
            confidence = "HIGH"
        elif incremental_score >= 40:
            loading_strategy = "INCREMENTAL_POSSIBLE"
            confidence = "MEDIUM"
        elif incremental_score >= 20:
            loading_strategy = "INCREMENTAL_CHALLENGING"
            confidence = "MEDIUM"
        else:
            loading_strategy = "FULL_REPLACE"
            confidence = "HIGH"
        
        return {
            'loading_strategy': loading_strategy,
            'confidence': confidence,
            'incremental_score': incremental_score,
            'recommended_watermark': recommended_watermark,
            'reasons': reasons,
            'strategy_notes': self._get_strategy_notes(loading_strategy, recommended_watermark)
        }
    
    def _get_strategy_notes(self, strategy: str, watermark: str) -> str:
        """Get implementation notes for the loading strategy"""
        
        if strategy == "INCREMENTAL_PREFERRED":
            if watermark:
                return f"Implement incremental loading using '{watermark}' as watermark column. " \
                       f"Query: SELECT * FROM table WHERE {watermark} > last_run_timestamp"
            else:
                return "Incremental loading recommended but requires manual watermark column identification"
        
        elif strategy == "INCREMENTAL_POSSIBLE":
            if watermark:
                return f"Incremental loading feasible with '{watermark}'. " \
                       f"Consider data volume and update patterns to decide vs full replace"
            else:
                return "Incremental loading possible but may require custom logic or composite keys"
        
        elif strategy == "INCREMENTAL_CHALLENGING":
            return "Incremental loading technically possible but may be complex. " \
                   "Consider full replace unless table is very large"
        
        else:  # FULL_REPLACE
            return "Full table replacement recommended. Simple and reliable approach"
    
    def generate_incremental_analysis(self):
        """Generate comprehensive incremental loading analysis"""
        self.logger.info("Starting incremental loading analysis...")
        
        tables = self.discover_all_tables()
        
        for i, table_name in enumerate(tables, 1):
            self.logger.info(f"Analyzing table {i:3d}/{len(tables)}: {table_name}")
            
            analysis = self.analyze_table_for_incremental_loading(table_name)
            self.tables_analysis.append(analysis)
            
            # Categorize tables
            strategy = analysis.get('loading_strategy', 'ERROR')
            if strategy in ['INCREMENTAL_PREFERRED', 'INCREMENTAL_POSSIBLE']:
                self.incremental_candidates.append(analysis)
            else:
                self.full_load_only.append(analysis)
        
        self.logger.info(f"Analysis complete: {len(tables)} tables analyzed")
        self.logger.info(f"Incremental candidates: {len(self.incremental_candidates)}")
        self.logger.info(f"Full load recommended: {len(self.full_load_only)}")
        
        return {
            'all_tables': self.tables_analysis,
            'incremental_candidates': self.incremental_candidates,
            'full_load_tables': self.full_load_only,
            'summary': {
                'total_tables': len(tables),
                'incremental_candidates': len(self.incremental_candidates),
                'full_load_only': len(self.full_load_only)
            }
        }
    
    def export_to_excel(self, analysis_results: dict, filename: str = None):
        """Export incremental loading analysis to Excel"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"incremental_loading_analysis_{timestamp}.xlsx"
        
        output_path = os.path.join(self.output_dir, filename)
        
        # Create DataFrames
        all_tables_df = pd.DataFrame(analysis_results['all_tables'])
        incremental_df = pd.DataFrame(analysis_results['incremental_candidates'])
        full_load_df = pd.DataFrame(analysis_results['full_load_tables'])
        
        # Create summary DataFrame
        summary_data = []
        for table in analysis_results['all_tables']:
            summary_data.append({
                'Table': table['table_name'],
                'Strategy': table.get('loading_strategy', 'ERROR'),
                'Confidence': table.get('confidence', 'LOW'),
                'Score': table.get('incremental_score', 0),
                'Rows': table.get('row_count', 0),
                'Size_MB': table.get('data_size_mb', 0),
                'Watermark_Column': table.get('recommended_watermark', ''),
                'Primary_Keys': ', '.join([pk['column'] for pk in table.get('primary_keys', [])]),
                'Timestamp_Columns': ', '.join([tc['column'] for tc in table.get('timestamp_columns', [])]),
                'Main_Reason': table.get('reasons', [''])[0] if table.get('reasons') else '',
                'Implementation_Notes': table.get('strategy_notes', '')
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        # Create detailed columns analysis
        detailed_data = []
        for table in analysis_results['all_tables']:
            table_name = table['table_name']
            for ts_col in table.get('timestamp_columns', []):
                detailed_data.append({
                    'Table': table_name,
                    'Column': ts_col['column'],
                    'Type': ts_col['type'],
                    'Nullable': ts_col['nullable'],
                    'Default': ts_col.get('default', ''),
                    'Detection_Method': ts_col['detection_method'],
                    'Matched_Pattern': ts_col.get('matched_pattern', ''),
                    'Is_Watermark': ts_col['column'] == table.get('recommended_watermark', ''),
                    'Table_Strategy': table['loading_strategy']
                })
        
        detailed_df = pd.DataFrame(detailed_data)
        
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            incremental_df.to_excel(writer, sheet_name='Incremental_Candidates', index=False)
            full_load_df.to_excel(writer, sheet_name='Full_Load_Tables', index=False)
            detailed_df.to_excel(writer, sheet_name='Timestamp_Columns', index=False)
            all_tables_df.to_excel(writer, sheet_name='All_Tables_Raw', index=False)
            
            # Add statistics sheet
            stats_data = [
                ['Total Tables', len(analysis_results['all_tables'])],
                ['Incremental Candidates', len(analysis_results['incremental_candidates'])],
                ['Full Load Only', len(analysis_results['full_load_tables'])],
                ['', ''],
                ['Strategy Breakdown', ''],
                ['INCREMENTAL_PREFERRED', len([t for t in analysis_results['all_tables'] 
                                              if t['loading_strategy'] == 'INCREMENTAL_PREFERRED'])],
                ['INCREMENTAL_POSSIBLE', len([t for t in analysis_results['all_tables'] 
                                             if t['loading_strategy'] == 'INCREMENTAL_POSSIBLE'])],
                ['INCREMENTAL_CHALLENGING', len([t for t in analysis_results['all_tables'] 
                                                if t['loading_strategy'] == 'INCREMENTAL_CHALLENGING'])],
                ['FULL_REPLACE', len([t for t in analysis_results['all_tables'] 
                                     if t['loading_strategy'] == 'FULL_REPLACE'])],
                ['', ''],
                ['Confidence Levels', ''],
                ['HIGH', len([t for t in analysis_results['all_tables'] if t['confidence'] == 'HIGH'])],
                ['MEDIUM', len([t for t in analysis_results['all_tables'] if t['confidence'] == 'MEDIUM'])],
                ['LOW', len([t for t in analysis_results['all_tables'] if t['confidence'] == 'LOW'])]
            ]
            stats_df = pd.DataFrame(stats_data, columns=['Metric', 'Value'])
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
        
        self.logger.info(f"Incremental loading analysis exported to: {output_path}")
        return output_path
    
    def export_to_csv(self, analysis_results: dict, base_filename: str = None):
        """Export analysis to CSV files"""
        if not base_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"incremental_analysis_{timestamp}"
        
        csv_dir = os.path.join(self.output_dir, f"{base_filename}_csv")
        os.makedirs(csv_dir, exist_ok=True)
        
        # Export summary
        summary_data = []
        for table in analysis_results['all_tables']:
            summary_data.append({
                'table': table['table_name'],
                'strategy': table.get('loading_strategy', 'ERROR'),
                'confidence': table.get('confidence', 'LOW'),
                'score': table.get('incremental_score', 0),
                'rows': table.get('row_count', 0),
                'watermark_column': table.get('recommended_watermark', ''),
                'primary_keys': ', '.join([pk['column'] for pk in table.get('primary_keys', [])]),
                'timestamp_columns': ', '.join([tc['column'] for tc in table.get('timestamp_columns', [])]),
                'implementation_notes': table.get('strategy_notes', '')
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(os.path.join(csv_dir, 'summary.csv'), index=False)
        
        # Export incremental candidates
        if analysis_results['incremental_candidates']:
            inc_df = pd.DataFrame(analysis_results['incremental_candidates'])
            inc_df.to_csv(os.path.join(csv_dir, 'incremental_candidates.csv'), index=False)
        
        self.logger.info(f"CSV files exported to: {csv_dir}")
        return csv_dir
    
    def generate_markdown_guide(self, analysis_results: dict, filename: str = None):
        """Generate a markdown implementation guide"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"incremental_loading_guide_{timestamp}.md"
        
        output_path = os.path.join(self.output_dir, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Incremental Loading Strategy Guide\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Summary
            f.write("## Summary\n\n")
            f.write(f"- **Total Tables Analyzed**: {len(analysis_results['all_tables'])}\n")
            f.write(f"- **Incremental Loading Candidates**: {len(analysis_results['incremental_candidates'])}\n")
            f.write(f"- **Full Load Recommended**: {len(analysis_results['full_load_tables'])}\n\n")
            
            # Strategy breakdown
            strategies = {}
            for table in analysis_results['all_tables']:
                strategy = table['loading_strategy']
                strategies[strategy] = strategies.get(strategy, 0) + 1
            
            f.write("### Strategy Breakdown\n\n")
            for strategy, count in strategies.items():
                f.write(f"- **{strategy}**: {count} tables\n")
            f.write("\n")
            
            # High priority incremental candidates
            high_confidence = [t for t in analysis_results['incremental_candidates'] 
                             if t['confidence'] == 'HIGH']
            
            if high_confidence:
                f.write("## High Priority Incremental Candidates\n\n")
                f.write("These tables are excellent candidates for incremental loading:\n\n")
                
                for table in high_confidence:
                    f.write(f"### {table['table_name']}\n\n")
                    f.write(f"- **Strategy**: {table['loading_strategy']}\n")
                    f.write(f"- **Confidence**: {table['confidence']}\n")
                    f.write(f"- **Row Count**: {table['row_count']:,}\n")
                    if table.get('recommended_watermark'):
                        f.write(f"- **Watermark Column**: `{table['recommended_watermark']}`\n")
                    f.write(f"- **Implementation**: {table.get('strategy_notes', '')}\n")
                    
                    if table.get('reasons'):
                        f.write("- **Analysis Reasons**:\n")
                        for reason in table['reasons']:
                            f.write(f"  - {reason}\n")
                    f.write("\n")
            
            # Implementation examples
            f.write("## Implementation Examples\n\n")
            
            # Example for timestamp-based incremental
            watermark_examples = [t for t in analysis_results['incremental_candidates'] 
                                if t.get('recommended_watermark')]
            
            if watermark_examples:
                example = watermark_examples[0]
                f.write("### Timestamp-based Incremental Loading\n\n")
                f.write(f"Example with table `{example['table_name']}`:\n\n")
                f.write("```sql\n")
                f.write(f"-- Initial load\n")
                f.write(f"SELECT * FROM {example['table_name']}\n")
                f.write(f"WHERE {example['recommended_watermark']} >= '2024-01-01'\n\n")
                f.write(f"-- Incremental load (subsequent runs)\n")
                f.write(f"SELECT * FROM {example['table_name']}\n")
                f.write(f"WHERE {example['recommended_watermark']} > '{{last_run_timestamp}}'\n")
                f.write("```\n\n")
            
            # Full tables list
            f.write("## Complete Analysis Results\n\n")
            
            for table in sorted(analysis_results['all_tables'], key=lambda x: x['table_name']):
                f.write(f"### {table['table_name']}\n")
                f.write(f"- **Strategy**: {table.get('loading_strategy', 'ERROR')}\n")
                f.write(f"- **Confidence**: {table.get('confidence', 'LOW')} (Score: {table.get('incremental_score', 0)})\n")
                f.write(f"- **Rows**: {table.get('row_count', 0):,}\n")
                if table.get('recommended_watermark'):
                    f.write(f"- **Watermark**: `{table['recommended_watermark']}`\n")
                if table.get('primary_keys'):
                    pk_cols = [pk['column'] for pk in table['primary_keys']]
                    f.write(f"- **Primary Keys**: {', '.join(pk_cols)}\n")
                if table.get('timestamp_columns'):
                    ts_cols = [tc['column'] for tc in table['timestamp_columns']]
                    f.write(f"- **Timestamp Columns**: {', '.join(ts_cols)}\n")
                f.write(f"- **Notes**: {table.get('strategy_notes', 'None')}\n")
                f.write("\n")
        
        self.logger.info(f"Markdown guide generated: {output_path}")
        return output_path

def main():
    """Main execution function"""
    try:
        documenter = IncrementalLoadingDocumenter()
        analysis_results = documenter.generate_incremental_analysis()
        
        # Create output directory
        os.makedirs(documenter.output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to different formats
        excel_path = documenter.export_to_excel(analysis_results)
        csv_dir = documenter.export_to_csv(analysis_results)
        markdown_path = documenter.generate_markdown_guide(analysis_results)
        
        print("\n" + "="*70)
        print("🎯 INCREMENTAL LOADING ANALYSIS COMPLETED!")
        print("="*70)
        print(f"📊 Analyzed {len(analysis_results['all_tables'])} tables")
        print(f"✅ Incremental candidates: {len(analysis_results['incremental_candidates'])}")
        print(f"🔄 Full load recommended: {len(analysis_results['full_load_tables'])}")
        
        # Show strategy breakdown
        strategies = {}
        for table in analysis_results['all_tables']:
            strategy = table.get('loading_strategy', 'ERROR')
            strategies[strategy] = strategies.get(strategy, 0) + 1
        
        print("\n📈 Strategy Breakdown:")
        for strategy, count in strategies.items():
            print(f"   {strategy}: {count} tables")
        
        # Show top incremental candidates
        high_confidence = [t for t in analysis_results['incremental_candidates'] 
                         if t.get('confidence', 'LOW') == 'HIGH']
        
        if high_confidence:
            print(f"\n🌟 Top Incremental Candidates ({len(high_confidence)} high confidence):")
            for table in high_confidence[:5]:  # Show top 5
                watermark = table.get('recommended_watermark', 'N/A')
                rows = table.get('row_count', 0)
                print(f"   - {table['table_name']}: {rows:,} rows, watermark: {watermark}")
        
        print(f"\n📁 Results saved to: {documenter.output_dir}")
        print(f"   📊 Excel: {os.path.basename(excel_path)}")
        print(f"   📄 Markdown: {os.path.basename(markdown_path)}")
        print(f"   📋 CSV: {os.path.basename(csv_dir)}")
        
    except Exception as e:
        logging.error(f"Error generating incremental loading analysis: {e}")
        raise

if __name__ == "__main__":
    main()
