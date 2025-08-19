#!/usr/bin/env python3
"""
Database Documentation Generator
Creates comprehensive documentation of all tables and columns in the pk_gest_xer database
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
from extractors.base_extractor import BaseExtractor

class DatabaseDocumenter(BaseExtractor):
    """Generate comprehensive database documentation"""
    
    def __init__(self):
        super().__init__()
        self.tables_info = []
        self.columns_info = []
        self.relationships = []
        
    def discover_all_tables(self):
        """Discover all tables in the database"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                # Get all tables
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]
                
                self.logger.info(f"Found {len(tables)} tables in database")
                return tables
        finally:
            connection.close()
    
    def get_table_info(self, table_name: str) -> dict:
        """Get detailed information about a table"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                # Get table status information
                cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
                status = cursor.fetchone()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                
                # Get table creation info
                cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                create_table = cursor.fetchone()[1]
                
                return {
                    'table_name': table_name,
                    'engine': status[1] if status and status[1] else 'Unknown',
                    'row_count': row_count,
                    'data_length': status[6] if status and status[6] is not None else 0,
                    'index_length': status[8] if status and status[8] is not None else 0,
                    'auto_increment': status[10] if status and status[10] is not None else None,
                    'create_time': status[11] if status and status[11] is not None else None,
                    'update_time': status[12] if status and status[12] is not None else None,
                    'table_comment': status[17] if status and status[17] is not None else '',
                    'create_statement': create_table
                }
        except Exception as e:
            self.logger.error(f"Error getting info for table {table_name}: {e}")
            return {
                'table_name': table_name,
                'engine': 'Unknown',
                'row_count': 0,
                'data_length': 0,
                'index_length': 0,
                'auto_increment': None,
                'create_time': None,
                'update_time': None,
                'table_comment': '',
                'create_statement': ''
            }
        finally:
            connection.close()
    
    def get_detailed_columns_info(self, table_name: str) -> list:
        """Get detailed column information for a table"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                # Get column information
                cursor.execute(f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
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
                
                return [{
                    'table_name': table_name,
                    'column_name': col[0],
                    'data_type': col[1],
                    'is_nullable': col[2],
                    'column_default': col[3],
                    'character_max_length': col[4],
                    'numeric_precision': col[5],
                    'numeric_scale': col[6],
                    'column_type': col[7],
                    'column_key': col[8],
                    'extra': col[9],
                    'column_comment': col[10]
                } for col in columns]
                
        except Exception as e:
            self.logger.error(f"Error getting columns for table {table_name}: {e}")
            return []
        finally:
            connection.close()
    
    def detect_foreign_keys(self):
        """Detect foreign key relationships"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        CONSTRAINT_NAME,
                        TABLE_NAME,
                        COLUMN_NAME,
                        REFERENCED_TABLE_NAME,
                        REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """)
                
                fks = cursor.fetchall()
                
                return [{
                    'constraint_name': fk[0],
                    'from_table': fk[1],
                    'from_column': fk[2],
                    'to_table': fk[3],
                    'to_column': fk[4]
                } for fk in fks]
                
        except Exception as e:
            self.logger.error(f"Error detecting foreign keys: {e}")
            return []
        finally:
            connection.close()
    
    def get_indexes_info(self, table_name: str) -> list:
        """Get index information for a table"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW INDEX FROM `{table_name}`")
                indexes = cursor.fetchall()
                
                return [{
                    'table_name': table_name,
                    'key_name': idx[2],
                    'column_name': idx[4],
                    'unique': not bool(idx[1]),
                    'seq_in_index': idx[3],
                    'cardinality': idx[6],
                    'index_type': idx[10] if len(idx) > 10 else 'BTREE'
                } for idx in indexes]
                
        except Exception as e:
            self.logger.error(f"Error getting indexes for table {table_name}: {e}")
            return []
        finally:
            connection.close()
    
    def analyze_sample_data(self, table_name: str, limit: int = 5) -> dict:
        """Analyze sample data from table"""
        connection = self.get_mysql_connection()
        try:
            with connection.cursor() as cursor:
                # Get sample data
                cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit}")
                sample_data = cursor.fetchall()
                
                # Get column names
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = [col[0] for col in cursor.fetchall()]
                
                return {
                    'table_name': table_name,
                    'sample_rows': len(sample_data),
                    'columns': columns,
                    'sample_data': [dict(zip(columns, row)) for row in sample_data]
                }
                
        except Exception as e:
            self.logger.error(f"Error analyzing sample data for table {table_name}: {e}")
            return {'table_name': table_name, 'sample_rows': 0, 'columns': [], 'sample_data': []}
        finally:
            connection.close()
    
    def generate_documentation(self):
        """Generate comprehensive database documentation"""
        self.logger.info("Starting database documentation generation...")
        
        # Discover all tables
        tables = self.discover_all_tables()
        
        # Collect information for each table
        all_indexes = []
        all_samples = []
        
        for table_name in tables:
            self.logger.info(f"Analyzing table: {table_name}")
            
            # Table info
            table_info = self.get_table_info(table_name)
            self.tables_info.append(table_info)
            
            # Column info
            columns = self.get_detailed_columns_info(table_name)
            self.columns_info.extend(columns)
            
            # Index info
            indexes = self.get_indexes_info(table_name)
            all_indexes.extend(indexes)
            
            # Sample data (optional, smaller tables only)
            if table_info['row_count'] < 10000:  # Only for smaller tables
                sample = self.analyze_sample_data(table_name)
                all_samples.append(sample)
        
        # Foreign keys
        self.relationships = self.detect_foreign_keys()
        
        self.logger.info(f"Documentation complete: {len(tables)} tables analyzed")
        
        return {
            'tables': self.tables_info,
            'columns': self.columns_info,
            'indexes': all_indexes,
            'foreign_keys': self.relationships,
            'samples': all_samples
        }
    
    def export_to_excel(self, data: dict, output_path: str):
        """Export documentation to Excel with multiple sheets"""
        self.logger.info(f"Exporting to Excel: {output_path}")
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Tables overview
            tables_df = pd.DataFrame(data['tables'])
            if not tables_df.empty:
                # Format data lengths
                tables_df['data_size_mb'] = ((tables_df['data_length'].fillna(0)) / 1024 / 1024).round(2)
                tables_df['index_size_mb'] = ((tables_df['index_length'].fillna(0)) / 1024 / 1024).round(2)
                
                # Select key columns for overview
                overview_cols = ['table_name', 'engine', 'row_count', 'data_size_mb', 
                               'index_size_mb', 'auto_increment', 'create_time', 'table_comment']
                tables_overview = tables_df[overview_cols].copy()
                tables_overview.to_excel(writer, sheet_name='Tables_Overview', index=False)
            
            # Detailed columns
            columns_df = pd.DataFrame(data['columns'])
            if not columns_df.empty:
                columns_df.to_excel(writer, sheet_name='Columns_Detail', index=False)
            
            # Indexes
            indexes_df = pd.DataFrame(data['indexes'])
            if not indexes_df.empty:
                indexes_df.to_excel(writer, sheet_name='Indexes', index=False)
            
            # Foreign keys
            fk_df = pd.DataFrame(data['foreign_keys'])
            if not fk_df.empty:
                fk_df.to_excel(writer, sheet_name='Foreign_Keys', index=False)
            
            # Create a summary sheet
            summary_data = {
                'Metric': [
                    'Total Tables',
                    'Total Columns',
                    'Total Indexes',
                    'Foreign Key Relationships',
                    'Total Database Size (MB)',
                    'Analysis Date'
                ],
                'Value': [
                    len(data['tables']),
                    len(data['columns']),
                    len(data['indexes']),
                    len(data['foreign_keys']),
                    round(sum((t.get('data_length') or 0) + (t.get('index_length') or 0) for t in data['tables']) / 1024 / 1024, 2),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        self.logger.info(f"✓ Excel documentation saved to {output_path}")
    
    def export_to_csv(self, data: dict, output_dir: str):
        """Export documentation to separate CSV files"""
        self.logger.info(f"Exporting to CSV files in: {output_dir}")
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Tables
        tables_df = pd.DataFrame(data['tables'])
        if not tables_df.empty:
            tables_df.to_csv(output_path / 'tables_overview.csv', index=False)
        
        # Columns
        columns_df = pd.DataFrame(data['columns'])
        if not columns_df.empty:
            columns_df.to_csv(output_path / 'columns_detail.csv', index=False)
        
        # Indexes
        indexes_df = pd.DataFrame(data['indexes'])
        if not indexes_df.empty:
            indexes_df.to_csv(output_path / 'indexes.csv', index=False)
        
        # Foreign keys
        fk_df = pd.DataFrame(data['foreign_keys'])
        if not fk_df.empty:
            fk_df.to_csv(output_path / 'foreign_keys.csv', index=False)
        
        self.logger.info(f"✓ CSV files saved to {output_dir}")
    
    def export_to_json(self, data: dict, output_path: str):
        """Export documentation to JSON for programmatic use"""
        self.logger.info(f"Exporting to JSON: {output_path}")
        
        # Add metadata
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'database': 'pk_gest_xer',
                'total_tables': len(data['tables']),
                'total_columns': len(data['columns']),
                'generator': 'DatabaseDocumenter v1.0'
            },
            'documentation': data
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
        
        self.logger.info(f"✓ JSON documentation saved to {output_path}")
    
    def generate_markdown_report(self, data: dict, output_path: str):
        """Generate a markdown report for easy reading"""
        self.logger.info(f"Generating Markdown report: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("# Database Documentation: pk_gest_xer\n\n")
            f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Summary
            f.write("## Summary\n\n")
            f.write(f"- **Total Tables:** {len(data['tables'])}\n")
            f.write(f"- **Total Columns:** {len(data['columns'])}\n")
            f.write(f"- **Total Indexes:** {len(data['indexes'])}\n")
            f.write(f"- **Foreign Key Relationships:** {len(data['foreign_keys'])}\n\n")
            
            # Tables overview
            f.write("## Tables Overview\n\n")
            f.write("| Table Name | Engine | Rows | Data Size (MB) | Comment |\n")
            f.write("|------------|--------|------|----------------|----------|\n")
            
            for table in data['tables']:
                data_size = round((table.get('data_length') or 0) / 1024 / 1024, 2)
                comment = table.get('table_comment', '').replace('|', '\\|')[:50]
                f.write(f"| {table['table_name']} | {table['engine']} | {table['row_count']:,} | {data_size} | {comment} |\n")
            
            # Detailed table information
            f.write("\n## Detailed Table Information\n\n")
            
            for table in data['tables']:
                table_name = table['table_name']
                f.write(f"### {table_name}\n\n")
                f.write(f"- **Rows:** {table['row_count']:,}\n")
                f.write(f"- **Engine:** {table['engine']}\n")
                f.write(f"- **Data Size:** {round((table.get('data_length') or 0) / 1024 / 1024, 2)} MB\n")
                if table.get('table_comment'):
                    f.write(f"- **Comment:** {table['table_comment']}\n")
                f.write("\n")
                
                # Columns for this table
                table_columns = [col for col in data['columns'] if col['table_name'] == table_name]
                if table_columns:
                    f.write("#### Columns\n\n")
                    f.write("| Column | Type | Nullable | Key | Default | Extra |\n")
                    f.write("|--------|------|----------|-----|---------|-------|\n")
                    
                    for col in table_columns:
                        f.write(f"| {col['column_name']} | {col['column_type']} | {col['is_nullable']} | {col['column_key']} | {col['column_default'] or ''} | {col['extra']} |\n")
                
                f.write("\n---\n\n")
        
        self.logger.info(f"✓ Markdown report saved to {output_path}")

def main():
    """Main execution function"""
    try:
        # Initialize documenter
        documenter = DatabaseDocumenter()
        
        # Generate documentation
        doc_data = documenter.generate_documentation()
        
        # Create output directory
        output_dir = Path("docs/database_documentation")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to different formats
        documenter.export_to_excel(doc_data, f"docs/database_documentation/pk_gest_xer_documentation_{timestamp}.xlsx")
        documenter.export_to_csv(doc_data, f"docs/database_documentation/csv_{timestamp}")
        documenter.export_to_json(doc_data, f"docs/database_documentation/pk_gest_xer_documentation_{timestamp}.json")
        documenter.generate_markdown_report(doc_data, f"docs/database_documentation/DATABASE_GUIDE_{timestamp}.md")
        
        print("\n" + "="*60)
        print("🎉 DATABASE DOCUMENTATION COMPLETED!")
        print("="*60)
        print(f"📊 Analyzed {len(doc_data['tables'])} tables")
        print(f"📋 Documented {len(doc_data['columns'])} columns")
        print(f"🔗 Found {len(doc_data['foreign_keys'])} foreign key relationships")
        print(f"\n📁 Documentation saved to: docs/database_documentation/")
        print("📄 Available formats: Excel (.xlsx), CSV, JSON, Markdown (.md)")
        
    except Exception as e:
        logging.error(f"Error generating documentation: {e}")
        raise

if __name__ == "__main__":
    main()
