#!/usr/bin/env python3
"""
Project Structure Validation Script
Validates the reorganized batch_ingestion project structure
"""

import os
import sys
from pathlib import Path

def validate_structure():
    """Validate the project structure"""
    base_dir = Path(__file__).parent.parent
    print(f"Validating structure for: {base_dir.absolute()}")
    
    # Define expected structure
    expected_structure = {
        'config': ['config.json', 'secrets.json.template'],
        'core': ['__init__.py', 'batch_extractor.py', 'data_validator.py', 'utils.py'],
        'bigquery': ['__init__.py', 'main.py', 'strategies.py'],
        'tests': ['__init__.py', 'test_pipeline_integration.py', 'test_advanced_strategies.py'],
        'scripts': ['run_extraction.py', 'run_bigquery_ingestion.py', 'migrate_region.py'],
        'docs': ['IMPLEMENTATION.md', 'ADVANCED_STRATEGIES.md'],
        'orchestration': ['README.md', 'requirements.txt', 'setup.sh', 'deploy.sh']
    }
    
    # Root files
    root_files = ['README.md', 'requirements.txt', '.gitignore', '.env.example']
    
    print("\n🔍 Checking root files...")
    missing_root = []
    for file in root_files:
        if (base_dir / file).exists():
            print(f"✅ {file}")
        else:
            print(f"❌ {file}")
            missing_root.append(file)
    
    print("\n🔍 Checking directory structure...")
    missing_dirs = []
    missing_files = []
    
    for directory, files in expected_structure.items():
        dir_path = base_dir / directory
        if dir_path.exists():
            print(f"✅ {directory}/")
            for file in files:
                file_path = dir_path / file
                if file_path.exists():
                    print(f"  ✅ {file}")
                else:
                    print(f"  ❌ {file}")
                    missing_files.append(f"{directory}/{file}")
        else:
            print(f"❌ {directory}/")
            missing_dirs.append(directory)
    
    # Check for Python package structure
    print("\n🔍 Checking Python package structure...")
    python_dirs = ['core', 'bigquery', 'tests']
    for dir_name in python_dirs:
        init_file = base_dir / dir_name / '__init__.py'
        if init_file.exists():
            print(f"✅ {dir_name}/__init__.py")
        else:
            print(f"❌ {dir_name}/__init__.py")
    
    # Summary
    print("\n📋 VALIDATION SUMMARY")
    print("=" * 50)
    
    if not missing_root and not missing_dirs and not missing_files:
        print("🎉 ALL CHECKS PASSED! Project structure is complete.")
        return True
    else:
        if missing_root:
            print(f"❌ Missing root files: {', '.join(missing_root)}")
        if missing_dirs:
            print(f"❌ Missing directories: {', '.join(missing_dirs)}")
        if missing_files:
            print(f"❌ Missing files: {', '.join(missing_files)}")
        return False

if __name__ == "__main__":
    success = validate_structure()
    sys.exit(0 if success else 1)
