#!/usr/bin/env python3
"""
Pipeline Setup Script
Sets up the environment and validates configuration for the reorganized pipeline
"""

import os
import json
import shutil
from pathlib import Path

def create_secrets_file():
    """Create secrets.json from template if it doesn't exist"""
    secrets_path = Path("config/secrets.json")
    template_path = Path("config/secrets.json.template")
    
    if not secrets_path.exists() and template_path.exists():
        print("📝 Creating secrets.json from template...")
        shutil.copy(template_path, secrets_path)
        print("✅ secrets.json created. Please update with your actual credentials.")
        return False
    elif secrets_path.exists():
        print("✅ secrets.json already exists")
        return True
    else:
        print("❌ No secrets template found")
        return False

def validate_config():
    """Validate the configuration file"""
    config_path = Path("config/config.json")
    
    if not config_path.exists():
        print("❌ config.json not found")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Check required sections
        required_sections = ['mysql', 'gcs', 'bigquery', 'extraction']
        for section in required_sections:
            if section not in config:
                print(f"❌ Missing section in config: {section}")
                return False
        
        print("✅ config.json validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Error reading config.json: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = [
        "data/bronze",
        "data/metadata", 
        "logs"
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Directory created: {dir_path}")

def main():
    """Main setup function"""
    print("🔧 SETTING UP BATCH INGESTION PIPELINE")
    print("=" * 50)
    
    # Create directories
    print("\n📁 Creating directories...")
    create_directories()
    
    # Validate config
    print("\n📋 Validating configuration...")
    config_valid = validate_config()
    
    # Setup secrets
    print("\n🔐 Setting up secrets...")
    secrets_ready = create_secrets_file()
    
    # Summary
    print("\n📋 SETUP SUMMARY")
    print("=" * 30)
    print(f"Configuration: {'✅ Valid' if config_valid else '❌ Invalid'}")
    print(f"Secrets: {'✅ Ready' if secrets_ready else '📝 Needs manual setup'}")
    
    if config_valid and secrets_ready:
        print("\n🎉 Setup complete! You can now run:")
        print("   python run_full_pipeline.py")
    else:
        print("\n⚠️  Please fix the issues above before running the pipeline")
        if not secrets_ready:
            print("   1. Edit config/secrets.json with your actual credentials")
        if not config_valid:
            print("   2. Fix config.json validation errors")

if __name__ == "__main__":
    main()
