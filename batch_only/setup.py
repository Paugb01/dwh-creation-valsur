"""
Setup script for batch_only extractor
Helps configure secrets.json and directory structure
"""
import json
import os
from pathlib import Path

def create_secrets_file():
    """Create secrets.json from template"""
    template_path = "./config/secrets.json.template"
    secrets_path = "./config/secrets.json"
    
    if Path(secrets_path).exists():
        print(f"✅ secrets.json already exists")
        return
    
    if not Path(template_path).exists():
        print(f"❌ Template file {template_path} not found")
        return
    
    # Copy template
    with open(template_path, 'r') as f:
        template = json.load(f)
    
    print(f"📝 Creating {secrets_path} from template...")
    
    # Interactive configuration
    print("\n🔧 MySQL Configuration:")
    template['mysql']['host'] = input(f"MySQL Host [{template['mysql']['host']}]: ") or template['mysql']['host']
    template['mysql']['port'] = int(input(f"MySQL Port [{template['mysql']['port']}]: ") or template['mysql']['port'])
    template['mysql']['database'] = input(f"Database Name [{template['mysql']['database']}]: ") or template['mysql']['database']
    template['mysql']['username'] = input(f"Username [{template['mysql']['username']}]: ") or template['mysql']['username']
    template['mysql']['password'] = input(f"Password: ") or template['mysql']['password']
    
    print("\n☁️ GCP Configuration:")
    template['gcp']['project_id'] = input(f"GCP Project ID [{template['gcp']['project_id']}]: ") or template['gcp']['project_id']
    template['gcp']['bucket_name'] = input(f"GCS Bucket Name [{template['gcp']['bucket_name']}]: ") or template['gcp']['bucket_name']
    
    # Save secrets file
    with open(secrets_path, 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"✅ Created {secrets_path}")

def create_directories():
    """Create necessary directories"""
    directories = [
        '.keys',
        'logs',
        'extracted_data',
        'extracted_data/bronze',
        'extracted_data/metadata'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def main():
    """Main setup function"""
    print("🚀 Setting up Batch Only Data Extractor")
    print("=" * 50)
    
    # Create directories
    print("\n📁 Creating directories...")
    create_directories()
    
    # Create secrets file
    print("\n🔐 Setting up credentials...")
    create_secrets_file()
    
    # Instructions
    print("\n📋 Next steps:")
    print("1. Place your GCP service account JSON file in .keys/")
    print("2. Update the service_account_key_path in secrets.json if needed")
    print("3. Install dependencies: pip install -r requirements.txt")
    print("4. Run the extractor: python run_batch.py")
    
    print("\n🎉 Setup completed!")

if __name__ == "__main__":
    main()
