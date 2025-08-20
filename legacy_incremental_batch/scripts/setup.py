"""
Setup Script for MySQL to GCP Data Pipeline
Helps users configure their secrets and test the setup
"""
import os
import json
import shutil
from config_manager import ConfigManager

def setup_secrets():
    """Interactive setup for secrets.json"""
    print("=" * 60)
    print("MySQL to GCP Data Pipeline - Setup")
    print("=" * 60)
    
    secrets_file = "secrets.json"
    template_file = "secrets.json.template"
    
    # Check if secrets.json already exists
    if os.path.exists(secrets_file):
        response = input(f"{secrets_file} already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Setup cancelled.")
            return False
    
    # Load template
    if not os.path.exists(template_file):
        print(f"❌ Template file {template_file} not found!")
        return False
    
    with open(template_file, 'r') as f:
        secrets_template = json.load(f)
    
    print("\nPlease provide the following information:")
    print("-" * 40)
    
    # Database credentials
    print("\n📊 Database Configuration:")
    db_username = input("MySQL Username [root]: ").strip() or "root"
    db_password = input("MySQL Password: ").strip()
    
    if not db_password:
        print("❌ Password cannot be empty!")
        return False
    
    # GCP configuration (optional for now)
    print("\n☁️  GCP Configuration (optional - press Enter to skip):")
    gcp_project = input("GCP Project ID: ").strip()
    gcp_bucket = input("GCP Bucket Name: ").strip()
    gcp_service_account = input("Service Account Key Path: ").strip()
    
    # Update secrets
    secrets = {
        "database": {
            "username": db_username,
            "password": db_password
        },
        "gcp": {
            "project_id": gcp_project or "your-gcp-project-id",
            "bucket_name": gcp_bucket or "your-dwh-bucket",
            "service_account_key_path": gcp_service_account or "path/to/service-account.json"
        }
    }
    
    # Save secrets.json
    with open(secrets_file, 'w') as f:
        json.dump(secrets, f, indent=2)
    
    print(f"\n✅ {secrets_file} created successfully!")
    return True

def test_configuration():
    """Test the configuration setup"""
    print("\n" + "=" * 40)
    print("Testing Configuration")
    print("=" * 40)
    
    try:
        config_manager = ConfigManager()
        
        # Test config loading
        print("📁 Loading configuration...")
        config = config_manager.load_config()
        print("✅ config.json loaded successfully")
        
        # Test secrets loading
        print("🔐 Loading secrets...")
        secrets = config_manager.load_secrets()
        print("✅ secrets.json loaded successfully")
        
        # Test database config
        print("📊 Testing database configuration...")
        db_config = config_manager.get_database_config()
        print(f"✅ Database config: {db_config['host']}:{db_config['port']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def test_database_connection():
    """Test database connection"""
    print("\n" + "=" * 40)
    print("Testing Database Connection")
    print("=" * 40)
    
    try:
        import pymysql
        config_manager = ConfigManager()
        db_config = config_manager.get_database_config()
        
        print("🔌 Connecting to database...")
        connection = pymysql.connect(**db_config)
        print("✅ Database connection successful!")
        
        # Test query
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = %s", 
                      (db_config['database'],))
        result = cursor.fetchone()
        print(f"✅ Found {result[0]} tables in database")
        
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def main():
    """Main setup function"""
    print("Welcome to the MySQL to GCP Data Pipeline Setup!")
    print("\nThis script will help you configure your credentials securely.")
    
    # Step 1: Setup secrets
    if not setup_secrets():
        return
    
    # Step 2: Test configuration
    if not test_configuration():
        return
    
    # Step 3: Test database connection
    if not test_database_connection():
        return
    
    print("\n" + "=" * 60)
    print("🎉 Setup completed successfully!")
    print("=" * 60)
    print("\nYou can now run the data extractor:")
    print("  python simple_extractor.py")
    print("\nRemember:")
    print("  - secrets.json is excluded from git (never commit it!)")
    print("  - config.json contains non-sensitive configuration")
    print("  - Use secrets.json.template for sharing configuration structure")

if __name__ == "__main__":
    main()
