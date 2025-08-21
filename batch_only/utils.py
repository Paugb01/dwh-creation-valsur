# batch_only/utils.py
"""
Utility functions for credential and configuration management
Best practices for production deployments
"""
import os
import json
from typing import Optional, Union, Dict, Any
from google.oauth2 import service_account
from google.auth import default

def read_file_content(path: Optional[str]) -> Optional[str]:
    """
    Read file content safely with error handling
    """
    if not path:
        return None
    
    try:
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
    except Exception as e:
        print(f"Warning: Could not read file {path}: {e}")
    
    return None

def get_credentials_from_env(env_var_name: str) -> Optional[service_account.Credentials]:
    """
    Get Google Cloud credentials from environment variable
    Supports both JSON content and file paths
    
    Best practices:
    - In production: Use JSON content in env vars (more secure)
    - In development: Use file paths for convenience
    """
    env_value = os.getenv(env_var_name)
    
    if not env_value:
        print(f"Warning: Environment variable {env_var_name} not set")
        return None
    
    try:
        # Try as JSON content first (production best practice)
        if env_value.strip().startswith('{'):
            credentials_info = json.loads(env_value)
            return service_account.Credentials.from_service_account_info(credentials_info)
        
        # Try as file path (development convenience)
        elif os.path.isfile(env_value):
            return service_account.Credentials.from_service_account_file(env_value)
        
        # Handle common path issues
        else:
            # Try with common variations
            possible_paths = [
                env_value,
                env_value.replace('service_account.json', 'service-account.json'),
                env_value.replace('service-account.json', 'service_account.json'),
                os.path.join('.keys', 'service-account.json'),
                os.path.join('gcs_to_bq', '.keys', 'service-account.json')
            ]
            
            for path in possible_paths:
                if os.path.isfile(path):
                    print(f"Found credentials at: {path}")
                    return service_account.Credentials.from_service_account_file(path)
            
            print(f"Error: Could not find credentials file at any of: {possible_paths}")
            return None
            
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {env_var_name}: {e}")
        return None
    except Exception as e:
        print(f"Error loading credentials from {env_var_name}: {e}")
        return None

def get_credentials_auto() -> Optional[service_account.Credentials]:
    """
    Automatic credential detection with fallback strategy
    
    Priority order:
    1. Environment variables
    2. Local service account files
    3. Default Application Credentials
    """
    
    # Try environment variables first
    for env_var in ['GCS_APPLICATION_CREDENTIALS', 'BQ_APPLICATION_CREDENTIALS', 'GOOGLE_APPLICATION_CREDENTIALS']:
        creds = get_credentials_from_env(env_var)
        if creds:
            print(f"✅ Credentials loaded from {env_var}")
            return creds
    
    # Try common local paths
    local_paths = [
        '.keys/service-account.json',
        'gcs_to_bq/.keys/service-account.json',
        'config/service-account.json'
    ]
    
    for path in local_paths:
        if os.path.isfile(path):
            try:
                creds = service_account.Credentials.from_service_account_file(path)
                print(f"✅ Credentials loaded from local file: {path}")
                return creds
            except Exception as e:
                print(f"Warning: Could not load credentials from {path}: {e}")
    
    # Try default credentials as last resort
    try:
        creds, project = default()
        print("✅ Using default Application Credentials")
        return creds
    except Exception as e:
        print(f"Warning: Could not load default credentials: {e}")
    
    print("❌ No valid credentials found")
    return None

def load_config_from_env(env_var_name: str = 'SECRETS') -> Optional[Dict[str, Any]]:
    """
    Load configuration from environment variable or file
    """
    env_value = os.getenv(env_var_name)
    
    if not env_value:
        # Try default config paths
        default_paths = ['config/secrets.json', 'secrets.json']
        for path in default_paths:
            if os.path.isfile(path):
                env_value = path
                break
    
    if not env_value:
        print(f"Warning: No configuration found in {env_var_name}")
        return None
    
    try:
        # Try as JSON content first
        if env_value.strip().startswith('{'):
            return json.loads(env_value)
        
        # Try as file path
        elif os.path.isfile(env_value):
            with open(env_value, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        else:
            print(f"Error: Configuration file not found: {env_value}")
            return None
            
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return None

def setup_environment():
    """
    Setup environment with proper credential paths
    Call this to auto-fix common path issues
    """
    
    # Auto-detect and set correct paths
    if os.path.isfile('.keys/service-account.json'):
        os.environ['GCS_APPLICATION_CREDENTIALS'] = '.keys/service-account.json'
        os.environ['BQ_APPLICATION_CREDENTIALS'] = '.keys/service-account.json'
        print("✅ Set credentials to .keys/service-account.json")
    
    elif os.path.isfile('gcs_to_bq/.keys/service-account.json'):
        os.environ['GCS_APPLICATION_CREDENTIALS'] = 'gcs_to_bq/.keys/service-account.json'
        os.environ['BQ_APPLICATION_CREDENTIALS'] = 'gcs_to_bq/.keys/service-account.json'
        print("✅ Set credentials to gcs_to_bq/.keys/service-account.json")
    
    if os.path.isfile('config/secrets.json'):
        os.environ['SECRETS'] = 'config/secrets.json'
        print("✅ Set secrets to config/secrets.json")