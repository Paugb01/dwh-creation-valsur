# batch_only/utils.py
"""
Utility functions for credential and configuration management
Optimized for single service account usage across all GCP services
"""
import os
import json
from typing import Optional, Dict, Any
from google.oauth2 import service_account
from google.auth import default


def read_file_safely(file_path: str) -> Optional[str]:
    """
    Read file content with error handling
    
    Args:
        file_path: Path to the file to read
        
    Returns:
        File content as string or None if file cannot be read
    """
    if not file_path or not os.path.isfile(file_path):
        return None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        print(f"Warning: Could not read file {file_path}: {e}")
        return None


def get_service_account_credentials(env_var_name: str = 'GOOGLE_APPLICATION_CREDENTIALS') -> Optional[service_account.Credentials]:
    """
    Get service account credentials from environment variable
    Supports both JSON content and file paths
    
    Args:
        env_var_name: Environment variable name containing credentials
        
    Returns:
        Service account credentials or None if not found
    """
    env_value = os.getenv(env_var_name)
    
    if not env_value:
        return None
    
    try:
        # Handle JSON content directly (production deployment)
        if env_value.strip().startswith('{'):
            credentials_info = json.loads(env_value)
            return service_account.Credentials.from_service_account_info(credentials_info)
        
        # Handle file path (development/local)
        if os.path.isfile(env_value):
            return service_account.Credentials.from_service_account_file(env_value)
        
        # Try common path variations
        possible_paths = [
            env_value,
            env_value.replace('service_account.json', 'service-account.json'),
            env_value.replace('service-account.json', 'service_account.json'),
            '.keys/service-account.json',
            'gcs_to_bq/.keys/service-account.json'
        ]
        
        for path in possible_paths:
            if os.path.isfile(path):
                return service_account.Credentials.from_service_account_file(path)
        
        print(f"Error: Credentials file not found in any expected location")
        return None
            
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {env_var_name}: {e}")
        return None
    except Exception as e:
        print(f"Error loading credentials from {env_var_name}: {e}")
        return None


def get_credentials() -> Optional[service_account.Credentials]:
    """
    Get credentials with automatic fallback strategy
    Uses single service account for all GCP services
    
    Priority order:
    1. GOOGLE_APPLICATION_CREDENTIALS environment variable
    2. Common local file paths
    3. Default Application Credentials
    
    Returns:
        Service account credentials or None
    """
    # Try primary environment variable
    credentials = get_service_account_credentials('GOOGLE_APPLICATION_CREDENTIALS')
    if credentials:
        print("Credentials loaded from GOOGLE_APPLICATION_CREDENTIALS")
        return credentials
    
    # Try legacy environment variables for backward compatibility
    for env_var in ['GCS_APPLICATION_CREDENTIALS', 'BQ_APPLICATION_CREDENTIALS']:
        credentials = get_service_account_credentials(env_var)
        if credentials:
            print(f"Credentials loaded from {env_var}")
            return credentials
    
    # Try common local file paths
    local_paths = [
        '.keys/service-account.json',
        'gcs_to_bq/.keys/service-account.json',
        'config/service-account.json'
    ]
    
    for path in local_paths:
        if os.path.isfile(path):
            try:
                credentials = service_account.Credentials.from_service_account_file(path)
                print(f"Credentials loaded from local file: {path}")
                return credentials
            except Exception as e:
                print(f"Warning: Could not load credentials from {path}: {e}")
    
    # Fallback to default credentials
    try:
        default_credentials, _ = default()
        print("Using default Application Credentials")
        return default_credentials
    except Exception as e:
        print(f"Warning: Could not load default credentials: {e}")
    
    print("Error: No valid credentials found")
    return None


def load_configuration(env_var_name: str = 'SECRETS') -> Optional[Dict[str, Any]]:
    """
    Load configuration from environment variable or default paths
    
    Args:
        env_var_name: Environment variable containing config path or JSON
        
    Returns:
        Configuration dictionary or None
    """
    env_value = os.getenv(env_var_name)
    
    # Try default paths if environment variable not set
    if not env_value:
        default_paths = ['config/secrets.json', 'secrets.json']
        for path in default_paths:
            if os.path.isfile(path):
                env_value = path
                break
    
    if not env_value:
        print(f"Warning: No configuration found for {env_var_name}")
        return None
    
    try:
        # Handle JSON content
        if env_value.strip().startswith('{'):
            return json.loads(env_value)
        
        # Handle file path
        if os.path.isfile(env_value):
            with open(env_value, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        print(f"Error: Configuration file not found: {env_value}")
        return None
            
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return None


def setup_environment():
    """
    Setup environment with automatic credential path detection
    Sets GOOGLE_APPLICATION_CREDENTIALS to the correct path
    """
    # Skip if already set
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        return
    
    # Auto-detect credential file locations
    credential_paths = [
        '.keys/service-account.json',
        'gcs_to_bq/.keys/service-account.json'
    ]
    
    for path in credential_paths:
        if os.path.isfile(path):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
            print(f"Set credentials to {path}")
            break
    
    # Auto-detect secrets file
    if not os.getenv('SECRETS') and os.path.isfile('config/secrets.json'):
        os.environ['SECRETS'] = 'config/secrets.json'
        print("Set secrets to config/secrets.json")


# Backward compatibility aliases
get_credentials_auto = get_credentials
load_config_from_env = load_configuration