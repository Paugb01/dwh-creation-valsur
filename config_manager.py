"""
Configuration and Secrets Manager
Handles loading of configuration and sensitive data securely
"""
import json
import os
from typing import Dict, Any

class ConfigManager:
    def __init__(self, config_file: str = "config.json", secrets_file: str = "secrets.json"):
        self.config_file = config_file
        self.secrets_file = secrets_file
        self._config = None
        self._secrets = None
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from config.json"""
        if self._config is None:
            if not os.path.exists(self.config_file):
                raise FileNotFoundError(f"Configuration file {self.config_file} not found")
            
            with open(self.config_file, 'r') as f:
                self._config = json.load(f)
        
        return self._config
    
    def load_secrets(self) -> Dict[str, Any]:
        """Load secrets from secrets.json"""
        if self._secrets is None:
            if not os.path.exists(self.secrets_file):
                raise FileNotFoundError(
                    f"Secrets file {self.secrets_file} not found. "
                    f"Please copy {self.secrets_file}.template to {self.secrets_file} "
                    f"and fill in your credentials."
                )
            
            with open(self.secrets_file, 'r') as f:
                self._secrets = json.load(f)
        
        return self._secrets
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get complete database configuration including credentials"""
        config = self.load_config()
        secrets = self.load_secrets()
        
        db_config = config['database'].copy()
        db_config.update(secrets['database'])
        
        # Map to PyMySQL parameter names
        return {
            'host': db_config['host'],
            'port': db_config.get('port', 3306),
            'user': db_config['username'],
            'password': db_config['password'],
            'database': db_config['database'],
            'connect_timeout': db_config.get('connection_timeout', 5)
        }
    
    def get_extraction_config(self) -> Dict[str, Any]:
        """Get extraction configuration"""
        config = self.load_config()
        return config['extraction']
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        config = self.load_config()
        return config['logging']
    
    def get_gcp_config(self) -> Dict[str, Any]:
        """Get GCP configuration"""
        secrets = self.load_secrets()
        return secrets.get('gcp', {})

# Global config manager instance
config_manager = ConfigManager()
