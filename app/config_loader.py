"""
Configuration loader utility
"""

import os
import sys
import yaml
import logging
from typing import Dict, Any


class ConfigLoader:
    """Configuration loader for YAML files"""
    
    def __init__(self, config_path: str = "./config.yaml"):
        self.config_path = config_path
        self._config = None
    
    def load(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if self._config is not None:
            return self._config
            
        try:
            with open(self.config_path, "r") as yaml_file:
                self._config = yaml.load(yaml_file, Loader=yaml.FullLoader)
            
            if self._config is None:
                raise Exception("empty data in configuration file")
                
            return self._config
            
        except Exception as e:
            print(f"Error while loading {self.config_path}: {e}")
            sys.exit(101)
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'redis.host')"""
        if self._config is None:
            self.load()
            
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_redis_url(self) -> str:
        """Get Redis URL from config"""
        return self.get('redis.url', 'redis://localhost:6379/0')
    
    def get_log_level(self) -> int:
        """Get logging level from config"""
        level_str = self.get('log.level', 'INFO').upper()
        return getattr(logging, level_str, logging.INFO)
    
    def get_celery_config(self) -> Dict[str, Any]:
        """Get Celery configuration as dict"""
        return self.get('celery', {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get API configuration as dict"""
        return self.get('api', {})


# Global configuration instance
config_loader = ConfigLoader()
configuration = config_loader.load()
