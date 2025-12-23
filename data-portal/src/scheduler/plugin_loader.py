"""Plugin loader for data sources"""

import yaml
import importlib
import logging
from typing import Dict, Any
from pathlib import Path
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata

logger = logging.getLogger(__name__)


def calculate_update_frequency(frequency: str) -> str:
    """
    Calculate update frequency as one quarter of the frequency.
    
    Args:
        frequency: Time frequency as PostgreSQL INTERVAL string (e.g., '1 hour', '1 day', '15 minutes')
        
    Returns:
        Update frequency string compatible with PostgreSQL INTERVAL (e.g., '15 minutes', '6 hours')
    """
    # Parse frequency
    parts = frequency.split()
    if len(parts) != 2:
        return '15 minutes'  # fallback
    
    try:
        value = int(parts[0])
        unit = parts[1].lower()
        
        # Normalize unit to singular/plural
        unit_singular = unit.rstrip('s')  # Remove trailing 's' if present
        
        # Convert to minutes for calculation
        minutes_map = {
            'minute': 1,
            'hour': 60,
            'day': 1440
        }
        
        if unit_singular not in minutes_map:
            logger.warning(f"Unknown time unit '{unit}' in frequency '{frequency}', using fallback")
            return '15 minutes'
        
        # Convert to total minutes
        total_minutes = value * minutes_map[unit_singular]
        
        # Calculate quarter
        quarter_minutes = total_minutes // 4
        
        # Ensure minimum of 1 minute
        if quarter_minutes < 1:
            quarter_minutes = 1
        
        # Convert back to appropriate unit
        if quarter_minutes >= 1440 and quarter_minutes % 1440 == 0:
            # Convert to days
            result_value = quarter_minutes // 1440
            result_unit = 'day' if result_value == 1 else 'days'
        elif quarter_minutes >= 60 and quarter_minutes % 60 == 0:
            # Convert to hours
            result_value = quarter_minutes // 60
            result_unit = 'hour' if result_value == 1 else 'hours'
        else:
            # Keep as minutes
            result_value = quarter_minutes
            result_unit = 'minute' if result_value == 1 else 'minutes'
        
        return f"{result_value} {result_unit}"

    except (ValueError, IndexError) as e:
        logger.warning(f"Failed to parse frequency '{frequency}': {e}, using fallback")
        return '15 minutes'  # fallback


class PluginLoader:
    """Loads and manages data source plugins from YAML configuration"""
    
    def __init__(self, config_path: str = "src/plugins/configs/sources.yaml"):
        self.config_path = config_path
        self.plugins: Dict[str, BasePlugin] = {}
        
    def load_plugins(self) -> Dict[str, BasePlugin]:
        """Load all plugins from configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'timeseries' not in config:
                logger.warning(f"No timeseries configuration found in {self.config_path}")
                return {}
            
            timeseries_config = config['timeseries']
            
            for endpoint_prefix, plugin_config in timeseries_config.items():
                try:
                    plugin = self._load_single_plugin(endpoint_prefix, plugin_config)
                    if plugin:
                        self.plugins[endpoint_prefix] = plugin
                        logger.info(f"Loaded plugin: {endpoint_prefix}")
                except Exception as e:
                    logger.error(f"Failed to load plugin {endpoint_prefix}: {e}", exc_info=True)
            
            logger.info(f"Successfully loaded {len(self.plugins)} plugins")
            return self.plugins
            
        except Exception as e:
            logger.error(f"Failed to load plugin configuration from {self.config_path}: {e}", exc_info=True)
            return {}
    
    def _load_single_plugin(self, endpoint_prefix: str, config: Dict[str, Any]) -> BasePlugin:
        """Load a single plugin from configuration"""
        module_name = config.get('module')
        class_name = config.get('class')
        metadata_dict = config.get('metadata', {})
        default_params = config.get('default_params', {})
        
        if not module_name or not class_name:
            raise ValueError(f"Missing module or class for plugin {endpoint_prefix}")
        
        # Create metadata object
        # Note: frequency values from YAML are strings that will be automatically
        # converted to PostgreSQL INTERVAL type when inserted into the database
        metadata = TimeSeriesMetadata(
            endpoint_prefix=endpoint_prefix,
            name=metadata_dict.get('name', endpoint_prefix),
            description=metadata_dict.get('description', ''),
            frequency=metadata_dict.get('frequency', '1 hour'),
            unit=metadata_dict.get('unit', ''),
            domain=metadata_dict.get('domain', ''),
            subdomain=metadata_dict.get('subdomain', ''),
            update_frequency=calculate_update_frequency(metadata_dict.get('frequency', '1 hour'))
        )
        
        # Dynamically import the plugin class
        try:
            module = importlib.import_module(module_name)
            plugin_class = getattr(module, class_name)
            plugin_instance = plugin_class(metadata, default_params)
            return plugin_instance
        except Exception as e:
            raise ImportError(f"Failed to import {class_name} from {module_name}: {e}")
    
    def get_plugin(self, endpoint_prefix: str) -> BasePlugin | None:
        """Get a loaded plugin by endpoint prefix"""
        return self.plugins.get(endpoint_prefix)
    
    def get_all_plugins(self) -> Dict[str, BasePlugin]:
        """Get all loaded plugins"""
        return self.plugins
    
    def get_plugin_ids(self) -> list:
        """Get list of all plugin IDs"""
        return list(self.plugins.keys())
