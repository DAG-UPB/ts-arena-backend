"""Plugin loader for data sources"""

import os
import yaml
import importlib
import logging
from typing import Dict, Any, List
from src.plugins.base_plugin import BasePlugin, TimeSeriesMetadata, MultiSeriesPlugin, TimeSeriesDefinition

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
        
        # Ensure minimum of 3 minutes
        if quarter_minutes < 3:
            quarter_minutes = 3
        
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
        self.multi_series_plugins: Dict[str, MultiSeriesPlugin] = {}
        
    def load_plugins(self) -> Dict[str, BasePlugin]:
        """Load all single-series plugins from configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config:
                logger.warning(f"Empty configuration in {self.config_path}")
                return {}
            
            # Load legacy single-series plugins
            if 'timeseries' in config:
                timeseries_config = config['timeseries']
                for unique_id, plugin_config in timeseries_config.items():
                    try:
                        plugin = self._load_single_plugin(unique_id, plugin_config)
                        if plugin:
                            self.plugins[unique_id] = plugin
                            logger.info(f"Loaded plugin: {unique_id}")
                    except Exception as e:
                        logger.error(f"Failed to load plugin {unique_id}: {e}", exc_info=True)
            
            logger.info(f"Successfully loaded {len(self.plugins)} single-series plugins")
            return self.plugins
            
        except Exception as e:
            logger.error(f"Failed to load plugin configuration from {self.config_path}: {e}", exc_info=True)
            return {}
    
    def load_multi_series_plugins(self) -> Dict[str, MultiSeriesPlugin]:
        """Load all multi-series plugins from request_groups configuration"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'request_groups' not in config:
                logger.info("No request_groups configuration found")
                return {}
            
            request_groups = config['request_groups']
            
            for group_id, group_config in request_groups.items():
                try:
                    plugin = self._load_multi_series_plugin(group_id, group_config)
                    if plugin:
                        self.multi_series_plugins[group_id] = plugin
                        logger.info(f"Loaded multi-series plugin: {group_id} with {len(plugin.get_series_definitions())} series")
                except Exception as e:
                    logger.error(f"Failed to load multi-series plugin {group_id}: {e}", exc_info=True)
            
            logger.info(f"Successfully loaded {len(self.multi_series_plugins)} multi-series plugins")
            return self.multi_series_plugins
            
        except Exception as e:
            logger.error(f"Failed to load request_groups from {self.config_path}: {e}", exc_info=True)
            return {}
    
    def _load_multi_series_plugin(self, group_id: str, config: Dict[str, Any]) -> MultiSeriesPlugin:
        """Load a single multi-series plugin from configuration"""
        module_name = config.get('module')
        class_name = config.get('class')
        request_params = config.get('request_params', {})
        schedule = config.get('schedule', '15 minutes')
        timeseries_list = config.get('timeseries', [])
        
        if not module_name or not class_name:
            raise ValueError(f"Missing module or class for multi-series plugin {group_id}")
        
        if not timeseries_list:
            raise ValueError(f"No timeseries defined for multi-series plugin {group_id}")
        
        # Expand environment variables in request_params
        request_params = self._expand_env_vars(request_params)
        
        # Build series definitions
        series_definitions: List[TimeSeriesDefinition] = []
        for ts_config in timeseries_list:
            unique_id = ts_config.get('unique_id')
            if not unique_id:
                logger.warning(f"Skipping timeseries without unique_id in group {group_id}")
                continue
            
            metadata = ts_config.get('metadata', {})
            extract_filter = ts_config.get('extract_filter', {})
            
            # Calculate update_frequency from frequency if not provided
            frequency = metadata.get('frequency', '1 hour')
            update_frequency = calculate_update_frequency(frequency)
            
            # Auto-detect subdomain for known electricity-related plugins
            subdomain = metadata.get('subdomain')
            if subdomain is None:
                if 'smard' in module_name.lower() or 'gridstatus' in module_name.lower():
                    subdomain = 'electricity'
            
            definition = TimeSeriesDefinition(
                unique_id=unique_id,
                name=metadata.get('name', unique_id),
                description=metadata.get('description', ''),
                frequency=frequency,
                unit=metadata.get('unit', ''),
                domain=metadata.get('domain', ''),
                category=metadata.get('category', ''),
                subdomain=subdomain,
                subcategory=metadata.get('subcategory'),
                imputation_policy=metadata.get('imputation_policy'),
                update_frequency=update_frequency,
                extract_filter=extract_filter
            )
            series_definitions.append(definition)
        
        # Dynamically import the plugin class
        try:
            module = importlib.import_module(module_name)
            plugin_class = getattr(module, class_name)
            plugin_instance = plugin_class(
                group_id=group_id,
                request_params=request_params,
                series_definitions=series_definitions,
                schedule=schedule
            )
            return plugin_instance
        except Exception as e:
            raise ImportError(f"Failed to import {class_name} from {module_name}: {e}")
    
    def _expand_env_vars(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Expand environment variables in parameter values"""
        result = {}
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                env_var = value[2:-1]
                result[key] = os.getenv(env_var, '')
                if not result[key]:
                    logger.warning(f"Environment variable {env_var} not found")
            elif isinstance(value, dict):
                result[key] = self._expand_env_vars(value)
            else:
                result[key] = value
        return result
    
    def _load_single_plugin(self, unique_id: str, config: Dict[str, Any]) -> BasePlugin:
        """Load a single plugin from configuration"""
        module_name = config.get('module')
        class_name = config.get('class')
        metadata_dict = config.get('metadata', {})
        default_params = config.get('default_params', {})
        
        if not module_name or not class_name:
            raise ValueError(f"Missing module or class for plugin {unique_id}")
        
        # Auto-detect subdomain for known electricity-related plugins
        subdomain = metadata_dict.get('subdomain')
        if subdomain is None:
            if 'smard' in module_name.lower() or 'gridstatus' in module_name.lower():
                subdomain = 'electricity'
        
        metadata = TimeSeriesMetadata(
            unique_id=unique_id,
            name=metadata_dict.get('name', unique_id),
            description=metadata_dict.get('description', ''),
            frequency=metadata_dict.get('frequency', '1 hour'),
            unit=metadata_dict.get('unit', ''),
            domain=metadata_dict.get('domain', ''),
            subdomain=subdomain,
            category=metadata_dict.get('category') or metadata_dict.get('subdomain', ''),
            subcategory=metadata_dict.get('subcategory', ''),
            update_frequency=calculate_update_frequency(metadata_dict.get('frequency', '1 hour')),
            imputation_policy=metadata_dict.get('imputation_policy')
        )
        
        # Dynamically import the plugin class
        try:
            module = importlib.import_module(module_name)
            plugin_class = getattr(module, class_name)
            plugin_instance = plugin_class(metadata, default_params)
            return plugin_instance
        except Exception as e:
            raise ImportError(f"Failed to import {class_name} from {module_name}: {e}")
    
    def get_plugin(self, unique_id: str) -> BasePlugin | None:
        """Get a loaded plugin by unique id"""
        return self.plugins.get(unique_id)
    
    def get_multi_series_plugin(self, group_id: str) -> MultiSeriesPlugin | None:
        """Get a loaded multi-series plugin by group ID"""
        return self.multi_series_plugins.get(group_id)
    
    def get_all_plugins(self) -> Dict[str, BasePlugin]:
        """Get all loaded single-series plugins"""
        return self.plugins
    
    def get_all_multi_series_plugins(self) -> Dict[str, MultiSeriesPlugin]:
        """Get all loaded multi-series plugins"""
        return self.multi_series_plugins
    
    def get_plugin_ids(self) -> list:
        """Get list of all single-series plugin IDs"""
        return list(self.plugins.keys())
    
    def get_multi_series_plugin_ids(self) -> list:
        """Get list of all multi-series plugin group IDs"""
        return list(self.multi_series_plugins.keys())
