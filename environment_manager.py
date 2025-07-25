#!/usr/bin/env python3
"""
Environment Variable Management System
Centralizes and standardizes environment variable handling across the application
"""

import os
from typing import Dict, Optional, Any
from pathlib import Path
from dotenv import load_dotenv
import json

class EnvironmentManager:
    """
    Centralized environment variable manager with validation and defaults
    """
    
    def __init__(self, env_file='.env'):
        self.env_file = env_file
        self.load_environment()
        
        # Define required environment variables with validation
        self.required_vars = {
            'REDDIT_CLIENT_ID': {
                'description': 'Reddit API Client ID',
                'type': str,
                'required': True,
                'sensitive': True
            },
            'REDDIT_CLIENT_SECRET': {
                'description': 'Reddit API Client Secret',
                'type': str,
                'required': True,
                'sensitive': True
            },
            'REDDIT_USER_AGENT': {
                'description': 'Reddit API User Agent string',
                'type': str,
                'required': True,
                'default': 'reddit-insights-bot/1.0'
            },
            'GROQ_API_KEY': {
                'description': 'Groq API Key for AI summarization',
                'type': str,
                'required': False,
                'sensitive': True
            }
        }
        
        # Optional configuration variables
        self.optional_vars = {
            'MAX_WORKERS': {
                'description': 'Maximum worker threads for parallel processing',
                'type': int,
                'default': 6,
                'min_value': 1,
                'max_value': 20
            },
            'CACHE_TTL_HOURS': {
                'description': 'Cache time-to-live in hours',
                'type': int,
                'default': 2,
                'min_value': 1,
                'max_value': 24
            },
            'DEFAULT_BASE_LIMIT': {
                'description': 'Default number of posts to extract per subreddit',
                'type': int,
                'default': 100,
                'min_value': 10,
                'max_value': 1000
            },
            'ENABLE_PARALLEL_EXTRACTION': {
                'description': 'Enable parallel extraction (true/false)',
                'type': bool,
                'default': True
            },
            'ENABLE_CACHING': {
                'description': 'Enable caching system (true/false)',
                'type': bool,
                'default': True
            },
            'LOG_LEVEL': {
                'description': 'Logging level (DEBUG, INFO, WARNING, ERROR)',
                'type': str,
                'default': 'INFO',
                'choices': ['DEBUG', 'INFO', 'WARNING', 'ERROR']
            }
        }
    
    def load_environment(self):
        """Load environment variables from .env file"""
        if os.path.exists(self.env_file):
            load_dotenv(self.env_file)
            print(f"✅ Loaded environment from {self.env_file}")
        else:
            print(f"⚠️  Environment file not found: {self.env_file}")
            print("   Using system environment variables only")
    
    def get_var(self, var_name: str, default: Any = None, var_type: type = str) -> Any:
        """
        Get environment variable with type conversion and validation
        
        Args:
            var_name: Name of environment variable
            default: Default value if not found
            var_type: Type to convert to (str, int, bool, float)
            
        Returns:
            Converted and validated environment variable value
        """
        value = os.getenv(var_name, default)
        
        if value is None:
            return None
        
        # Convert string to appropriate type
        if var_type == bool:
            return str(value).lower() in ('true', '1', 'yes', 'on', 'enabled')
        elif var_type == int:
            try:
                return int(value)
            except ValueError:
                print(f"⚠️  Invalid integer value for {var_name}: {value}, using default")
                return default
        elif var_type == float:
            try:
                return float(value)
            except ValueError:
                print(f"⚠️  Invalid float value for {var_name}: {value}, using default")
                return default
        else:
            return str(value)
    
    def validate_environment(self) -> Dict[str, Any]:
        """
        Validate all environment variables and return status report
        
        Returns:
            Dict with validation results
        """
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'missing_required': [],
            'config_values': {}
        }
        
        # Check required variables
        for var_name, config in self.required_vars.items():
            value = self.get_var(var_name, None, config['type'])
            
            if value is None and config['required']:
                results['missing_required'].append(var_name)
                results['valid'] = False
                results['errors'].append(f"Missing required variable: {var_name}")
            elif value is None and not config['required']:
                results['warnings'].append(f"Optional variable not set: {var_name}")
            else:
                # Store non-sensitive values
                if not config.get('sensitive', False):
                    results['config_values'][var_name] = value
                else:
                    results['config_values'][var_name] = '[SENSITIVE]'
        
        # Check optional variables with validation
        for var_name, config in self.optional_vars.items():
            value = self.get_var(var_name, config['default'], config['type'])
            
            # Validate ranges
            if config['type'] in (int, float):
                if 'min_value' in config and value < config['min_value']:
                    results['warnings'].append(f"{var_name} below minimum ({value} < {config['min_value']})")
                    value = config['default']
                if 'max_value' in config and value > config['max_value']:
                    results['warnings'].append(f"{var_name} above maximum ({value} > {config['max_value']})")
                    value = config['default']
            
            # Validate choices
            if 'choices' in config and value not in config['choices']:
                results['warnings'].append(f"{var_name} invalid choice: {value}, using default")
                value = config['default']
            
            results['config_values'][var_name] = value
        
        return results
    
    def generate_env_template(self, output_file='.env.template'):
        """Generate a template .env file with all variables and descriptions"""
        template_lines = [
            "# Reddit Insights Environment Configuration",
            "# Copy this file to .env and fill in your values",
            "",
            "# =============================================================================",
            "# REQUIRED VARIABLES",
            "# =============================================================================",
            ""
        ]
        
        # Add required variables
        for var_name, config in self.required_vars.items():
            template_lines.append(f"# {config['description']}")
            if config.get('required', False):
                template_lines.append(f"# REQUIRED: This variable must be set")
            if 'default' in config:
                template_lines.append(f"# Default: {config['default']}")
            template_lines.append(f"{var_name}=")
            template_lines.append("")
        
        template_lines.extend([
            "# =============================================================================",
            "# OPTIONAL VARIABLES",
            "# =============================================================================",
            ""
        ])
        
        # Add optional variables
        for var_name, config in self.optional_vars.items():
            template_lines.append(f"# {config['description']}")
            template_lines.append(f"# Default: {config['default']}")
            if 'choices' in config:
                template_lines.append(f"# Choices: {', '.join(config['choices'])}")
            if config['type'] in (int, float):
                if 'min_value' in config and 'max_value' in config:
                    template_lines.append(f"# Range: {config['min_value']} - {config['max_value']}")
            template_lines.append(f"#{var_name}={config['default']}")
            template_lines.append("")
        
        # Write template file
        with open(output_file, 'w') as f:
            f.write('\\n'.join(template_lines))
        
        print(f"✅ Environment template generated: {output_file}")
        return output_file
    
    def create_config_summary(self) -> Dict[str, Any]:
        """Create a summary of current configuration for dashboard/logging"""
        validation = self.validate_environment()
        
        summary = {
            'environment_status': 'valid' if validation['valid'] else 'invalid',
            'required_vars_set': len(self.required_vars) - len(validation['missing_required']),
            'total_required_vars': len(self.required_vars),
            'configuration': {},
            'errors': validation['errors'],
            'warnings': validation['warnings']
        }
        
        # Add non-sensitive configuration values
        for var_name, value in validation['config_values'].items():
            if var_name in self.required_vars and not self.required_vars[var_name].get('sensitive', False):
                summary['configuration'][var_name] = value
            elif var_name in self.optional_vars:
                summary['configuration'][var_name] = value
        
        return summary
    
    def setup_wizard(self):
        """Interactive setup wizard for environment configuration"""
        print("🔧 Reddit Insights Environment Setup Wizard")
        print("=" * 50)
        
        env_vars = {}
        
        print("\\n📋 Required Variables:")
        for var_name, config in self.required_vars.items():
            current_value = os.getenv(var_name)
            
            print(f"\\n{var_name}:")
            print(f"  Description: {config['description']}")
            if current_value:
                if config.get('sensitive', False):
                    print(f"  Current: [SET]")
                else:
                    print(f"  Current: {current_value}")
            else:
                print(f"  Current: [NOT SET]")
            
            if config.get('required', False):
                while True:
                    value = input(f"  Enter value for {var_name}: ").strip()
                    if value:
                        env_vars[var_name] = value
                        break
                    else:
                        print("  ❌ This variable is required!")
            else:
                value = input(f"  Enter value for {var_name} (optional): ").strip()
                if value:
                    env_vars[var_name] = value
        
        print("\\n⚙️  Optional Variables (press Enter to use defaults):")
        for var_name, config in self.optional_vars.items():
            current_value = os.getenv(var_name, config['default'])
            
            print(f"\\n{var_name}:")
            print(f"  Description: {config['description']}")
            print(f"  Default: {config['default']}")
            print(f"  Current: {current_value}")
            
            value = input(f"  New value (or Enter for default): ").strip()
            if value:
                env_vars[var_name] = value
        
        # Write .env file
        self._write_env_file(env_vars)
        
        print("\\n✅ Environment configuration complete!")
        print("🔄 Restart the application to use new settings.")
    
    def _write_env_file(self, env_vars: Dict[str, str]):
        """Write environment variables to .env file"""
        lines = [
            "# Reddit Insights Environment Configuration",
            f"# Generated: {os.path.basename(__file__)} setup wizard",
            f"# Date: {__import__('datetime').datetime.now().isoformat()}",
            ""
        ]
        
        for var_name, value in env_vars.items():
            lines.append(f"{var_name}={value}")
        
        with open(self.env_file, 'w') as f:
            f.write('\\n'.join(lines))
        
        print(f"✅ Environment file written: {self.env_file}")

# Global environment manager instance
env_manager = EnvironmentManager()

def get_config(var_name: str, default: Any = None, var_type: type = str) -> Any:
    """Convenience function to get configuration values"""
    return env_manager.get_var(var_name, default, var_type)

def validate_config() -> bool:
    """Convenience function to validate configuration"""
    validation = env_manager.validate_environment()
    return validation['valid']

def main():
    """Main function for environment management"""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'validate':
            print("🔍 Validating Environment Configuration")
            print("=" * 40)
            
            validation = env_manager.validate_environment()
            
            if validation['valid']:
                print("✅ Environment configuration is valid!")
            else:
                print("❌ Environment configuration has issues:")
                for error in validation['errors']:
                    print(f"  • {error}")
            
            if validation['warnings']:
                print("\\n⚠️  Warnings:")
                for warning in validation['warnings']:
                    print(f"  • {warning}")
            
            print("\\n📊 Current Configuration:")
            for var, value in validation['config_values'].items():
                print(f"  {var}: {value}")
            
            sys.exit(0 if validation['valid'] else 1)
        
        elif command == 'template':
            env_manager.generate_env_template()
            sys.exit(0)
        
        elif command == 'setup':
            env_manager.setup_wizard()
            sys.exit(0)
        
        else:
            print(f"Unknown command: {command}")
            print("Available commands: validate, template, setup")
            sys.exit(1)
    
    else:
        print("🔧 Environment Manager")
        print("=" * 30)
        print("Commands:")
        print("  validate  - Validate current environment")
        print("  template  - Generate .env template")
        print("  setup     - Run interactive setup wizard")

if __name__ == "__main__":
    main()