#!/usr/bin/env python3
"""
Reddit Insights Setup Script
Automated setup and deployment script for the Reddit Insights platform
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
import json
import time

class RedditInsightsSetup:
    """Complete setup and deployment manager"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.python_executable = sys.executable
        self.setup_log = []
        
        # Required Python packages
        self.requirements = [
            'praw>=7.7.0',
            'pandas>=1.5.0',
            'python-dotenv>=1.0.0',
            'groq>=0.4.0',
            'flask>=2.3.0',
            'flask-cors>=4.0.0',
            'psutil>=5.9.0',
            'requests>=2.31.0'
        ]
        
        # Optional packages for enhanced features
        self.optional_requirements = [
            'jupyter>=1.0.0',
            'matplotlib>=3.7.0',
            'seaborn>=0.12.0',
            'plotly>=5.15.0'
        ]
    
    def log_step(self, message, success=True):
        """Log setup step with timestamp"""
        timestamp = time.strftime('%H:%M:%S')
        status = "✅" if success else "❌"
        log_entry = f"[{timestamp}] {status} {message}"
        print(log_entry)
        self.setup_log.append(log_entry)
    
    def check_python_version(self):
        """Check if Python version is compatible"""
        self.log_step("Checking Python version...")
        
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            self.log_step(f"Python {version.major}.{version.minor} is too old. Python 3.8+ required.", False)
            return False
        
        self.log_step(f"Python {version.major}.{version.minor}.{version.micro} is compatible")
        return True
    
    def install_requirements(self, optional=False):
        """Install required Python packages"""
        packages = self.optional_requirements if optional else self.requirements
        package_type = "optional" if optional else "required"
        
        self.log_step(f"Installing {package_type} packages...")
        
        try:
            # Create requirements file
            req_file = f"{package_type}_requirements.txt"
            with open(req_file, 'w') as f:
                f.write('\\n'.join(packages))
            
            # Install packages
            result = subprocess.run([
                self.python_executable, '-m', 'pip', 'install', '-r', req_file
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log_step(f"Successfully installed {len(packages)} {package_type} packages")
                return True
            else:
                self.log_step(f"Failed to install {package_type} packages: {result.stderr}", False)
                return False
                
        except Exception as e:
            self.log_step(f"Error installing {package_type} packages: {e}", False)
            return False
        finally:
            # Clean up requirements file
            if os.path.exists(req_file):
                os.remove(req_file)
    
    def setup_environment(self):
        """Set up environment configuration"""
        self.log_step("Setting up environment configuration...")
        
        # Generate environment template if it doesn't exist
        if not os.path.exists('.env'):
            try:
                from environment_manager import EnvironmentManager
                env_manager = EnvironmentManager()
                env_manager.generate_env_template('.env.template')
                
                self.log_step("Generated .env template file")
                self.log_step("⚠️  Please edit .env with your API credentials before running the system")
                return True
                
            except Exception as e:
                self.log_step(f"Error setting up environment: {e}", False)
                return False
        else:
            self.log_step("Environment file already exists")
            return True
    
    def validate_system(self):
        """Validate system configuration and components"""
        self.log_step("Validating system components...")
        
        try:
            # Run system validation
            result = subprocess.run([
                self.python_executable, 'phase3_verification.py'
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                self.log_step("System validation passed")
                return True
            else:
                self.log_step("System validation failed - check phase3_verification.py output", False)
                print(result.stdout)
                return False
                
        except Exception as e:
            self.log_step(f"Error during system validation: {e}", False)
            return False
    
    def create_directories(self):
        """Create necessary directories"""
        self.log_step("Creating project directories...")
        
        directories = [
            'logs',
            'cache',
            'data',
            'backups',
            'exports'
        ]
        
        created_count = 0
        for directory in directories:
            dir_path = self.project_root / directory
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                created_count += 1
        
        if created_count > 0:
            self.log_step(f"Created {created_count} project directories")
        else:
            self.log_step("All project directories already exist")
        
        return True
    
    def run_initial_tests(self):
        """Run initial test suite"""
        self.log_step("Running initial test suite...")
        
        try:
            result = subprocess.run([
                self.python_executable, 'test_suite.py', 'quick'
            ], capture_output=True, text=True, cwd=self.project_root)
            
            if result.returncode == 0:
                self.log_step("Initial tests passed")
                return True
            else:
                self.log_step("Some initial tests failed - system may still be functional", False)
                return True  # Don't fail setup for test failures
                
        except Exception as e:
            self.log_step(f"Error running tests: {e}", False)
            return True  # Don't fail setup for test errors
    
    def generate_sample_config(self):
        """Generate sample configuration files"""
        self.log_step("Generating sample configuration...")
        
        try:
            # Generate default reddit_config.json if it doesn't exist
            config_file = self.project_root / 'reddit_config.json'
            if not config_file.exists():
                from config_manager import ConfigManager
                config_manager = ConfigManager()
                self.log_step("Generated default configuration file")
            else:
                self.log_step("Configuration file already exists")
            
            return True
            
        except Exception as e:
            self.log_step(f"Error generating configuration: {e}", False)
            return False
    
    def create_startup_scripts(self):
        """Create convenient startup scripts"""
        self.log_step("Creating startup scripts...")
        
        # Create run script for different platforms
        if os.name == 'nt':  # Windows
            script_content = f'''@echo off
echo Starting Reddit Insights Dashboard...
cd /d "{self.project_root}"
"{self.python_executable}" ultimate_pipeline.py
pause
'''
            script_file = 'run_reddit_insights.bat'
        else:  # Unix-like
            script_content = f'''#!/bin/bash
echo "Starting Reddit Insights Dashboard..."
cd "{self.project_root}"
"{self.python_executable}" ultimate_pipeline.py
'''
            script_file = 'run_reddit_insights.sh'
        
        try:
            with open(script_file, 'w') as f:
                f.write(script_content)
            
            # Make executable on Unix-like systems
            if os.name != 'nt':
                os.chmod(script_file, 0o755)
            
            self.log_step(f"Created startup script: {script_file}")
            return True
            
        except Exception as e:
            self.log_step(f"Error creating startup script: {e}", False)
            return False
    
    def show_completion_summary(self):
        """Show setup completion summary and next steps"""
        print("\\n" + "=" * 60)
        print("🎯 REDDIT INSIGHTS SETUP COMPLETE!")
        print("=" * 60)
        
        # Count successful steps
        successful_steps = len([log for log in self.setup_log if "✅" in log])
        total_steps = len(self.setup_log)
        
        print(f"\\n📊 Setup Summary:")
        print(f"  Steps completed: {successful_steps}/{total_steps}")
        
        # Show failed steps if any
        failed_steps = [log for log in self.setup_log if "❌" in log]
        if failed_steps:
            print(f"  ⚠️  Failed steps: {len(failed_steps)}")
            for step in failed_steps:
                print(f"    • {step}")
        
        print(f"\\n🚀 Next Steps:")
        print(f"  1. Edit .env file with your API credentials:")
        print(f"     • REDDIT_CLIENT_ID")
        print(f"     • REDDIT_CLIENT_SECRET") 
        print(f"     • REDDIT_USER_AGENT")
        print(f"     • GROQ_API_KEY (optional, for AI summaries)")
        
        print(f"\\n  2. Validate your configuration:")
        print(f"     python environment_manager.py validate")
        
        print(f"\\n  3. Run a test extraction:")
        print(f"     python ultimate_pipeline.py")
        
        print(f"\\n  4. Generate dashboard:")
        print(f"     python dashboard_enhanced.py")
        
        print(f"\\n📚 Documentation:")
        print(f"  • README_v2.md - Complete documentation")
        print(f"  • Use 'python environment_manager.py setup' for interactive config")
        print(f"  • Use 'python test_suite.py' for full testing")
        
        if os.name == 'nt':
            print(f"\\n💡 Windows users can double-click: run_reddit_insights.bat")
        else:
            print(f"\\n💡 Unix users can run: ./run_reddit_insights.sh")
        
        print("\\n" + "=" * 60)
    
    def run_full_setup(self):
        """Run complete setup process"""
        print("🚀 Reddit Insights Setup & Deployment")
        print("=" * 50)
        print("Setting up Reddit Insights v2.0 with base architecture...")
        print()
        
        setup_steps = [
            ("Python Version Check", self.check_python_version),
            ("Install Required Packages", lambda: self.install_requirements(False)),
            ("Create Project Directories", self.create_directories),
            ("Setup Environment Configuration", self.setup_environment),
            ("Generate Sample Configuration", self.generate_sample_config),
            ("Validate System Components", self.validate_system),
            ("Run Initial Tests", self.run_initial_tests),
            ("Create Startup Scripts", self.create_startup_scripts)
        ]
        
        for step_name, step_function in setup_steps:
            print(f"\\n📋 {step_name}:")
            success = step_function()
            
            if not success and step_name in ["Python Version Check", "Install Required Packages"]:
                print("\\n❌ Critical setup step failed. Cannot continue.")
                return False
        
        # Optionally install optional packages
        print("\\n📦 Optional Packages:")
        install_optional = input("Install optional packages for enhanced features? (y/N): ").lower().strip()
        if install_optional == 'y':
            self.install_requirements(True)
        
        self.show_completion_summary()
        return True

def main():
    """Main setup function"""
    setup = RedditInsightsSetup()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'validate':
            print("🔍 System Validation")
            success = setup.validate_system()
            sys.exit(0 if success else 1)
        
        elif command == 'test':
            print("🧪 Running Tests")
            success = setup.run_initial_tests()
            sys.exit(0 if success else 1)
        
        elif command == 'dirs':
            print("📁 Creating Directories")
            setup.create_directories()
            sys.exit(0)
        
        else:
            print(f"Unknown command: {command}")
            print("Available commands: validate, test, dirs")
            sys.exit(1)
    
    else:
        # Run full setup
        success = setup.run_full_setup()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()