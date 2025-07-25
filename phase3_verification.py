#!/usr/bin/env python3
"""
Phase 3 Verification Script
Verifies that all consolidation work is complete and working
"""

import os
import sys
import importlib
from pathlib import Path

class Phase3Verifier:
    """Verifies Phase 3 consolidation is complete"""
    
    def __init__(self):
        self.results = {
            'imports': {'passed': 0, 'failed': 0, 'details': []},
            'architecture': {'passed': 0, 'failed': 0, 'details': []},
            'files': {'passed': 0, 'failed': 0, 'details': []},
            'integration': {'passed': 0, 'failed': 0, 'details': []}
        }
    
    def verify_imports(self):
        """Verify all imports are working correctly"""
        print("🔍 Verifying Import Structure...")
        
        # Core imports to test
        test_imports = [
            ('base_extractor', 'BaseRedditExtractor'),
            ('base_classifier', 'BaseClassifier'),
            ('base_pipeline', 'BasePipeline'),
            ('config_manager', 'ConfigManager'),
            ('finance_extractor', 'FinanceRedditExtractor'),
            ('finance_classifier', 'FinanceClassifier'),
            ('entertainment_extractor_new', 'EntertainmentRedditExtractor'),
            ('entertainment_classifier', 'EntertainmentClassifier'),
            ('ultimate_pipeline', 'UltimatePipeline'),
            ('performance_extractor', 'PerformanceRedditExtractor'),
            ('resilient_extractor', 'ResilientRedditExtractor'),
        ]
        
        for module_name, class_name in test_imports:
            try:
                module = importlib.import_module(module_name)
                if hasattr(module, class_name):
                    self.results['imports']['passed'] += 1
                    self.results['imports']['details'].append(f"✅ {module_name}.{class_name}")
                else:
                    self.results['imports']['failed'] += 1
                    self.results['imports']['details'].append(f"❌ {module_name}.{class_name} - Class not found")
            except ImportError as e:
                self.results['imports']['failed'] += 1
                self.results['imports']['details'].append(f"❌ {module_name} - Import error: {e}")
            except Exception as e:
                self.results['imports']['failed'] += 1
                self.results['imports']['details'].append(f"❌ {module_name} - Error: {e}")
    
    def verify_architecture(self):
        """Verify base architecture is properly implemented"""
        print("🏗️  Verifying Base Architecture...")
        
        # Test base class inheritance
        try:
            from base_extractor import BaseRedditExtractor
            from finance_extractor import FinanceRedditExtractor
            from entertainment_extractor_new import EntertainmentRedditExtractor
            
            # Verify inheritance
            if issubclass(FinanceRedditExtractor, BaseRedditExtractor):
                self.results['architecture']['passed'] += 1
                self.results['architecture']['details'].append("✅ FinanceRedditExtractor inherits BaseRedditExtractor")
            else:
                self.results['architecture']['failed'] += 1
                self.results['architecture']['details'].append("❌ FinanceRedditExtractor inheritance broken")
            
            if issubclass(EntertainmentRedditExtractor, BaseRedditExtractor):
                self.results['architecture']['passed'] += 1
                self.results['architecture']['details'].append("✅ EntertainmentRedditExtractor inherits BaseRedditExtractor")
            else:
                self.results['architecture']['failed'] += 1
                self.results['architecture']['details'].append("❌ EntertainmentRedditExtractor inheritance broken")
            
        except Exception as e:
            self.results['architecture']['failed'] += 1
            self.results['architecture']['details'].append(f"❌ Architecture test failed: {e}")
        
        # Test configuration system
        try:
            from config_manager import ConfigManager
            config = ConfigManager()
            
            # Test domain configs
            finance_config = config.get_domain_config('finance')
            entertainment_config = config.get_domain_config('entertainment')
            
            if len(finance_config.subreddits) > 0:
                self.results['architecture']['passed'] += 1
                self.results['architecture']['details'].append(f"✅ Finance config: {len(finance_config.subreddits)} subreddits")
            else:
                self.results['architecture']['failed'] += 1
                self.results['architecture']['details'].append("❌ Finance config empty")
            
            if len(entertainment_config.subreddits) > 0:
                self.results['architecture']['passed'] += 1
                self.results['architecture']['details'].append(f"✅ Entertainment config: {len(entertainment_config.subreddits)} subreddits")
            else:
                self.results['architecture']['failed'] += 1
                self.results['architecture']['details'].append("❌ Entertainment config empty")
                
        except Exception as e:
            self.results['architecture']['failed'] += 1
            self.results['architecture']['details'].append(f"❌ Configuration test failed: {e}")
    
    def verify_file_structure(self):
        """Verify file structure is clean"""
        print("📁 Verifying File Structure...")
        
        # Check for legacy files that should be removed
        legacy_files = [
            'balanced_extractor.py',
            'comprehensive_extractor.py',
            'entertainment_balanced_extractor.py',
            'entertainment_balanced_extractor_v2.py',
            'post_classifier.py',
            'update_pipeline.py',
            'unified_update_pipeline.py',
            'scheduler.py'
        ]
        
        removed_count = 0
        for file in legacy_files:
            if not os.path.exists(file):
                removed_count += 1
            else:
                self.results['files']['failed'] += 1
                self.results['files']['details'].append(f"❌ Legacy file still exists: {file}")
        
        if removed_count == len(legacy_files):
            self.results['files']['passed'] += 1
            self.results['files']['details'].append(f"✅ All {removed_count} legacy files removed")
        
        # Check for required new files
        required_files = [
            'base_extractor.py',
            'base_classifier.py', 
            'base_pipeline.py',
            'config_manager.py',
            'performance_extractor.py',
            'resilient_extractor.py',
            'ultimate_pipeline.py'
        ]
        
        existing_count = 0
        for file in required_files:
            if os.path.exists(file):
                existing_count += 1
            else:
                self.results['files']['failed'] += 1
                self.results['files']['details'].append(f"❌ Required file missing: {file}")
        
        if existing_count == len(required_files):
            self.results['files']['passed'] += 1
            self.results['files']['details'].append(f"✅ All {existing_count} required files present")
        
        # Check backup directory
        if os.path.exists('legacy_backup'):
            self.results['files']['passed'] += 1
            self.results['files']['details'].append("✅ Legacy backup directory created")
        else:
            self.results['files']['details'].append("⚠️  No legacy backup directory found")
    
    def verify_integration(self):
        """Verify components work together"""
        print("🔗 Verifying Integration...")
        
        try:
            # Test ultimate pipeline initialization
            from ultimate_pipeline import UltimatePipeline
            
            pipeline = UltimatePipeline(max_workers=2)  # Small test
            
            self.results['integration']['passed'] += 1
            self.results['integration']['details'].append("✅ UltimatePipeline initialization successful")
            
            # Test configuration integration
            from config_manager import ConfigManager
            config = ConfigManager()
            
            # Verify configs are loaded
            domains = config.config.get('domains', {})
            if 'finance' in domains and 'entertainment' in domains:
                self.results['integration']['passed'] += 1
                self.results['integration']['details'].append("✅ Multi-domain configuration working")
            else:
                self.results['integration']['failed'] += 1
                self.results['integration']['details'].append("❌ Domain configuration incomplete")
        
        except Exception as e:
            self.results['integration']['failed'] += 1
            self.results['integration']['details'].append(f"❌ Integration test failed: {e}")
        
        # Test AI summarizer integration
        try:
            from ai_summarizer_enhanced import EnhancedRedditSummarizer
            
            # Just test initialization (don't actually call API)
            summarizer = EnhancedRedditSummarizer()
            
            self.results['integration']['passed'] += 1
            self.results['integration']['details'].append("✅ Enhanced AI summarizer integration working")
            
        except Exception as e:
            self.results['integration']['failed'] += 1
            self.results['integration']['details'].append(f"❌ AI summarizer integration failed: {e}")
    
    def generate_report(self):
        """Generate comprehensive verification report"""
        print("\\n" + "=" * 60)
        print("📋 PHASE 3 CONSOLIDATION VERIFICATION REPORT")
        print("=" * 60)
        
        total_passed = 0
        total_failed = 0
        
        for category, results in self.results.items():
            passed = results['passed']
            failed = results['failed']
            total_passed += passed
            total_failed += failed
            
            status = "✅ PASS" if failed == 0 else "❌ FAIL"
            print(f"\\n{category.upper()} TESTS: {status} ({passed} passed, {failed} failed)")
            
            for detail in results['details']:
                print(f"  {detail}")
        
        print(f"\\n{'='*60}")
        print(f"OVERALL RESULT: {total_passed + total_failed} tests")
        print(f"✅ Passed: {total_passed}")
        print(f"❌ Failed: {total_failed}")
        
        if total_failed == 0:
            print("\\n🎯 Phase 3 consolidation COMPLETE! All systems operational.")
            return True
        else:
            print(f"\\n⚠️  Phase 3 consolidation has {total_failed} issues to resolve.")
            return False
    
    def run_full_verification(self):
        """Run complete Phase 3 verification"""
        print("🔍 Phase 3 Consolidation Verification")
        print("=" * 40)
        
        self.verify_imports()
        self.verify_architecture()
        self.verify_file_structure()
        self.verify_integration()
        
        return self.generate_report()

def main():
    """Main verification function"""
    verifier = Phase3Verifier()
    success = verifier.run_full_verification()
    
    if success:
        print("\\n🚀 Ready to proceed to Phase 4!")
        sys.exit(0)
    else:
        print("\\n🔧 Please resolve issues before continuing.")
        sys.exit(1)

if __name__ == "__main__":
    main()