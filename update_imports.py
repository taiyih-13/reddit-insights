#!/usr/bin/env python3
"""
Import Update Script
Updates all imports to use the new base architecture
"""

import os
import re
from pathlib import Path

class ImportUpdater:
    """Updates imports across all Python files to use new architecture"""
    
    def __init__(self):
        # Define import mappings from old to new
        self.import_mappings = {
            # Old extractor imports
            'from balanced_extractor import': 'from base_extractor import BaseRedditExtractor',
            'from comprehensive_extractor import': 'from base_extractor import BaseRedditExtractor',
            'from entertainment_balanced_extractor import': 'from entertainment_extractor_new import EntertainmentRedditExtractor',
            'from entertainment_balanced_extractor_v2 import': 'from entertainment_extractor_new import EntertainmentRedditExtractor',
            'from entertainment_extractor import': 'from entertainment_extractor_new import EntertainmentRedditExtractor',
            
            # Old classifier imports
            'from post_classifier import': 'from base_classifier import BaseClassifier',
            
            # Old pipeline imports
            'from update_pipeline import': 'from base_pipeline import BasePipeline',
            'from entertainment_update_pipeline import': 'from base_pipeline import BasePipeline',
            'from unified_update_pipeline import': 'from ultimate_pipeline import UltimatePipeline',
            
            # Version-specific imports to clean up
            'from entertainment_classifier_v2 import': 'from entertainment_classifier import',
            'from entertainment_extractor_v3 import': 'from entertainment_extractor_new import',
            'from unified_pipeline_v2 import': 'from unified_pipeline import',
        }
        
        # Class name mappings
        self.class_mappings = {
            'BalancedExtractor': 'BaseRedditExtractor',
            'ComprehensiveRedditExtractor': 'BaseRedditExtractor', 
            'EntertainmentBalancedExtractor': 'EntertainmentRedditExtractor',
            'PostClassifier': 'BaseClassifier',
            'UpdatePipeline': 'BasePipeline',
            'EntertainmentUpdatePipeline': 'BasePipeline',
            'UnifiedUpdatePipeline': 'UltimatePipeline',
        }
        
        # Files to scan for imports
        self.python_files = []
        self.scan_directory()
    
    def scan_directory(self):
        """Scan for Python files to update"""
        for file_path in Path('.').glob('*.py'):
            if file_path.name not in ['update_imports.py', 'cleanup_legacy_files.py']:
                self.python_files.append(str(file_path))
        
        print(f"📁 Found {len(self.python_files)} Python files to scan")
    
    def update_file_imports(self, file_path):
        """Update imports in a single file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            updates_made = []
            
            # Update import statements
            for old_import, new_import in self.import_mappings.items():
                if old_import in content:
                    content = content.replace(old_import, new_import)
                    updates_made.append(f"Import: {old_import} → {new_import}")
            
            # Update class names in the content
            for old_class, new_class in self.class_mappings.items():
                # Use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(old_class) + r'\b'
                if re.search(pattern, content):
                    content = re.sub(pattern, new_class, content)
                    updates_made.append(f"Class: {old_class} → {new_class}")
            
            # Write back if changes were made
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"✅ Updated {file_path}:")
                for update in updates_made:
                    print(f"    • {update}")
                return len(updates_made)
            else:
                print(f"⚪ No changes needed: {file_path}")
                return 0
                
        except Exception as e:
            print(f"❌ Error updating {file_path}: {e}")
            return 0
    
    def update_all_imports(self):
        """Update imports in all Python files"""
        print("🔄 Updating imports across all Python files...")
        print("=" * 50)
        
        total_updates = 0
        files_updated = 0
        
        for file_path in self.python_files:
            updates = self.update_file_imports(file_path)
            if updates > 0:
                files_updated += 1
                total_updates += updates
        
        print(f"\n📊 Update Summary:")
        print(f"  Files scanned: {len(self.python_files)}")
        print(f"  Files updated: {files_updated}")
        print(f"  Total changes: {total_updates}")
        
        return files_updated, total_updates
    
    def verify_imports(self):
        """Verify that updated imports are syntactically correct"""
        print("\n🔍 Verifying updated imports...")
        
        failed_files = []
        
        for file_path in self.python_files:
            try:
                with open(file_path, 'r') as f:
                    code = f.read()
                
                # Try to compile the code to check syntax
                compile(code, file_path, 'exec')
                print(f"✅ {file_path}: Syntax OK")
                
            except SyntaxError as e:
                failed_files.append((file_path, str(e)))
                print(f"❌ {file_path}: Syntax Error - {e}")
            except Exception as e:
                print(f"⚠️  {file_path}: Could not verify - {e}")
        
        if failed_files:
            print(f"\n⚠️  {len(failed_files)} files have syntax errors after update")
            return False
        else:
            print(f"\n✅ All files passed syntax verification")
            return True

def main():
    """Main import update function"""
    print("🔄 Import Update Script (Phase 3)")
    print("=" * 40)
    
    updater = ImportUpdater()
    
    # Update all imports
    files_updated, total_updates = updater.update_all_imports()
    
    if files_updated > 0:
        # Verify the updates
        verification_passed = updater.verify_imports()
        
        if verification_passed:
            print("\n🎯 Import updates completed successfully!")
        else:
            print("\n⚠️  Some files may need manual review")
    else:
        print("\n✅ All imports are already up to date!")

if __name__ == "__main__":
    main()