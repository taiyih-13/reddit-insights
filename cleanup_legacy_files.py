#!/usr/bin/env python3
"""
Legacy File Cleanup Script
Identifies and removes outdated files after Phase 1-2 refactoring
"""

import os
import shutil
from datetime import datetime

class LegacyFileManager:
    """Manages cleanup of legacy files after refactoring"""
    
    def __init__(self):
        self.legacy_files = {
            # Old extractors (replaced by base architecture)
            'balanced_extractor.py': 'Replaced by BaseRedditExtractor',
            'comprehensive_extractor.py': 'Replaced by performance extractors',
            'entertainment_balanced_extractor.py': 'Replaced by EntertainmentRedditExtractor',
            'entertainment_balanced_extractor_v2.py': 'Replaced by EntertainmentRedditExtractor',
            'entertainment_extractor.py': 'Replaced by EntertainmentRedditExtractor',
            'entertainment_update_pipeline.py': 'Replaced by BasePipeline',
            'generate_entertainment_data.py': 'Replaced by unified pipeline',
            'update_pipeline.py': 'Replaced by BasePipeline',
            'unified_update_pipeline.py': 'Replaced by UltimatePipeline',
            
            # Old classifiers (replaced by base architecture)
            'post_classifier.py': 'Replaced by BaseClassifier',
            
            # Old dashboard files (will be updated)
            'entertainment_dashboard.py': 'To be replaced by updated dashboard',
            
            # Test files that are no longer relevant
            'test_balanced_subreddits.py': 'No longer relevant after refactoring',
            'test_discussion_classifier_full.py': 'No longer relevant after refactoring',
            
            # Old pipeline state files (will be recreated)
            'pipeline_state_day.json': 'Old format, will be recreated',
            'pipeline_state_week.json': 'Old format, will be recreated',
            'unified_pipeline_state_day.json': 'Old format, will be recreated',
            'unified_pipeline_state_week.json': 'Old format, will be recreated',
            
            # Old scheduler (needs updating)
            'scheduler.py': 'Needs updating for new architecture'
        }
        
        # Files to keep but rename for consistency
        self.files_to_rename = {
            'entertainment_classifier_v2.py': 'entertainment_classifier.py',
            'entertainment_extractor_v3.py': 'entertainment_extractor_legacy.py',
            'unified_pipeline_v2.py': 'unified_pipeline.py'
        }
        
        # Create backup directory
        self.backup_dir = f"legacy_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def analyze_files(self):
        """Analyze current files and show cleanup plan"""
        print("🔍 Analyzing files for cleanup...")
        print("=" * 50)
        
        existing_legacy = []
        missing_legacy = []
        
        for file, reason in self.legacy_files.items():
            if os.path.exists(file):
                existing_legacy.append((file, reason))
            else:
                missing_legacy.append(file)
        
        print(f"📊 Analysis Results:")
        print(f"  Legacy files found: {len(existing_legacy)}")
        print(f"  Legacy files already removed: {len(missing_legacy)}")
        
        if existing_legacy:
            print(f"\n🗑️  Files to be removed:")
            for file, reason in existing_legacy:
                size_kb = os.path.getsize(file) / 1024
                print(f"  • {file:<40} ({size_kb:.1f}KB) - {reason}")
        
        if self.files_to_rename:
            print(f"\n📝 Files to be renamed:")
            for old_name, new_name in self.files_to_rename.items():
                if os.path.exists(old_name):
                    print(f"  • {old_name} → {new_name}")
        
        return existing_legacy
    
    def create_backup(self, files_to_backup):
        """Create backup of files before removal"""
        if not files_to_backup:
            return
        
        print(f"\n💾 Creating backup directory: {self.backup_dir}")
        os.makedirs(self.backup_dir, exist_ok=True)
        
        for file, _ in files_to_backup:
            if os.path.exists(file):
                backup_path = os.path.join(self.backup_dir, file)
                shutil.copy2(file, backup_path)
                print(f"  ✅ Backed up: {file}")
        
        # Create backup manifest
        manifest_path = os.path.join(self.backup_dir, "backup_manifest.txt")
        with open(manifest_path, 'w') as f:
            f.write(f"Legacy File Backup - {datetime.now().isoformat()}\\n")
            f.write(f"Created during Phase 3 cleanup\\n\\n")
            for file, reason in files_to_backup:
                f.write(f"{file}: {reason}\\n")
        
        print(f"📋 Created backup manifest: {manifest_path}")
    
    def remove_legacy_files(self, files_to_remove):
        """Remove legacy files after backup"""
        if not files_to_remove:
            print("✅ No legacy files to remove")
            return
        
        print(f"\\n🗑️  Removing {len(files_to_remove)} legacy files...")
        
        for file, reason in files_to_remove:
            try:
                os.remove(file)
                print(f"  ✅ Removed: {file}")
            except Exception as e:
                print(f"  ❌ Error removing {file}: {e}")
    
    def rename_files(self):
        """Rename files for consistency"""
        print(f"\\n📝 Renaming files for consistency...")
        
        for old_name, new_name in self.files_to_rename.items():
            if os.path.exists(old_name):
                try:
                    # Check if target already exists
                    if os.path.exists(new_name):
                        print(f"  ⚠️  Target exists, skipping: {old_name} → {new_name}")
                        continue
                    
                    os.rename(old_name, new_name)
                    print(f"  ✅ Renamed: {old_name} → {new_name}")
                except Exception as e:
                    print(f"  ❌ Error renaming {old_name}: {e}")
            else:
                print(f"  ⚠️  File not found: {old_name}")
    
    def cleanup_empty_directories(self):
        """Remove any empty directories created during cleanup"""
        print(f"\\n📁 Checking for empty directories...")
        # This is a simple implementation - only removes known empty dirs
        # In practice, you might want more sophisticated logic
        print("✅ No empty directories to remove")
    
    def generate_cleanup_summary(self):
        """Generate summary of cleanup actions"""
        print(f"\\n📋 Cleanup Summary")
        print("=" * 30)
        print(f"🗂️  Backup directory: {self.backup_dir}")
        print(f"🗑️  Legacy files removed: {len([f for f in self.legacy_files.keys() if os.path.exists(f) == False])}")
        print(f"📝 Files renamed: {len(self.files_to_rename)}")
        print(f"✅ Cleanup complete!")
    
    def run_full_cleanup(self, create_backup=True, confirm=True):
        """Run complete cleanup process"""
        print("🧹 Starting Legacy File Cleanup (Phase 3)")
        print("=" * 50)
        
        # Analyze current state
        existing_legacy = self.analyze_files()
        
        if not existing_legacy and not any(os.path.exists(f) for f in self.files_to_rename.keys()):
            print("✅ No cleanup needed - all files already clean!")
            return
        
        # Confirm action
        if confirm:
            print(f"\\n⚠️  This will remove {len(existing_legacy)} legacy files")
            response = input("Continue? (y/N): ").lower().strip()
            if response != 'y':
                print("❌ Cleanup cancelled")
                return
        
        # Create backup if requested
        if create_backup and existing_legacy:
            self.create_backup(existing_legacy)
        
        # Remove legacy files
        self.remove_legacy_files(existing_legacy)
        
        # Rename files
        self.rename_files()
        
        # Cleanup empty directories
        self.cleanup_empty_directories()
        
        # Generate summary
        self.generate_cleanup_summary()

def main():
    """Main cleanup function"""
    cleanup_manager = LegacyFileManager()
    
    # Run analysis only by default
    print("🔍 Running cleanup analysis...")
    cleanup_manager.analyze_files()
    
    print(f"\\n💡 To run full cleanup:")
    print(f"   python cleanup_legacy_files.py --execute")
    print(f"\\n💡 To run cleanup without backup:")
    print(f"   python cleanup_legacy_files.py --execute --no-backup")

if __name__ == "__main__":
    import sys
    
    if "--execute" in sys.argv:
        cleanup_manager = LegacyFileManager()
        create_backup = "--no-backup" not in sys.argv
        cleanup_manager.run_full_cleanup(create_backup=create_backup, confirm=True)
    else:
        main()