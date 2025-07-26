import pandas as pd
import os
import json
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.finance_balanced_extractor import FinanceBalancedExtractor
from extractors.entertainment_balanced_extractor import EntertainmentBalancedExtractorV2
from utils.dashboard_generator import CleanRedditDashboard as RedditDashboard
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class UnifiedRealTimeUpdater:
    def __init__(self, time_filter='week'):
        self.time_filter = time_filter
        self.state_file = f'assets/unified_pipeline_state_{time_filter}.json'
        
        # Initialize extractors for both categories
        self.finance_extractor = FinanceBalancedExtractor()
        self.entertainment_extractor = EntertainmentBalancedExtractorV2()
        
        # Data files for both categories
        self.data_files = {
            'finance': f'assets/{time_filter}_reddit_posts.csv',
            'entertainment': f'assets/{time_filter}_entertainment_posts.csv'
        }
        
        # Update frequencies (in hours)
        self.automated_extraction_interval = 6  # 6 hours
        self.full_refresh_interval = 24 # 24 hours (daily cleanup)
        
        # Data retention based on time filter
        if time_filter == 'day':
            self.retention_days = 1  # Keep 1 day for daily pipeline
        else:
            self.retention_days = 7  # Keep 7 days for weekly pipeline
        
    def get_pipeline_state(self):
        """Load unified pipeline state from file"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                # Convert string timestamps back to datetime
                for key in ['last_automated_extraction', 'last_full_refresh']:
                    if key in state:
                        state[key] = datetime.fromisoformat(state[key])
                return state
        else:
            # Default state
            now = datetime.now()
            return {
                'last_automated_extraction': now - timedelta(hours=7),  # Force initial run
                'last_full_refresh': now - timedelta(hours=25),  # Force initial daily cleanup
                'total_updates': 0,
                'last_finance_update': None,
                'last_entertainment_update': None
            }
    
    def save_pipeline_state(self, state):
        """Save unified pipeline state to file"""
        # Convert datetime to string for JSON serialization
        state_copy = state.copy()
        for key in ['last_automated_extraction', 'last_full_refresh', 'last_finance_update', 'last_entertainment_update']:
            if key in state_copy and state_copy[key] is not None:
                state_copy[key] = state_copy[key].isoformat()
        
        with open(self.state_file, 'w') as f:
            json.dump(state_copy, f, indent=2)
    
    def _update_state(self, key, value):
        """Helper method to update a single state value"""
        state = self.get_pipeline_state()
        state[key] = value
        self.save_pipeline_state(state)
    
    def extract_finance_data(self):
        """Extract finance data using balanced extractor"""
        print("   🏦 Extracting finance posts using balanced extraction...")
        try:
            df = self.finance_extractor.extract_balanced_posts(
                time_filter=self.time_filter, 
                base_limit=100
            )
            if len(df) > 0:
                print(f"   ✅ Finance: Extracted {len(df)} quality posts")
                return df
            else:
                print("   ❌ Finance: No posts met quality standards")
                return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ Finance extraction error: {e}")
            return pd.DataFrame()
    
    def extract_entertainment_data(self):
        """Extract entertainment data using balanced extractor"""
        print("   🎬 Extracting entertainment posts using balanced extraction...")
        try:
            df = self.entertainment_extractor.extract_balanced_posts(
                time_filter=self.time_filter, 
                base_limit=100
            )
            if len(df) > 0:
                print(f"   ✅ Entertainment: Extracted {len(df)} quality posts")
                return df
            else:
                print("   ❌ Entertainment: No posts met quality standards")
                return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ Entertainment extraction error: {e}")
            return pd.DataFrame()
    
    def automated_extraction_system(self):
        """Run comprehensive automated extraction for both categories simultaneously"""
        print("🚀 Running unified automated extraction system...")
        
        results = {'finance': None, 'entertainment': None}
        success_count = 0
        
        # Use ThreadPoolExecutor to run both extractions in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both extraction tasks
            future_to_category = {
                executor.submit(self.extract_finance_data): 'finance',
                executor.submit(self.extract_entertainment_data): 'entertainment'
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    df = future.result()
                    results[category] = df
                    if len(df) > 0:
                        success_count += 1
                except Exception as e:
                    print(f"   Error in {category} extraction: {e}")
                    results[category] = pd.DataFrame()
        
        # Process results and update data files
        updated_categories = []
        
        for category, df in results.items():
            if df is not None and len(df) > 0:
                # Incremental update for this category
                self._update_data_incrementally(df, category)
                updated_categories.append(category)
                
                # Show category breakdown
                if category == 'finance':
                    print(f"   📊 Finance category breakdown:")
                    category_counts = df['category'].value_counts()
                    for cat, count in category_counts.items():
                        percentage = (count / len(df)) * 100
                        print(f"      • {cat}: {count} posts ({percentage:.1f}%)")
                
                elif category == 'entertainment':
                    print(f"   📊 Entertainment discussion-type breakdown:")
                    category_counts = df['category'].value_counts()
                    for cat, count in category_counts.items():
                        percentage = (count / len(df)) * 100
                        print(f"      • {cat}: {count} posts ({percentage:.1f}%)")
        
        if updated_categories:
            # Update state tracking
            now = datetime.now()
            state = self.get_pipeline_state()
            state['last_automated_extraction'] = now
            if 'finance' in updated_categories:
                state['last_finance_update'] = now
            if 'entertainment' in updated_categories:
                state['last_entertainment_update'] = now
            self.save_pipeline_state(state)
            
            print(f"   ✅ Updated categories: {', '.join(updated_categories)}")
            return True
        else:
            print("   ❌ No new posts met quality standards in either category")
            return False
    
    def full_refresh(self):
        """Complete pipeline refresh for both categories"""
        print("🔄 Running unified full pipeline refresh...")
        
        results = {'finance': None, 'entertainment': None}
        
        # Use ThreadPoolExecutor to run both extractions in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both extraction tasks
            future_to_category = {
                executor.submit(self.extract_finance_data): 'finance',
                executor.submit(self.extract_entertainment_data): 'entertainment'
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    df = future.result()
                    results[category] = df
                except Exception as e:
                    print(f"   Error in {category} full refresh: {e}")
                    results[category] = pd.DataFrame()
        
        # Process results
        updated_categories = []
        
        for category, df in results.items():
            if df is not None and len(df) > 0:
                # Clean old data (keep only last retention_days)
                cutoff_date = datetime.now() - timedelta(days=self.retention_days)
                df['created_utc'] = pd.to_datetime(df['created_utc'])
                df = df[df['created_utc'] >= cutoff_date]
                
                # For full refresh, we do complete replacement with cleaned data
                self._save_category_data(df, category, full_refresh=True)
                updated_categories.append(category)
                print(f"   ✅ {category.title()}: Full refresh complete! {len(df)} posts")
        
        if updated_categories:
            return True
        else:
            print("   ❌ No data extracted for either category")
            return False
    
    def _update_data_incrementally(self, new_df, category):
        """Incrementally update data with new posts while maintaining retention policy"""
        print(f"   📊 Updating {category} data incrementally...")
        
        data_file = self.data_files[category]
        
        # Load existing data if it exists
        if os.path.exists(data_file):
            try:
                existing_df = pd.read_csv(data_file)
                existing_df['created_utc'] = pd.to_datetime(existing_df['created_utc'])
                print(f"   📄 Found existing {category} data: {len(existing_df)} posts")
            except Exception as e:
                print(f"   ⚠️  Error loading existing {category} data: {e}")
                existing_df = pd.DataFrame()
        else:
            print(f"   📄 No existing {category} data found, creating new dataset")
            existing_df = pd.DataFrame()
        
        # Ensure new data has datetime format
        new_df['created_utc'] = pd.to_datetime(new_df['created_utc'])
        
        # Combine existing and new data
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"   🔄 Combined {category} data: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total")
        else:
            combined_df = new_df.copy()
            print(f"   🆕 Using new {category} data: {len(combined_df)} posts")
        
        # Remove duplicates (keep most recent version)
        combined_df = combined_df.drop_duplicates(subset=['post_id'], keep='last')
        dedupe_count = len(pd.concat([existing_df, new_df], ignore_index=True)) - len(combined_df) if not existing_df.empty else 0
        if dedupe_count > 0:
            print(f"   🔍 Removed {dedupe_count} duplicate {category} posts")
        
        # Apply retention policy (keep posts within retention window)
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        before_retention = len(combined_df)
        combined_df = combined_df[combined_df['created_utc'] >= cutoff_date]
        retention_removed = before_retention - len(combined_df)
        if retention_removed > 0:
            print(f"   🗑️  Removed {retention_removed} {category} posts older than {self.retention_days} days")
        
        # Save the updated data
        self._save_category_data(combined_df, category, full_refresh=False)
        print(f"   ✅ Incremental {category} update complete: {len(combined_df)} posts in dataset")
    
    def _save_category_data(self, df, category, full_refresh=False):
        """Save category data to CSV"""
        data_file = self.data_files[category]
        df.to_csv(data_file, index=False)
        
        if full_refresh:
            print(f"   💾 Full refresh: Saved {len(df)} {category} posts to {data_file}")
        else:
            print(f"   💾 Incremental update: Saved {len(df)} {category} posts to {data_file}")
    
    def regenerate_unified_dashboard(self):
        """Regenerate unified dashboard with both finance and entertainment data"""
        try:
            from dashboard_generator_clean import CleanRedditDashboard
            
            # Check which CSV files actually exist
            weekly_finance_exists = os.path.exists('assets/week_reddit_posts.csv')
            daily_finance_exists = os.path.exists('assets/day_reddit_posts.csv')
            weekly_entertainment_exists = os.path.exists('assets/week_entertainment_posts.csv')
            daily_entertainment_exists = os.path.exists('assets/day_entertainment_posts.csv')
            
            if weekly_finance_exists or daily_finance_exists or weekly_entertainment_exists or daily_entertainment_exists:
                dashboard = CleanRedditDashboard(
                    weekly_csv='assets/week_reddit_posts.csv' if weekly_finance_exists else None,
                    daily_csv='assets/day_reddit_posts.csv' if daily_finance_exists else None,
                    weekly_entertainment_csv='assets/week_entertainment_posts.csv' if weekly_entertainment_exists else None,
                    daily_entertainment_csv='assets/day_entertainment_posts.csv' if daily_entertainment_exists else None
                )
                # Always generate the main dashboard file
                dashboard.generate_dashboard('assets/reddit_dashboard.html')
                
                file_status = []
                if weekly_finance_exists: file_status.append("weekly finance")
                if daily_finance_exists: file_status.append("daily finance")  
                if weekly_entertainment_exists: file_status.append("weekly entertainment")
                if daily_entertainment_exists: file_status.append("daily entertainment")
                print(f"   📊 Unified dashboard updated with {' and '.join(file_status)} data: assets/reddit_dashboard.html")
                return True
            else:
                print(f"   ⚠️  No CSV files found for dashboard generation")
                return False
                
        except Exception as e:
            print(f"   ⚠️  Dashboard update failed: {e}")
            return False
    
    def run_update_cycle(self):
        """Run one complete unified update cycle"""
        print(f"\n{'='*80}")
        print(f"UNIFIED REDDIT PIPELINE UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        state = self.get_pipeline_state()
        now = datetime.now()
        updated = False
        
        # Check what updates are needed
        automated_extraction_due = (now - state['last_automated_extraction']).total_seconds() / 3600 >= self.automated_extraction_interval
        full_refresh_due = (now - state['last_full_refresh']).total_seconds() / 3600 >= self.full_refresh_interval
        
        # Prioritize full refresh if due (daily cleanup)
        if full_refresh_due:
            if self.full_refresh():
                state['last_full_refresh'] = now
                state['last_automated_extraction'] = now  # Full refresh includes extraction
                updated = True
        
        # Otherwise run automated extraction system
        elif automated_extraction_due:
            if self.automated_extraction_system():
                state['last_automated_extraction'] = now
                updated = True
        
        # Always regenerate dashboard if any updates occurred
        if updated:
            print(f"\n📊 Regenerating unified dashboard...")
            dashboard_updated = self.regenerate_unified_dashboard()
            if dashboard_updated:
                print(f"   ✅ Dashboard successfully updated!")
            
            # Update state
            state['total_updates'] += 1
            self.save_pipeline_state(state)
        
        # Summary
        if updated:
            print(f"✅ Unified update complete! Total updates: {state['total_updates']}")
        else:
            print("⏭️  No updates needed at this time")
        
        # Show next update times and last update info
        next_extraction = state['last_automated_extraction'] + timedelta(hours=self.automated_extraction_interval)
        next_full = state['last_full_refresh'] + timedelta(hours=self.full_refresh_interval)
        
        print(f"\n📅 Next scheduled updates:")
        print(f"   Automated extraction: {next_extraction.strftime('%m/%d %H:%M')}")
        print(f"   Full refresh: {next_full.strftime('%m/%d %H:%M')}")
        
        # Show last category updates
        if state.get('last_finance_update'):
            print(f"   Last finance update: {state['last_finance_update']}")
        if state.get('last_entertainment_update'):
            print(f"   Last entertainment update: {state['last_entertainment_update']}")
        
        return updated

def run_unified_update(time_filter='week', target_categories=None):
    """
    Run a unified update for both finance and entertainment
    
    Args:
        time_filter: 'week' or 'day'  
        target_categories: None for both, or list like ['finance'] or ['entertainment']
    """
    updater = UnifiedRealTimeUpdater(time_filter)
    
    if target_categories:
        print(f"🎯 Targeting specific categories: {', '.join(target_categories)}")
        # Note: For now, we always update both since extraction logic handles both
        # Future enhancement could allow selective updates
    
    return updater.run_update_cycle()

def run_continuous_unified_updates(time_filter='week'):
    """Run continuous unified update loop"""
    updater = UnifiedRealTimeUpdater(time_filter)
    
    print("🚀 Starting Unified Reddit Pipeline Real-Time Updates...")
    print("📊 Dashboard will be automatically updated at: assets/reddit_dashboard.html")
    print("🎯 Monitoring both Finance and Entertainment categories")
    print("⏹️  Press Ctrl+C to stop\n")
    
    try:
        while True:
            updater.run_update_cycle()
            
            # Wait 10 minutes before checking again
            print(f"\n💤 Sleeping for 10 minutes...")
            time.sleep(600)  # 10 minutes
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Unified updates stopped by user")
        print(f"📊 Dashboard remains available at: assets/reddit_dashboard.html")

if __name__ == "__main__":
    import sys
    
    # Parse arguments
    time_filter = 'week'  # default
    continuous = False
    
    for arg in sys.argv[1:]:
        if arg == "--continuous":
            continuous = True
        elif arg in ['--daily', '--day']:
            time_filter = 'day'
        elif arg in ['--weekly', '--week']:
            time_filter = 'week'
    
    print(f"Using {time_filter} time filter for unified pipeline")
    
    if continuous:
        run_continuous_unified_updates(time_filter)
    else:
        run_unified_update(time_filter)