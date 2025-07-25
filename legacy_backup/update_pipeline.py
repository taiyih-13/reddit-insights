import pandas as pd
import os
import json
from datetime import datetime, timedelta
from balanced_extractor import BalancedExtractor
from dashboard_generator_clean import CleanRedditDashboard as RedditDashboard
import time

class RealTimeUpdater:
    def __init__(self, time_filter='week'):
        self.time_filter = time_filter
        self.data_file = f'{time_filter}_reddit_posts.csv'
        self.state_file = f'pipeline_state_{time_filter}.json'
        self.extractor = BalancedExtractor()
        
        # Update frequencies (in hours)
        self.automated_extraction_interval = 6  # 6 hours
        self.full_refresh_interval = 24 # 24 hours (daily cleanup)
        
        # Data retention based on time filter
        if time_filter == 'day':
            self.retention_days = 1  # Keep 1 day for daily pipeline
        else:
            self.retention_days = 7  # Keep 7 days for weekly pipeline
        
    def get_pipeline_state(self):
        """Load pipeline state from file"""
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
                'total_updates': 0
            }
    
    def save_pipeline_state(self, state):
        """Save pipeline state to file"""
        # Convert datetime to string for JSON serialization
        state_copy = state.copy()
        for key in ['last_automated_extraction', 'last_full_refresh']:
            if key in state_copy:
                state_copy[key] = state_copy[key].isoformat()
        
        with open(self.state_file, 'w') as f:
            json.dump(state_copy, f, indent=2)
    
    def _update_state(self, key, value):
        """Helper method to update a single state value"""
        state = self.get_pipeline_state()
        state[key] = value
        self.save_pipeline_state(state)
    
    def automated_extraction_system(self):
        """Run comprehensive automated extraction using standard balanced extraction"""
        print("Running automated extraction system...")
        
        try:
            # Use the same proven extraction logic as manual process
            print("   Extracting new posts using standard balanced extraction...")
            new_df = self.extractor.extract_balanced_posts(time_filter=self.time_filter, base_limit=100)
            
            if len(new_df) > 0:
                print(f"   Extracted {len(new_df)} quality posts")
                
                # Incremental update instead of complete replacement
                self._update_data_incrementally(new_df)
                
                # Update state tracking
                self._update_state('last_automated_extraction', datetime.now())
                
                print(f"   ✅ Data updated successfully")
                print(f"   📊 Category breakdown:")
                
                # Show category breakdown
                category_counts = new_df['category'].value_counts()
                for category, count in category_counts.items():
                    percentage = (count / len(new_df)) * 100
                    print(f"      • {category}: {count} posts ({percentage:.1f}%)")
                
                return True
            else:
                print("   No new posts met quality standards")
                return False
                
        except Exception as e:
            print(f"   Error in automated extraction: {e}")
            return False
    
    def rebalance_categories(self):
        """Rebalance categories if needed"""
        print("⚖️  Rebalancing categories...")
        
        try:
            # Load current data
            if not os.path.exists(self.data_file):
                print("   No existing data found, running full refresh")
                return self.full_refresh()
            
            df = pd.read_csv(self.data_file)
            
            # Check category distribution
            current_counts = df['category'].value_counts()
            targets = self.extractor.category_minimums
            
            needs_rebalance = False
            for category, target in targets.items():
                current = current_counts.get(category, 0)
                if current < target * 0.8:  # If below 80% of target
                    needs_rebalance = True
                    print(f"   {category}: {current}/{target} (needs more)")
            
            if needs_rebalance:
                print("   Running category rebalancing...")
                # Use the same time filter as the original extraction
                balanced_df = self.extractor.extract_balanced_posts(time_filter=self.time_filter, base_limit=25)
                self._save_data(balanced_df)
                print("   ✅ Categories rebalanced!")
                return True
            else:
                print("   Categories already balanced")
                return False
                
        except Exception as e:
            print(f"   Error rebalancing: {e}")
            return False
    
    def full_refresh(self):
        """Complete pipeline refresh"""
        print("🔄 Running full pipeline refresh...")
        
        try:
            # Run complete balanced extraction
            balanced_df = self.extractor.extract_balanced_posts(time_filter=self.time_filter, base_limit=50)
            
            if len(balanced_df) > 0:
                # Clean old data (keep only last 7 days)
                cutoff_date = datetime.now() - timedelta(days=self.retention_days)
                balanced_df['created_utc'] = pd.to_datetime(balanced_df['created_utc'])
                balanced_df = balanced_df[balanced_df['created_utc'] >= cutoff_date]
                
                # For full refresh, we do complete replacement with cleaned data
                self._save_data(balanced_df, full_refresh=True)
                print(f"   ✅ Full refresh complete! {len(balanced_df)} posts")
                return True
            else:
                print("   ❌ No data extracted")
                return False
                
        except Exception as e:
            print(f"   Error in full refresh: {e}")
            return False
    
    def _add_new_posts(self, new_posts_list):
        """Add new posts to existing data"""
        # Convert to DataFrame and process
        df_new = pd.DataFrame(new_posts_list)
        
        # Score and classify new posts
        df_scored = self.extractor.ranker.calculate_popularity_score(df_new)
        df_classified = self.extractor.classifier.classify_dataframe(df_scored)
        
        # Load existing data
        if os.path.exists(self.data_file):
            df_existing = pd.read_csv(self.data_file)
            
            # Remove duplicates and combine
            df_combined = pd.concat([df_existing, df_classified], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['post_id'], keep='last')
        else:
            df_combined = df_classified
        
        self._save_data(df_combined)
    
    def _update_data_incrementally(self, new_df):
        """Incrementally update data with new posts while maintaining retention policy"""
        print("   📊 Updating data incrementally...")
        
        # Load existing data if it exists
        if os.path.exists(self.data_file):
            try:
                existing_df = pd.read_csv(self.data_file)
                existing_df['created_utc'] = pd.to_datetime(existing_df['created_utc'])
                print(f"   📄 Found existing data: {len(existing_df)} posts")
            except Exception as e:
                print(f"   ⚠️  Error loading existing data: {e}")
                existing_df = pd.DataFrame()
        else:
            print("   📄 No existing data found, creating new dataset")
            existing_df = pd.DataFrame()
        
        # Ensure new data has datetime format
        new_df['created_utc'] = pd.to_datetime(new_df['created_utc'])
        
        # Combine existing and new data
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"   🔄 Combined data: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total")
        else:
            combined_df = new_df.copy()
            print(f"   🆕 Using new data: {len(combined_df)} posts")
        
        # Remove duplicates (keep most recent version)
        combined_df = combined_df.drop_duplicates(subset=['post_id'], keep='last')
        dedupe_count = len(pd.concat([existing_df, new_df], ignore_index=True)) - len(combined_df) if not existing_df.empty else 0
        if dedupe_count > 0:
            print(f"   🔍 Removed {dedupe_count} duplicate posts")
        
        # Apply retention policy (keep posts within retention window)
        cutoff_date = datetime.now() - timedelta(days=self.retention_days)
        before_retention = len(combined_df)
        combined_df = combined_df[combined_df['created_utc'] >= cutoff_date]
        retention_removed = before_retention - len(combined_df)
        if retention_removed > 0:
            print(f"   🗑️  Removed {retention_removed} posts older than {self.retention_days} days")
        
        # Save the updated data
        self._save_data(combined_df, full_refresh=False)
        print(f"   ✅ Incremental update complete: {len(combined_df)} posts in dataset")
    
    def _save_data(self, df, full_refresh=False):
        """Save data and regenerate unified dashboard"""
        # Save CSV
        df.to_csv(self.data_file, index=False)
        
        if full_refresh:
            print(f"   💾 Full refresh: Saved {len(df)} posts to {self.data_file}")
        else:
            print(f"   💾 Incremental update: Saved {len(df)} posts to {self.data_file}")
        
        # Regenerate unified dashboard - ensure both files exist for unified view
        try:
            from dashboard_generator_clean import CleanRedditDashboard
            
            # Check which CSV files actually exist
            weekly_exists = os.path.exists('week_reddit_posts.csv')
            daily_exists = os.path.exists('day_reddit_posts.csv')
            
            if weekly_exists or daily_exists:
                dashboard = CleanRedditDashboard(
                    weekly_csv='week_reddit_posts.csv' if weekly_exists else None,
                    daily_csv='day_reddit_posts.csv' if daily_exists else None
                )
                # Always generate the main dashboard file
                dashboard.generate_dashboard('reddit_dashboard.html')
                
                file_status = []
                if weekly_exists: file_status.append("weekly")
                if daily_exists: file_status.append("daily")
                print(f"   📊 Unified dashboard updated with {' and '.join(file_status)} data: reddit_dashboard.html")
            else:
                print(f"   ⚠️  No CSV files found for dashboard generation")
                
        except Exception as e:
            print(f"   ⚠️  Dashboard update failed: {e}")
    
    def run_update_cycle(self):
        """Run one complete update cycle"""
        print(f"\n{'='*60}")
        print(f"REDDIT PIPELINE UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        
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
        
        # Update state
        if updated:
            state['total_updates'] += 1
            
        self.save_pipeline_state(state)
        
        # Summary
        if updated:
            print(f"✅ Update complete! Total updates: {state['total_updates']}")
        else:
            print("⏭️  No updates needed at this time")
        
        # Show next update times
        next_extraction = state['last_automated_extraction'] + timedelta(hours=self.automated_extraction_interval)
        next_full = state['last_full_refresh'] + timedelta(hours=self.full_refresh_interval)
        
        print(f"\n📅 Next scheduled updates:")
        print(f"   Automated extraction: {next_extraction.strftime('%m/%d %H:%M')}")
        print(f"   Full refresh: {next_full.strftime('%m/%d %H:%M')}")
        
        return updated

def run_continuous_updates(time_filter='week'):
    """Run continuous update loop"""
    updater = RealTimeUpdater(time_filter)
    
    print("🚀 Starting Reddit Pipeline Real-Time Updates...")
    print("📊 Dashboard will be automatically updated at: reddit_dashboard.html")
    print("⏹️  Press Ctrl+C to stop\n")
    
    try:
        while True:
            updater.run_update_cycle()
            
            # Wait 10 minutes before checking again
            print(f"\n💤 Sleeping for 10 minutes...")
            time.sleep(600)  # 10 minutes
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Updates stopped by user")
        print(f"📊 Dashboard remains available at: reddit_dashboard.html")

def run_single_update(time_filter='week'):
    """Run a single update cycle"""
    updater = RealTimeUpdater(time_filter)
    updater.run_update_cycle()

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
    
    print(f"Using {time_filter} time filter")
    
    if continuous:
        run_continuous_updates(time_filter)
    else:
        run_single_update(time_filter)