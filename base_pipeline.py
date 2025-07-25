#!/usr/bin/env python3
"""
Base Pipeline Class
Provides shared functionality for data pipeline workflows
"""

import pandas as pd
import json
import os
from datetime import datetime
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

class BasePipeline(ABC):
    """
    Abstract base class for data pipelines
    Handles common pipeline operations like state management and data saving
    """
    
    def __init__(self):
        self.state_file = f"{self.domain_name}_pipeline_state.json"
        self.load_state()
    
    @property
    @abstractmethod
    def domain_name(self):
        """Domain name for file naming and identification"""
        pass
    
    @property
    @abstractmethod
    def extractor(self):
        """Reddit extractor instance - must be implemented by subclasses"""
        pass
    
    @property
    @abstractmethod
    def classifier(self):
        """Content classifier instance - must be implemented by subclasses"""
        pass
    
    def load_state(self):
        """Load pipeline state from JSON file"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
                print(f"📋 Loaded {self.domain_name} pipeline state from {self.state_file}")
            else:
                self.state = self._create_initial_state()
                print(f"📋 Created new {self.domain_name} pipeline state")
        except Exception as e:
            print(f"⚠️  Error loading state: {e}. Creating new state.")
            self.state = self._create_initial_state()
    
    def save_state(self):
        """Save pipeline state to JSON file"""
        try:
            self.state['last_updated'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            print(f"💾 Saved {self.domain_name} pipeline state to {self.state_file}")
        except Exception as e:
            print(f"❌ Error saving state: {e}")
    
    def _create_initial_state(self):
        """Create initial pipeline state"""
        return {
            'created_at': datetime.now().isoformat(),
            'last_updated': None,
            'extractions': {
                'weekly': {'count': 0, 'last_run': None, 'status': 'never_run'},
                'daily': {'count': 0, 'last_run': None, 'status': 'never_run'}
            },
            'statistics': {
                'total_posts_processed': 0,
                'successful_runs': 0,
                'failed_runs': 0
            }
        }
    
    def update_extraction_state(self, time_filter, status, post_count=0):
        """Update state for a specific extraction"""
        self.state['extractions'][time_filter] = {
            'count': post_count,
            'last_run': datetime.now().isoformat(),
            'status': status
        }
        
        if status == 'success':
            self.state['statistics']['successful_runs'] += 1
            self.state['statistics']['total_posts_processed'] += post_count
        else:
            self.state['statistics']['failed_runs'] += 1
        
        self.save_state()
    
    def get_output_filename(self, time_filter):
        """Get standardized output filename"""
        period_map = {'week': 'week', 'day': 'day'}
        period = period_map.get(time_filter, time_filter)
        return f"{period}_reddit_posts.csv"
    
    def save_posts_to_csv(self, df, time_filter):
        """
        Save posts to CSV file with standardized naming
        
        Args:
            df: DataFrame with posts
            time_filter: 'week' or 'day'
        """
        if df.empty:
            print(f"❌ No data to save for {time_filter} period")
            return False
        
        filename = self.get_output_filename(time_filter)
        
        try:
            # Ensure consistent column ordering
            standard_columns = [
                'title', 'subreddit', 'author', 'score', 'num_comments',
                'popularity_score', 'category', 'created_utc', 'url',
                'selftext', 'upvote_ratio', 'link_flair_text', 'post_id'
            ]
            
            # Only include columns that exist in the DataFrame
            columns_to_save = [col for col in standard_columns if col in df.columns]
            df_to_save = df[columns_to_save]
            
            df_to_save.to_csv(filename, index=False)
            print(f"💾 Saved {len(df)} posts to {filename}")
            return True
            
        except Exception as e:
            print(f"❌ Error saving to {filename}: {e}")
            return False
    
    def extract_and_classify_data(self, time_filter='week', base_limit=100):
        """
        Complete extraction and classification pipeline for a single time period
        
        Args:
            time_filter: 'week' or 'day'
            base_limit: Number of posts to extract per subreddit
            
        Returns:
            pandas.DataFrame: Processed posts or empty DataFrame on failure
        """
        print(f"\n🚀 Starting {self.domain_name} {time_filter} pipeline...")
        
        try:
            # Step 1: Extract and process posts
            df_processed = self.extractor.extract_and_process(time_filter, base_limit)
            
            if df_processed.empty:
                print(f"❌ No posts extracted for {time_filter} period")
                self.update_extraction_state(time_filter, 'failed_extraction')
                return pd.DataFrame()
            
            # Step 2: Classify posts
            df_classified = self.classifier.process_classification_pipeline(df_processed)
            
            if df_classified.empty:
                print(f"❌ No posts classified for {time_filter} period")
                self.update_extraction_state(time_filter, 'failed_classification')
                return pd.DataFrame()
            
            # Step 3: Save results
            if self.save_posts_to_csv(df_classified, time_filter):
                self.update_extraction_state(time_filter, 'success', len(df_classified))
                print(f"✅ {self.domain_name} {time_filter} pipeline completed successfully!")
                return df_classified
            else:
                self.update_extraction_state(time_filter, 'failed_save')
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Pipeline error for {time_filter}: {e}")
            self.update_extraction_state(time_filter, 'error')
            return pd.DataFrame()
    
    def extract_both_periods(self, base_limit=100, parallel=True):
        """
        Extract data for both weekly and daily periods
        
        Args:
            base_limit: Number of posts to extract per subreddit
            parallel: Whether to run extractions in parallel
            
        Returns:
            dict: Results for both periods
        """
        print(f"\n🔄 Starting {self.domain_name} dual-period extraction...")
        
        if parallel:
            return self._extract_parallel(base_limit)
        else:
            return self._extract_sequential(base_limit)
    
    def _extract_parallel(self, base_limit):
        """Run weekly and daily extractions in parallel"""
        print("⚡ Running parallel extraction...")
        
        results = {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_period = {
                executor.submit(self.extract_and_classify_data, 'week', base_limit): 'weekly',
                executor.submit(self.extract_and_classify_data, 'day', base_limit): 'daily'
            }
            
            for future in future_to_period:
                period = future_to_period[future]
                try:
                    result = future.result()
                    results[period] = result
                    print(f"✅ {period.capitalize()} extraction completed")
                except Exception as e:
                    print(f"❌ {period.capitalize()} extraction failed: {e}")
        
        return results
    
    def _extract_sequential(self, base_limit):
        """Run weekly and daily extractions sequentially"""
        print("🔄 Running sequential extraction...")
        
        weekly_result = self.extract_and_classify_data('week', base_limit)
        daily_result = self.extract_and_classify_data('day', base_limit)
        
        return {'weekly': weekly_result, 'daily': daily_result}
    
    def get_pipeline_status(self):
        """Get current pipeline status and statistics"""
        print(f"\n📊 {self.domain_name.title()} Pipeline Status")
        print("=" * 50)
        
        for period, data in self.state['extractions'].items():
            status_emoji = {
                'success': '✅',
                'failed_extraction': '❌',
                'failed_classification': '❌',
                'failed_save': '❌',
                'error': '💥',
                'never_run': '⏳'
            }.get(data['status'], '❓')
            
            print(f"{period.title()}: {status_emoji} {data['status']} ({data['count']} posts)")
            if data['last_run']:
                print(f"  Last run: {data['last_run']}")
        
        stats = self.state['statistics']
        print(f"\nStatistics:")
        print(f"  Total posts processed: {stats['total_posts_processed']}")
        print(f"  Successful runs: {stats['successful_runs']}")
        print(f"  Failed runs: {stats['failed_runs']}")
        
        if stats['successful_runs'] + stats['failed_runs'] > 0:
            success_rate = stats['successful_runs'] / (stats['successful_runs'] + stats['failed_runs']) * 100
            print(f"  Success rate: {success_rate:.1f}%")
    
    def reset_pipeline_state(self):
        """Reset pipeline state to initial values"""
        print(f"🔄 Resetting {self.domain_name} pipeline state...")
        self.state = self._create_initial_state()
        self.save_state()
        print("✅ Pipeline state reset complete")