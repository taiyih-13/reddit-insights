#!/usr/bin/env python3
"""
Performance-Enhanced Reddit Extractor
Implements parallel processing, caching, and optimized data operations
"""

import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from popularity_ranker_v2 import PopularityRankerV2

load_dotenv()

class RedditCache:
    """Thread-safe cache for Reddit data with TTL support"""
    
    def __init__(self, cache_file='reddit_cache.json', ttl_hours=1):
        self.cache_file = cache_file
        self.ttl_hours = ttl_hours
        self.cache = {}
        self.lock = Lock()
        self.load_cache()
    
    def load_cache(self):
        """Load cache from disk"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    data = json.load(f)
                    # Filter out expired entries
                    current_time = datetime.now()
                    self.cache = {
                        key: value for key, value in data.items()
                        if self._is_valid(value.get('timestamp'), current_time)
                    }
                print(f"📚 Loaded {len(self.cache)} cached entries")
        except Exception as e:
            print(f"⚠️  Cache load error: {e}")
            self.cache = {}
    
    def save_cache(self):
        """Save cache to disk"""
        try:
            with self.lock:
                with open(self.cache_file, 'w') as f:
                    json.dump(self.cache, f, default=str, indent=2)
        except Exception as e:
            print(f"⚠️  Cache save error: {e}")
    
    def _is_valid(self, timestamp_str, current_time):
        """Check if cache entry is still valid"""
        try:
            if not timestamp_str:
                return False
            cached_time = datetime.fromisoformat(timestamp_str)
            return (current_time - cached_time).total_seconds() < (self.ttl_hours * 3600)
        except:
            return False
    
    def get(self, key):
        """Get cached data if valid"""
        with self.lock:
            entry = self.cache.get(key)
            if entry and self._is_valid(entry.get('timestamp'), datetime.now()):
                return entry.get('data')
            return None
    
    def set(self, key, data):
        """Set cached data with timestamp"""
        with self.lock:
            self.cache[key] = {
                'data': data,
                'timestamp': datetime.now().isoformat()
            }

class PerformanceRedditExtractor(ABC):
    """
    High-performance Reddit extractor with parallel processing and caching
    """
    
    def __init__(self, max_workers=8, cache_ttl_hours=1):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        self.ranker = PopularityRankerV2()
        self.cache = RedditCache(ttl_hours=cache_ttl_hours)
        self.max_workers = max_workers
        self.rate_limit_delay = 0.1  # Small delay to respect API limits
        
    @property
    @abstractmethod
    def subreddits(self):
        pass
    
    @property
    @abstractmethod
    def domain_name(self):
        pass
    
    @property
    @abstractmethod
    def min_popularity_threshold(self):
        pass
    
    def _extract_subreddit_data(self, subreddit_name, time_filter, limit):
        """
        Extract data from a single subreddit with caching and error handling
        
        Args:
            subreddit_name: Name of subreddit
            time_filter: 'week' or 'day'
            limit: Number of posts to extract
            
        Returns:
            tuple: (subreddit_name, posts_data, success)
        """
        cache_key = f"{subreddit_name}_{time_filter}_{limit}"
        
        # Check cache first
        cached_data = self.cache.get(cache_key)
        if cached_data:
            print(f"📚 Using cached data for r/{subreddit_name}")
            return (subreddit_name, cached_data, True)
        
        try:
            # Add small delay to respect rate limits
            time.sleep(self.rate_limit_delay)
            
            print(f"🌐 Fetching fresh data from r/{subreddit_name}...")
            subreddit = self.reddit.subreddit(subreddit_name)
            
            # Get posts
            if time_filter == 'week':
                posts = list(subreddit.top(time_filter='week', limit=limit))
            elif time_filter == 'day':
                posts = list(subreddit.top(time_filter='day', limit=limit))
            else:
                raise ValueError("time_filter must be 'week' or 'day'")
            
            # Extract post data
            posts_data = []
            for post in posts:
                post_data = self._extract_post_data(post, subreddit_name)
                posts_data.append(post_data)
            
            # Cache the results
            self.cache.set(cache_key, posts_data)
            
            print(f"  ✅ Extracted {len(posts_data)} posts from r/{subreddit_name}")
            return (subreddit_name, posts_data, True)
            
        except Exception as e:
            print(f"  ❌ Error with r/{subreddit_name}: {e}")
            return (subreddit_name, [], False)
    
    def extract_raw_posts_parallel(self, time_filter='week', limit_per_subreddit=100):
        """
        Extract posts from all subreddits in parallel
        
        Args:
            time_filter: 'week' or 'day'
            limit_per_subreddit: Posts per subreddit
            
        Returns:
            pandas.DataFrame: Combined posts data
        """
        print(f"⚡ Starting parallel {self.domain_name} extraction...")
        print(f"🔧 Using {self.max_workers} workers for {len(self.subreddits)} subreddits")
        print(f"📊 Target: {limit_per_subreddit} posts per subreddit")
        print("=" * 60)
        
        start_time = time.time()
        all_posts = []
        successful_extractions = 0
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_subreddit = {
                executor.submit(
                    self._extract_subreddit_data, 
                    subreddit_name, 
                    time_filter, 
                    limit_per_subreddit
                ): subreddit_name 
                for subreddit_name in self.subreddits
            }
            
            # Process completed tasks
            for future in as_completed(future_to_subreddit):
                subreddit_name, posts_data, success = future.result()
                
                if success and posts_data:
                    all_posts.extend(posts_data)
                    successful_extractions += 1
        
        # Save cache after all extractions
        self.cache.save_cache()
        
        elapsed_time = time.time() - start_time
        print(f"\n⚡ Parallel extraction complete in {elapsed_time:.1f}s")
        print(f"🎯 Extracted {len(all_posts)} posts from {successful_extractions}/{len(self.subreddits)} subreddits")
        
        if all_posts:
            # Optimize DataFrame creation
            df = pd.DataFrame(all_posts)
            # Optimize data types to reduce memory usage
            df = self._optimize_dataframe(df)
            return df
        else:
            return pd.DataFrame()
    
    def _optimize_dataframe(self, df):
        """
        Optimize DataFrame memory usage and data types
        
        Args:
            df: Input DataFrame
            
        Returns:
            pandas.DataFrame: Optimized DataFrame
        """
        if df.empty:
            return df
        
        print("🔧 Optimizing DataFrame memory usage...")
        
        # Convert object columns to categorical where appropriate
        categorical_columns = ['subreddit', 'author', 'link_flair_text', 'domain']
        for col in categorical_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype('category')
        
        # Optimize integer columns
        int_columns = ['score', 'num_comments']
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], downcast='integer')
        
        # Optimize float columns
        float_columns = ['upvote_ratio']
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Optimize boolean columns
        bool_columns = ['over_18', 'spoiler', 'stickied']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].astype('bool')
        
        return df
    
    def _extract_post_data(self, post, subreddit_name):
        """Extract standardized post data with error handling"""
        try:
            return {
                'subreddit': subreddit_name,
                'title': post.title,
                'author': str(post.author) if post.author else '[deleted]',
                'score': post.score,
                'upvote_ratio': getattr(post, 'upvote_ratio', 0.0),
                'num_comments': post.num_comments,
                'created_utc': pd.to_datetime(post.created_utc, unit='s'),
                'url': post.url,
                'selftext': (post.selftext[:1000] if post.selftext else ''),
                'link_flair_text': getattr(post, 'link_flair_text', ''),
                'post_id': post.id,
                'domain': getattr(post, 'domain', ''),
                'over_18': getattr(post, 'over_18', False),
                'spoiler': getattr(post, 'spoiler', False),
                'stickied': getattr(post, 'stickied', False)
            }
        except Exception as e:
            print(f"⚠️  Error extracting post data: {e}")
            return None
    
    def apply_scoring_and_filtering_optimized(self, df):
        """
        Apply scoring and filtering with memory optimization
        
        Args:
            df: DataFrame with raw posts
            
        Returns:
            pandas.DataFrame: Scored and filtered posts
        """
        if df.empty:
            return df
        
        print(f"🧮 Applying optimized scoring and filtering...")
        start_time = time.time()
        
        # Calculate scores in chunks for large datasets
        chunk_size = 10000
        if len(df) > chunk_size:
            print(f"📊 Processing {len(df)} posts in chunks of {chunk_size}")
            scored_chunks = []
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size].copy()
                scored_chunk = self.ranker.calculate_popularity_score(chunk)
                scored_chunks.append(scored_chunk)
            
            df_scored = pd.concat(scored_chunks, ignore_index=True)
        else:
            df_scored = self.ranker.calculate_popularity_score(df)
        
        # Apply filtering
        df_filtered = self.ranker.apply_filters(
            df_scored, 
            min_popularity=self.min_popularity_threshold
        )
        
        elapsed_time = time.time() - start_time
        print(f"⚡ Scoring complete in {elapsed_time:.1f}s")
        print(f"📈 Posts above {self.min_popularity_threshold} threshold: {len(df_filtered)}")
        
        return df_filtered
    
    def extract_and_process_performance(self, time_filter='week', base_limit=100):
        """
        High-performance extraction and processing pipeline
        
        Args:
            time_filter: 'week' or 'day'
            base_limit: Posts per subreddit
            
        Returns:
            pandas.DataFrame: Processed posts
        """
        print(f"\n🚀 Starting high-performance {self.domain_name} pipeline...")
        print(f"⚡ Max workers: {self.max_workers}")
        print(f"📚 Cache TTL: {self.cache.ttl_hours}h")
        print(f"🎯 Popularity threshold: {self.min_popularity_threshold}")
        
        total_start_time = time.time()
        
        # Step 1: Parallel extraction
        df_raw = self.extract_raw_posts_parallel(time_filter, base_limit)
        
        if df_raw.empty:
            print("❌ No raw posts extracted")
            return pd.DataFrame()
        
        # Step 2: Optimized scoring and filtering
        df_processed = self.apply_scoring_and_filtering_optimized(df_raw)
        
        total_elapsed = time.time() - total_start_time
        
        if df_processed.empty:
            print("❌ No posts passed filtering")
        else:
            print(f"\n🎯 High-performance pipeline complete in {total_elapsed:.1f}s")
            print(f"✅ {len(df_processed)} posts ready for classification")
        
        return df_processed
    
    def get_cache_stats(self):
        """Get cache statistics"""
        print(f"\n📚 Cache Statistics:")
        print(f"  Entries: {len(self.cache.cache)}")
        print(f"  TTL: {self.cache.ttl_hours}h")
        print(f"  Cache file: {self.cache.cache_file}")
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.cache = {}
        self.cache.save_cache()
        print("🗑️  Cache cleared")