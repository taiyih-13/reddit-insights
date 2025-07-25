#!/usr/bin/env python3
"""
Resilient Reddit Extractor
Adds retry mechanisms, circuit breaker, and connection pooling
"""

import praw
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from popularity_ranker_v2 import PopularityRankerV2

load_dotenv()

class CircuitBreaker:
    """Circuit breaker pattern for handling API failures"""
    
    def __init__(self, failure_threshold=5, recovery_timeout=300, expected_exception=Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.lock = Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == 'OPEN':
                if self._should_attempt_reset():
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception as e:
                self._on_failure()
                raise e
    
    def _should_attempt_reset(self):
        """Check if enough time has passed to attempt reset"""
        return (
            self.last_failure_time and
            time.time() - self.last_failure_time >= self.recovery_timeout
        )
    
    def _on_success(self):
        """Handle successful execution"""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

class ResilientRedditExtractor(ABC):
    """
    Resilient Reddit extractor with retry logic, circuit breaker, and connection pooling
    """
    
    def __init__(self, max_workers=6, cache_ttl_hours=1, max_retries=3):
        # Create multiple Reddit instances for connection pooling
        self.reddit_pool = self._create_reddit_pool(pool_size=max_workers)
        self.pool_index = 0
        self.pool_lock = Lock()
        
        self.ranker = PopularityRankerV2()
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.rate_limit_delay = 0.2  # Increased delay for stability
        
        # Circuit breakers for each subreddit
        self.circuit_breakers = {}
        
        # Enhanced cache with metadata
        self.cache = self._initialize_cache(cache_ttl_hours)
        
    def _create_reddit_pool(self, pool_size=6):
        """Create pool of Reddit instances for connection pooling"""
        pool = []
        for i in range(pool_size):
            reddit_instance = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=f"{os.getenv('REDDIT_USER_AGENT')}_pool_{i}"
            )
            pool.append(reddit_instance)
        return pool
    
    def _get_reddit_instance(self):
        """Get next Reddit instance from pool (round-robin)"""
        with self.pool_lock:
            reddit = self.reddit_pool[self.pool_index]
            self.pool_index = (self.pool_index + 1) % len(self.reddit_pool)
            return reddit
    
    def _initialize_cache(self, ttl_hours):
        """Initialize enhanced cache system"""
        return {
            'data': {},
            'metadata': {
                'ttl_hours': ttl_hours,
                'hits': 0,
                'misses': 0,
                'created': datetime.now().isoformat()
            }
        }
    
    def _get_circuit_breaker(self, subreddit_name):
        """Get or create circuit breaker for subreddit"""
        if subreddit_name not in self.circuit_breakers:
            self.circuit_breakers[subreddit_name] = CircuitBreaker(
                failure_threshold=3,
                recovery_timeout=600,  # 10 minutes
                expected_exception=Exception
            )
        return self.circuit_breakers[subreddit_name]
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Retry function with exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise e
                
                # Exponential backoff with jitter
                delay = (2 ** attempt) + random.uniform(0, 1)
                print(f"  🔄 Retry {attempt + 1}/{self.max_retries} in {delay:.1f}s: {e}")
                time.sleep(delay)
    
    def _extract_subreddit_resilient(self, subreddit_name, time_filter, limit):
        """
        Extract from subreddit with full resilience features
        
        Returns:
            tuple: (subreddit_name, posts_data, success, from_cache)
        """
        cache_key = f"{subreddit_name}_{time_filter}_{limit}"
        
        # Check cache first
        cached_entry = self.cache['data'].get(cache_key)
        if cached_entry and self._is_cache_valid(cached_entry):
            self.cache['metadata']['hits'] += 1
            print(f"📚 Cache hit for r/{subreddit_name}")
            return (subreddit_name, cached_entry['data'], True, True)
        
        self.cache['metadata']['misses'] += 1
        circuit_breaker = self._get_circuit_breaker(subreddit_name)
        
        def extract_posts():
            reddit = self._get_reddit_instance()
            
            # Rate limiting
            time.sleep(self.rate_limit_delay + random.uniform(0, 0.1))
            
            print(f"🌐 Fetching r/{subreddit_name} (attempt with resilience)...")
            subreddit = reddit.subreddit(subreddit_name)
            
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
                post_data = self._extract_post_data_safe(post, subreddit_name)
                if post_data:  # Only add if extraction succeeded
                    posts_data.append(post_data)
            
            return posts_data
        
        try:
            # Use circuit breaker and retry logic
            posts_data = circuit_breaker.call(
                self._retry_with_backoff,
                extract_posts
            )
            
            # Cache successful results
            self.cache['data'][cache_key] = {
                'data': posts_data,
                'timestamp': datetime.now().isoformat(),
                'subreddit': subreddit_name
            }
            
            print(f"  ✅ Extracted {len(posts_data)} posts from r/{subreddit_name}")
            return (subreddit_name, posts_data, True, False)
            
        except Exception as e:
            print(f"  💥 Failed r/{subreddit_name} after all retries: {e}")
            return (subreddit_name, [], False, False)
    
    def _is_cache_valid(self, cache_entry):
        """Check if cache entry is still valid"""
        try:
            cached_time = datetime.fromisoformat(cache_entry['timestamp'])
            age_hours = (datetime.now() - cached_time).total_seconds() / 3600
            return age_hours < self.cache['metadata']['ttl_hours']
        except:
            return False
    
    def _extract_post_data_safe(self, post, subreddit_name):
        """Safely extract post data with error handling"""
        try:
            return {
                'subreddit': subreddit_name,
                'title': post.title or '[No Title]',
                'author': str(post.author) if post.author else '[deleted]',
                'score': max(0, post.score),  # Ensure non-negative
                'upvote_ratio': max(0.0, min(1.0, getattr(post, 'upvote_ratio', 0.5))),
                'num_comments': max(0, post.num_comments),
                'created_utc': pd.to_datetime(post.created_utc, unit='s'),
                'url': post.url or '',
                'selftext': (post.selftext[:1000] if post.selftext else ''),
                'link_flair_text': getattr(post, 'link_flair_text', '') or '',
                'post_id': post.id,
                'domain': getattr(post, 'domain', '') or '',
                'over_18': bool(getattr(post, 'over_18', False)),
                'spoiler': bool(getattr(post, 'spoiler', False)),
                'stickied': bool(getattr(post, 'stickied', False))
            }
        except Exception as e:
            print(f"⚠️  Error extracting post from r/{subreddit_name}: {e}")
            return None
    
    def extract_with_full_resilience(self, time_filter='week', limit_per_subreddit=100):
        """
        Extract with all resilience features enabled
        
        Returns:
            pandas.DataFrame: Extracted posts
        """
        print(f"🛡️  Starting resilient {self.domain_name} extraction...")
        print(f"🔧 Workers: {self.max_workers}, Retries: {self.max_retries}")
        print(f"📊 Target: {len(self.subreddits)} subreddits × {limit_per_subreddit} posts")
        print("=" * 60)
        
        start_time = time.time()
        all_posts = []
        successful_extractions = 0
        cache_hits = 0
        
        # Parallel extraction with resilience
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_subreddit = {
                executor.submit(
                    self._extract_subreddit_resilient,
                    subreddit_name,
                    time_filter,
                    limit_per_subreddit
                ): subreddit_name
                for subreddit_name in self.subreddits
            }
            
            for future in as_completed(future_to_subreddit):
                subreddit_name, posts_data, success, from_cache = future.result()
                
                if success and posts_data:
                    all_posts.extend(posts_data)
                    successful_extractions += 1
                    if from_cache:
                        cache_hits += 1
        
        elapsed_time = time.time() - start_time
        
        # Report results
        print(f"\n🛡️  Resilient extraction complete in {elapsed_time:.1f}s")
        print(f"✅ Success: {successful_extractions}/{len(self.subreddits)} subreddits")
        print(f"📚 Cache hits: {cache_hits}/{successful_extractions}")
        print(f"📊 Total posts: {len(all_posts)}")
        
        # Show circuit breaker states
        open_breakers = [name for name, cb in self.circuit_breakers.items() if cb.state == 'OPEN']
        if open_breakers:
            print(f"⚠️  Circuit breakers OPEN: {len(open_breakers)} subreddits")
        
        if all_posts:
            df = pd.DataFrame(all_posts)
            return self._optimize_dataframe(df)
        else:
            return pd.DataFrame()
    
    def _optimize_dataframe(self, df):
        """Optimize DataFrame for memory efficiency"""
        if df.empty:
            return df
        
        # Optimize data types
        categorical_cols = ['subreddit', 'author', 'link_flair_text', 'domain']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype('category')
        
        # Downcast numeric types
        numeric_cols = ['score', 'num_comments']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], downcast='integer')
        
        return df
    
    def get_resilience_stats(self):
        """Get comprehensive resilience statistics"""
        print(f"\n🛡️  Resilience Statistics")
        print("=" * 30)
        
        # Cache stats
        cache_meta = self.cache['metadata']
        total_requests = cache_meta['hits'] + cache_meta['misses']
        hit_rate = (cache_meta['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        print(f"📚 Cache:")
        print(f"  Entries: {len(self.cache['data'])}")
        print(f"  Hit rate: {hit_rate:.1f}% ({cache_meta['hits']}/{total_requests})")
        print(f"  TTL: {cache_meta['ttl_hours']}h")
        
        # Circuit breaker stats
        print(f"\n⚡ Circuit Breakers:")
        states = {'CLOSED': 0, 'OPEN': 0, 'HALF_OPEN': 0}
        for cb in self.circuit_breakers.values():
            states[cb.state] += 1
        
        for state, count in states.items():
            print(f"  {state}: {count}")
        
        print(f"\n🔧 Connection Pool: {len(self.reddit_pool)} instances")
    
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