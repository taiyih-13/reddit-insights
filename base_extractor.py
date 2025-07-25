#!/usr/bin/env python3
"""
Base Reddit Extractor Class
Provides shared functionality for all Reddit data extraction workflows
"""

import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from popularity_ranker_v2 import PopularityRankerV2

load_dotenv()

class BaseRedditExtractor(ABC):
    """
    Abstract base class for Reddit extractors
    Handles common Reddit API operations and data processing
    """
    
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        self.ranker = PopularityRankerV2()
        
    @property
    @abstractmethod
    def subreddits(self):
        """List of subreddits to extract from - must be implemented by subclasses"""
        pass
    
    @property
    @abstractmethod
    def domain_name(self):
        """Domain name for logging/identification - must be implemented by subclasses"""
        pass
    
    @property
    @abstractmethod
    def min_popularity_threshold(self):
        """Minimum popularity score threshold - must be implemented by subclasses"""
        pass
    
    def extract_raw_posts(self, time_filter='week', limit_per_subreddit=100):
        """
        Extract raw posts from all subreddits
        
        Args:
            time_filter: 'week' or 'day'
            limit_per_subreddit: Number of posts to extract per subreddit
            
        Returns:
            pandas.DataFrame: Raw posts data
        """
        print(f"🔍 Extracting {self.domain_name} posts from past {time_filter}...")
        print(f"📊 Targeting {len(self.subreddits)} subreddits with {limit_per_subreddit} posts each")
        print("=" * 60)
        
        all_posts = []
        successful_extractions = 0
        
        for subreddit_name in self.subreddits:
            try:
                print(f"📥 Extracting from r/{subreddit_name}...")
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get posts based on time filter
                if time_filter == 'week':
                    posts = list(subreddit.top(time_filter='week', limit=limit_per_subreddit))
                elif time_filter == 'day':
                    posts = list(subreddit.top(time_filter='day', limit=limit_per_subreddit))
                else:
                    raise ValueError("time_filter must be 'week' or 'day'")
                
                # Extract post data
                for post in posts:
                    post_data = self._extract_post_data(post, subreddit_name)
                    all_posts.append(post_data)
                
                print(f"  ✅ Extracted {len(posts)} posts")
                successful_extractions += 1
                
            except Exception as e:
                print(f"  ❌ Error extracting from r/{subreddit_name}: {e}")
        
        if all_posts:
            df = pd.DataFrame(all_posts)
            print(f"\n🎯 Successfully extracted {len(df)} total posts from {successful_extractions}/{len(self.subreddits)} subreddits")
            return df
        else:
            print("\n❌ No posts extracted!")
            return pd.DataFrame()
    
    def _extract_post_data(self, post, subreddit_name):
        """
        Extract standardized post data
        
        Args:
            post: Reddit post object
            subreddit_name: Name of the subreddit
            
        Returns:
            dict: Standardized post data
        """
        return {
            'subreddit': subreddit_name,
            'title': post.title,
            'author': str(post.author),
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created_utc': pd.to_datetime(post.created_utc, unit='s'),
            'url': post.url,
            'selftext': post.selftext[:1000] if post.selftext else '',
            'link_flair_text': post.link_flair_text,
            'post_id': post.id,
            'domain': getattr(post, 'domain', ''),
            'over_18': getattr(post, 'over_18', False),
            'spoiler': getattr(post, 'spoiler', False),
            'stickied': getattr(post, 'stickied', False)
        }
    
    def apply_scoring_and_filtering(self, df):
        """
        Apply popularity scoring and filtering using domain-specific threshold
        
        Args:
            df: DataFrame with raw posts
            
        Returns:
            pandas.DataFrame: Scored and filtered posts
        """
        print(f"\n🧮 Applying popularity scoring...")
        
        # Calculate popularity scores
        df_scored = self.ranker.calculate_popularity_score(df)
        
        # Apply domain-specific filtering
        df_filtered = self.ranker.apply_filters(
            df_scored, 
            min_popularity=self.min_popularity_threshold
        )
        
        print(f"📈 Posts above {self.min_popularity_threshold} threshold: {len(df_filtered)}")
        
        return df_filtered
    
    def get_subreddit_stats(self, time_filter='week', limit_per_subreddit=100):
        """
        Get statistics about post counts from each subreddit
        
        Args:
            time_filter: 'week' or 'day'
            limit_per_subreddit: Number of posts to check per subreddit
            
        Returns:
            dict: Subreddit statistics
        """
        print(f"📊 Gathering {self.domain_name} subreddit statistics...")
        
        stats = {}
        total_posts = 0
        
        for subreddit_name in self.subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = list(subreddit.top(time_filter='week', limit=limit_per_subreddit))
                elif time_filter == 'day':
                    posts = list(subreddit.top(time_filter='day', limit=limit_per_subreddit))
                
                post_count = len(posts)
                stats[subreddit_name] = {
                    'post_count': post_count,
                    'subscribers': getattr(subreddit, 'subscribers', 0)
                }
                total_posts += post_count
                
                print(f"  r/{subreddit_name}: {post_count} posts")
                
            except Exception as e:
                print(f"  ❌ Error with r/{subreddit_name}: {e}")
                stats[subreddit_name] = {'post_count': 0, 'subscribers': 0}
        
        stats['_total'] = total_posts
        print(f"\n📈 Total posts available: {total_posts}")
        
        return stats
    
    def extract_and_process(self, time_filter='week', base_limit=100):
        """
        Complete extraction and processing pipeline
        
        Args:
            time_filter: 'week' or 'day'
            base_limit: Number of posts to extract per subreddit
            
        Returns:
            pandas.DataFrame: Processed posts ready for classification
        """
        print(f"\n🚀 Starting {self.domain_name} extraction and processing pipeline...")
        print(f"⏱️  Time filter: {time_filter}")
        print(f"📊 Base limit per subreddit: {base_limit}")
        print(f"🎯 Popularity threshold: {self.min_popularity_threshold}")
        
        # Step 1: Extract raw posts
        df_raw = self.extract_raw_posts(time_filter, base_limit)
        
        if df_raw.empty:
            print("❌ No raw posts extracted. Exiting pipeline.")
            return pd.DataFrame()
        
        # Step 2: Apply scoring and filtering
        df_processed = self.apply_scoring_and_filtering(df_raw)
        
        if df_processed.empty:
            print("❌ No posts passed filtering. Consider lowering threshold.")
            return pd.DataFrame()
        
        print(f"\n✅ Pipeline complete: {len(df_processed)} posts ready for classification")
        return df_processed