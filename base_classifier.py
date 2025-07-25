#!/usr/bin/env python3
"""
Base Classifier Class
Provides shared functionality for content classification workflows
"""

import pandas as pd
import re
from abc import ABC, abstractmethod
from collections import defaultdict

class BaseClassifier(ABC):
    """
    Abstract base class for content classifiers
    Handles common classification patterns and media filtering
    """
    
    def __init__(self):
        # Common media patterns that should be filtered out
        self.media_patterns = {
            'image_extensions': r'\.(jpg|jpeg|png|gif|webp|bmp|svg)(\?|$)',
            'video_extensions': r'\.(mp4|webm|mov|avi|mkv|flv|wmv)(\?|$)',
            'youtube_links': r'(youtube\.com|youtu\.be)',
            'social_media': r'(twitter\.com|instagram\.com|tiktok\.com|facebook\.com)',
            'streaming_clips': r'(twitch\.tv|clips\.twitch\.tv)',
            'reddit_media': r'(i\.redd\.it|v\.redd\.it)',
            'image_hosts': r'(imgur\.com|gyazo\.com|postimage\.org)'
        }
        
        # Common words that indicate low-quality content
        self.low_quality_indicators = [
            'daily thread', 'daily discussion', 'what are your moves',
            'weekly thread', 'weekly discussion', 'earnings thread',
            'weekend discussion', 'chat thread', 'simple questions'
        ]
    
    @property
    @abstractmethod
    def classification_rules(self):
        """Classification rules mapping - must be implemented by subclasses"""
        pass
    
    @property
    @abstractmethod
    def category_minimums(self):
        """Minimum posts per category - must be implemented by subclasses"""
        pass
    
    @property
    @abstractmethod
    def domain_name(self):
        """Domain name for logging/identification - must be implemented by subclasses"""
        pass
    
    def is_media_content(self, url, title=''):
        """
        Check if content is primarily media-based
        
        Args:
            url: Post URL
            title: Post title (optional)
            
        Returns:
            bool: True if content is media-based
        """
        if not url:
            return False
        
        url_lower = url.lower()
        title_lower = title.lower()
        
        # Check URL patterns
        for pattern_name, pattern in self.media_patterns.items():
            if re.search(pattern, url_lower):
                return True
        
        # Check title for media indicators
        media_keywords = ['[image]', '[video]', '[gif]', 'screenshot', 'photo', 'pic']
        for keyword in media_keywords:
            if keyword in title_lower:
                return True
        
        return False
    
    def is_low_quality_content(self, title, selftext=''):
        """
        Check if content is low-quality discussion thread
        
        Args:
            title: Post title
            selftext: Post content (optional)
            
        Returns:
            bool: True if content is low-quality
        """
        combined_text = f"{title} {selftext}".lower()
        
        for indicator in self.low_quality_indicators:
            if indicator in combined_text:
                return True
        
        return False
    
    def apply_content_filters(self, df, filter_media=True, filter_low_quality=True):
        """
        Apply content quality filters
        
        Args:
            df: DataFrame with posts
            filter_media: Whether to filter out media content
            filter_low_quality: Whether to filter out low-quality content
            
        Returns:
            pandas.DataFrame: Filtered posts
        """
        initial_count = len(df)
        df_filtered = df.copy()
        
        if filter_media:
            media_mask = df_filtered.apply(
                lambda row: self.is_media_content(row.get('url', ''), row.get('title', '')), 
                axis=1
            )
            df_filtered = df_filtered[~media_mask]
            media_filtered = initial_count - len(df_filtered)
            print(f"🎬 Filtered {media_filtered} media posts")
        
        if filter_low_quality:
            quality_mask = df_filtered.apply(
                lambda row: self.is_low_quality_content(row.get('title', ''), row.get('selftext', '')), 
                axis=1
            )
            df_filtered = df_filtered[~quality_mask]
            quality_filtered = len(df_filtered) - initial_count + (media_filtered if filter_media else 0)
            print(f"🗑️  Filtered {abs(quality_filtered)} low-quality posts")
        
        final_count = len(df_filtered)
        print(f"✅ Content filtering complete: {initial_count} → {final_count} posts")
        
        return df_filtered
    
    def classify_single_post(self, title, selftext='', subreddit=''):
        """
        Classify a single post using the classification rules
        
        Args:
            title: Post title
            selftext: Post content
            subreddit: Source subreddit
            
        Returns:
            str: Category name or 'other'
        """
        combined_text = f"{title} {selftext}".lower()
        
        # Check each category's rules
        for category, rules in self.classification_rules.items():
            # Check subreddit mapping (Layer 1)
            if 'subreddits' in rules and subreddit in rules['subreddits']:
                return category
            
            # Check pattern matching (Layer 2+)
            if 'patterns' in rules:
                for pattern in rules['patterns']:
                    if re.search(pattern.lower(), combined_text):
                        return category
            
            # Check keyword matching
            if 'keywords' in rules:
                for keyword in rules['keywords']:
                    if keyword.lower() in combined_text:
                        return category
        
        return 'other'
    
    def classify_posts(self, df):
        """
        Classify all posts in the DataFrame
        
        Args:
            df: DataFrame with posts
            
        Returns:
            pandas.DataFrame: Posts with category column added
        """
        print(f"🏷️  Classifying {len(df)} {self.domain_name} posts...")
        
        df_classified = df.copy()
        df_classified['category'] = df_classified.apply(
            lambda row: self.classify_single_post(
                row.get('title', ''),
                row.get('selftext', ''),
                row.get('subreddit', '')
            ),
            axis=1
        )
        
        # Show classification distribution
        category_counts = df_classified['category'].value_counts()
        print(f"\n📊 Classification results:")
        for category, count in category_counts.items():
            print(f"  {category}: {count} posts")
        
        return df_classified
    
    def apply_balanced_selection(self, df):
        """
        Apply balanced selection ensuring category minimums are met
        
        Args:
            df: DataFrame with classified posts
            
        Returns:
            pandas.DataFrame: Balanced selection of posts
        """
        print(f"\n⚖️  Applying balanced selection with category minimums...")
        
        df_sorted = df.sort_values('popularity_score', ascending=False)
        selected_posts = []
        
        # Track category counts
        category_counts = defaultdict(int)
        
        # First pass: Ensure minimums are met
        for category, minimum in self.category_minimums.items():
            category_posts = df_sorted[df_sorted['category'] == category]
            
            if len(category_posts) >= minimum:
                # Take all posts for this category (minimums are floors, not caps)
                selected_for_category = category_posts
                selected_posts.append(selected_for_category)
                category_counts[category] = len(selected_for_category)
                print(f"  {category}: {len(selected_for_category)} posts (minimum: {minimum})")
            else:
                # Take what we have if below minimum
                if len(category_posts) > 0:
                    selected_posts.append(category_posts)
                    category_counts[category] = len(category_posts)
                    print(f"  {category}: {len(category_posts)} posts (below minimum of {minimum})")
                else:
                    print(f"  {category}: 0 posts (minimum: {minimum}) ⚠️")
        
        # Handle 'other' category if it exists
        other_posts = df_sorted[df_sorted['category'] == 'other']
        if len(other_posts) > 0:
            selected_posts.append(other_posts)
            category_counts['other'] = len(other_posts)
            print(f"  other: {len(other_posts)} posts")
        
        # Combine all selected posts
        if selected_posts:
            df_balanced = pd.concat(selected_posts, ignore_index=True)
            df_balanced = df_balanced.sort_values('popularity_score', ascending=False)
            
            print(f"\n✅ Balanced selection complete: {len(df_balanced)} total posts")
            return df_balanced
        else:
            print("\n❌ No posts selected in balanced selection")
            return pd.DataFrame()
    
    def process_classification_pipeline(self, df, filter_media=True, filter_low_quality=True):
        """
        Complete classification pipeline
        
        Args:
            df: DataFrame with scored posts
            filter_media: Whether to filter media content
            filter_low_quality: Whether to filter low-quality content
            
        Returns:
            pandas.DataFrame: Fully processed and classified posts
        """
        print(f"\n🏭 Starting {self.domain_name} classification pipeline...")
        
        # Step 1: Apply content filters
        if filter_media or filter_low_quality:
            df_filtered = self.apply_content_filters(df, filter_media, filter_low_quality)
            if df_filtered.empty:
                print("❌ No posts remaining after content filtering")
                return pd.DataFrame()
        else:
            df_filtered = df
        
        # Step 2: Classify posts
        df_classified = self.classify_posts(df_filtered)
        
        # Step 3: Apply balanced selection
        df_balanced = self.apply_balanced_selection(df_classified)
        
        print(f"\n🎯 Classification pipeline complete: {len(df_balanced)} posts ready for dashboard")
        return df_balanced