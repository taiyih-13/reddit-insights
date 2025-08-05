#!/usr/bin/env python3
"""
Travel Database Extractor - Supabase-First Version
Extracts travel posts and saves directly to Supabase with computed fields
"""

import pandas as pd
import time
import praw
import os
import sys
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.popularity_ranker import PopularityRankerV2
from classifiers.travel_classifier import TravelClassifier
# Comment fetcher removed - now using live comment API
from services.enhanced_database_service import get_enhanced_db_service
from services.fixed_database_service import save_posts_basic_schema
from utils.travel_sentiment_analyzer import TravelSentimentAnalyzer
from utils.travel_city_tracker import TravelCityTracker

load_dotenv()

class TravelDatabaseExtractor:
    """
    Travel extractor that saves directly to Supabase database
    Includes all computed fields from the original CSV approach
    """
    
    def __init__(self):
        # Initialize Reddit client
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Define travel subreddits (matching database service mapping)
        self.travel_subreddits = [
            'travel', 'solotravel', 'backpacking', 'JapanTravel', 'ItalyTravel',
            'travel_Europe', 'ThailandTourism', 'IndiaTravel', 'SouthEastAsia',
            'koreatravel', 'chinatravel', 'VietnamTravel', 'Nepal', 'indonesia',
            'VisitingIceland', 'uktravel', 'Spain', 'france', 'germany', 'greece',
            'portugal', 'MexicoTravel', 'usatravel', 'CanadaTravel', 'caribbeantravel',
            'Guatemala', 'CostaRica', 'braziltravel', 'argentina', 'chile', 'peru',
            'colombia', 'ecuador', 'australia', 'newzealand', 'southafrica',
            'Morocco', 'Kenya', 'Ethiopia', 'TravelNoPics', 'travelhacks', 'onebag', 'Shoestring'
        ]
        
        # Expose subreddits for incremental update service
        self.subreddits = self.travel_subreddits
        
        self.ranker = PopularityRankerV2()
        self.classifier = TravelClassifier()
        self.sentiment_analyzer = TravelSentimentAnalyzer()
        self.city_tracker = TravelCityTracker()
        self.db_service = get_enhanced_db_service()
        
        # Target minimums for each category
        self.category_minimums = {
            'Regional Travel': 200,
            'Travel Tips & Advice': 100,
            'Solo Travel': 80,
            'Budget Travel': 60,
            'General Travel': 75
        }
        
        # Category-specific popularity thresholds
        self.category_thresholds = {
            'Regional Travel': 80,
            'Travel Tips & Advice': 60,
            'Solo Travel': 70,
            'Budget Travel': 50,
            'General Travel': 65
        }
    
    def extract_and_save_to_database(self, time_filter='week', base_limit=30) -> Dict[str, Any]:
        """
        Extract travel posts and save directly to Supabase database
        
        Args:
            time_filter: 'week' or 'day'
            base_limit: Base limit per subreddit
            
        Returns:
            Dictionary with extraction statistics
        """
        
        print(f"✈️ TRAVEL DATABASE EXTRACTION ({time_filter})")
        print("=" * 60)
        print(f"Target: {sum(self.category_minimums.values())} balanced posts")
        print(f"Subreddits: {len(self.travel_subreddits)}")
        print(f"Database: Saving directly to Supabase")
        
        extraction_start = datetime.now()
        
        # Check if posts already exist for this time period
        existing_posts = self.db_service.get_posts_by_domain('travel', time_filter)
        existing_ids = set(existing_posts['id'].tolist()) if not existing_posts.empty else set()
        
        print(f"📊 Found {len(existing_ids)} existing posts in database")
        
        # Extract posts for each category
        all_posts = []
        total_api_calls = 0
        
        for category, minimum in self.category_minimums.items():
            print(f"\n🎯 Extracting {category} (target: {minimum})...")
            
            threshold = self.category_thresholds.get(category, 65)
            
            if category == 'Regional Travel':
                # Focus on regional/country-specific subreddits
                category_subreddits = [
                    'JapanTravel', 'ItalyTravel', 'travel_Europe', 'ThailandTourism', 
                    'IndiaTravel', 'SouthEastAsia', 'koreatravel', 'chinatravel', 
                    'VietnamTravel', 'Nepal', 'indonesia', 'VisitingIceland', 
                    'uktravel', 'Spain', 'france', 'germany', 'greece', 'portugal',
                    'MexicoTravel', 'usatravel', 'CanadaTravel', 'caribbeantravel',
                    'Guatemala', 'CostaRica', 'braziltravel', 'argentina', 'chile',
                    'peru', 'colombia', 'ecuador', 'australia', 'newzealand',
                    'southafrica', 'Morocco', 'Kenya', 'Ethiopia'
                ]
            elif category == 'Travel Tips & Advice':
                category_subreddits = ['travel', 'TravelNoPics', 'travelhacks', 'onebag', 'Shoestring']
            elif category == 'Solo Travel':
                category_subreddits = ['solotravel', 'travel', 'backpacking']
            elif category == 'Budget Travel':
                category_subreddits = ['Shoestring', 'backpacking', 'travel', 'onebag']
            else:
                # General Travel
                category_subreddits = ['travel', 'backpacking', 'TravelNoPics']
            
            category_posts = self._extract_category_posts(
                category_subreddits, category, threshold, base_limit, time_filter, existing_ids
            )
            
            total_api_calls += len(category_subreddits)
            all_posts.extend(category_posts)
            
            print(f"   ✅ Extracted {len(category_posts)} {category} posts")
            
            # Add delay between categories to avoid rate limiting
            time.sleep(2)
        
        if not all_posts:
            print("❌ No posts extracted!")
            return {
                'total_posts': 0,
                'inserted_count': 0,
                'error_count': 0,
                'api_calls_made': total_api_calls,
                'extraction_time': 0
            }
        
        # Convert to DataFrame and add computed fields
        posts_df = pd.DataFrame(all_posts)
        
        print(f"\n🔄 Processing {len(posts_df)} posts...")
        
        # Add computed fields
        posts_df = self._add_computed_fields(posts_df, time_filter)
        
        # Save to database
        print(f"\n💾 Saving to Supabase database...")
        save_result = save_posts_basic_schema(posts_df)
        
        extraction_time = (datetime.now() - extraction_start).total_seconds()
        
        # Print results
        print(f"\n📊 EXTRACTION RESULTS:")
        print(f"   Posts extracted: {len(posts_df)}")
        print(f"   Posts saved: {save_result.get('inserted_count', 0)}")
        print(f"   Errors: {save_result.get('error_count', 0)}")
        print(f"   Success rate: {save_result.get('success_rate', 0):.1f}%")
        print(f"   API calls made: {total_api_calls}")
        print(f"   Extraction time: {extraction_time:.1f}s")
        print(f"   Computed fields supported: {save_result.get('computed_fields_supported', False)}")
        
        # Show category breakdown
        category_counts = posts_df['category'].value_counts()
        print(f"\n📋 CATEGORY BREAKDOWN:")
        for category, count in category_counts.items():
            target = self.category_minimums.get(category, 0)
            percentage = (count / len(posts_df)) * 100
            status = "✅" if count >= target else f"❌ (need {target - count} more)"
            print(f"   {category}: {count} posts ({percentage:.1f}%) {status}")
        
        return {
            'total_posts': len(posts_df),
            'inserted_count': save_result.get('inserted_count', 0),
            'error_count': save_result.get('error_count', 0),
            'success_rate': save_result.get('success_rate', 0),
            'api_calls_made': total_api_calls,
            'extraction_time': extraction_time,
            'computed_fields_supported': save_result.get('computed_fields_supported', False),
            'category_breakdown': category_counts.to_dict()
        }
    
    def extract_posts_since(self, since_timestamp: datetime, time_filter: str = 'week') -> pd.DataFrame:
        """
        Extract posts newer than since_timestamp for incremental updates
        Maintains same quality standards as full extraction
        """
        print(f"🔄 Incremental travel extraction since {since_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        
        all_posts = []
        cutoff_time = since_timestamp.timestamp()
        
        # Use same category targeting for quality
        for category, minimum in self.category_minimums.items():
            threshold = self.category_thresholds.get(category, 75)
            
            # Get category-specific subreddits for travel
            if 'Europe' in category:
                category_subreddits = ['travel_Europe', 'uktravel', 'Spain', 'france', 'germany', 'greece', 'portugal', 'VisitingIceland', 'ItalyTravel']
            elif 'Asia' in category:
                category_subreddits = ['JapanTravel', 'ThailandTourism', 'IndiaTravel', 'SouthEastAsia', 'koreatravel', 'chinatravel', 'VietnamTravel', 'Nepal', 'indonesia']
            elif 'America' in category:
                category_subreddits = ['MexicoTravel', 'usatravel', 'CanadaTravel', 'caribbeantravel', 'Guatemala', 'CostaRica', 'braziltravel', 'argentina', 'chile', 'peru', 'colombia', 'ecuador']
            elif 'Oceania' in category or 'Africa' in category:
                category_subreddits = ['australia', 'newzealand', 'southafrica', 'Morocco', 'Kenya', 'Ethiopia']
            elif 'Solo' in category:
                category_subreddits = ['solotravel', 'travel', 'backpacking']
            elif 'Budget' in category:
                category_subreddits = ['Shoestring', 'onebag', 'travel', 'backpacking']
            else:  # General travel advice
                category_subreddits = ['travel', 'TravelNoPics', 'travelhacks', 'onebag']
            
            category_posts = self._extract_new_posts_for_category(
                category_subreddits, category, threshold, cutoff_time, time_filter
            )
            all_posts.extend(category_posts)
        
        if not all_posts:
            return pd.DataFrame()
        
        # Same processing pipeline as full extraction
        posts_df = pd.DataFrame(all_posts)
        posts_df = self._add_computed_fields(posts_df, time_filter)
        
        print(f"✅ Extracted {len(posts_df)} new travel posts")
        return posts_df
    
    def extract_fast_update(self, time_filter: str = 'week') -> pd.DataFrame:
        """
        Fast update: Extract only top 5 posts from 5 key travel subreddits (~15 seconds)
        """
        print(f"⚡ Fast travel update ({time_filter})")
        
        # Top 5 most active travel subreddits
        key_subreddits = ['travel', 'solotravel', 'backpacking', 'JapanTravel', 'travel_Europe']
        posts_per_subreddit = 5
        
        all_posts = []
        cutoff_time = (datetime.utcnow() - (timedelta(days=7) if time_filter == 'week' else timedelta(days=1))).timestamp()
        
        for subreddit_name in key_subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get recent posts (small limit for speed)
                if time_filter == 'week':
                    posts = subreddit.top(time_filter='week', limit=posts_per_subreddit + 5)
                else:
                    posts = subreddit.hot(limit=posts_per_subreddit + 5)
                
                posts_added = 0
                for post in posts:
                    if posts_added >= posts_per_subreddit:
                        break
                        
                    # Only process recent posts
                    if post.created_utc <= cutoff_time:
                        continue
                    
                    post_data = {
                        'id': post.id,
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'author': str(post.author),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': pd.to_datetime(post.created_utc, unit='s'),
                        'url': post.url,
                        'selftext': post.selftext[:1000],
                        'link_flair_text': post.link_flair_text,
                        'time_filter': time_filter
                    }
                    
                    # Quick quality pipeline: scoring, threshold check, classification
                    df_temp = pd.DataFrame([post_data])
                    df_scored = self.ranker.calculate_popularity_score(df_temp)
                    
                    # Lower threshold for fast updates
                    if df_scored.iloc[0]['popularity_score'] >= 50:  # Lower threshold for travel
                        df_classified = self.classifier.classify_dataframe(df_scored)
                        enriched_post = df_classified.iloc[0].to_dict()
                        all_posts.append(enriched_post)
                        posts_added += 1
                
                time.sleep(0.5)  # Short delay for fast updates
                print(f"   ✅ {subreddit_name}: {posts_added} posts")
                
            except Exception as e:
                print(f"   ⚠️  Error with {subreddit_name}: {e}")
                continue
        
        if not all_posts:
            return pd.DataFrame()
        
        # Same processing pipeline as full extraction
        posts_df = pd.DataFrame(all_posts)
        posts_df = self._add_computed_fields(posts_df, time_filter)
        
        print(f"⚡ Fast travel update complete: {len(posts_df)} posts")
        return posts_df
    
    def _extract_new_posts_for_category(self, subreddits, target_category, threshold, cutoff_time, time_filter):
        """Extract new posts for a specific category since cutoff_time"""
        category_posts = []
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                # Get recent posts (larger limit to find new ones)
                if time_filter == 'week':
                    posts = subreddit.top(time_filter='week', limit=100)
                else:
                    posts = subreddit.hot(limit=50)  # Use hot for day filter
                
                for post in posts:
                    # Only process posts newer than cutoff
                    if post.created_utc <= cutoff_time:
                        continue
                    
                    post_data = {
                        'id': post.id,
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'author': str(post.author),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': pd.to_datetime(post.created_utc, unit='s'),
                        'url': post.url,
                        'selftext': post.selftext[:1000],
                        'link_flair_text': post.link_flair_text,
                        'time_filter': time_filter
                    }
                    
                    # Same quality pipeline: scoring, threshold check, classification
                    df_temp = pd.DataFrame([post_data])
                    df_scored = self.ranker.calculate_popularity_score(df_temp)
                    
                    if df_scored.iloc[0]['popularity_score'] >= threshold:
                        df_classified = self.classifier.classify_dataframe(df_scored)
                        classified_category = df_classified.iloc[0]['category']
                        
                        if classified_category == target_category:
                            enriched_post = df_classified.iloc[0].to_dict()
                            category_posts.append(enriched_post)
                
                time.sleep(2)  # Rate limiting (shorter for travel due to more subreddits)
                
            except Exception as e:
                print(f"   ⚠️  Error processing {subreddit_name}: {e}")
                continue
        
        return category_posts
    
    def _extract_category_posts(self, subreddits, target_category, threshold, limit, time_filter, existing_ids):
        """Extract posts for a specific category"""
        category_posts = []
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = subreddit.top(time_filter='week', limit=limit)
                elif time_filter == 'day':
                    posts = subreddit.top(time_filter='day', limit=limit)
                
                for post in posts:
                    # Skip if we already have this post
                    if post.id in existing_ids:
                        continue
                    
                    post_data = {
                        'id': post.id,
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'author': str(post.author),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': pd.to_datetime(post.created_utc, unit='s'),
                        'url': post.url,
                        'selftext': post.selftext[:1000],
                        'link_flair_text': post.link_flair_text,
                        'time_filter': time_filter
                    }
                    
                    # Score the post
                    df_temp = pd.DataFrame([post_data])
                    df_scored = self.ranker.calculate_popularity_score(df_temp)
                    
                    # Check if it meets threshold
                    if df_scored.iloc[0]['popularity_score'] >= threshold:
                        # Classify it
                        df_classified = self.classifier.classify_dataframe(df_scored)
                        
                        # Check if it matches target category
                        classified_category = df_classified.iloc[0]['category']
                        if classified_category == target_category:
                            # Get the enriched post data
                            enriched_post = df_classified.iloc[0].to_dict()
                            category_posts.append(enriched_post)
                            existing_ids.add(post.id)  # Avoid duplicates
                
                # Add delay between subreddits
                time.sleep(5)
                
            except Exception as e:
                print(f"   ⚠️  Error processing {subreddit_name}: {e}")
                continue
        
        return category_posts
    
    def _add_computed_fields(self, posts_df: pd.DataFrame, time_filter: str) -> pd.DataFrame:
        """Add all computed fields to posts DataFrame"""
        
        print("🔄 Adding computed fields...")
        
        # Calculate hours_old
        now = datetime.now()
        posts_df['hours_old'] = posts_df['created_utc'].apply(
            lambda x: (now - pd.to_datetime(x)).total_seconds() / 3600
        )
        
        # Calculate base_score (this may already be done by ranker)
        if 'base_score' not in posts_df.columns:
            posts_df['base_score'] = posts_df['score'] * posts_df['upvote_ratio']
        
        # Add comment multiplier and adjusted weight (if not already present)
        if 'comment_multiplier' not in posts_df.columns:
            posts_df['comment_multiplier'] = 1.0
        if 'adjusted_comment_weight' not in posts_df.columns:
            posts_df['adjusted_comment_weight'] = posts_df['num_comments'] * posts_df['comment_multiplier']
        
        # Add time bonus (if not already present)
        if 'time_bonus' not in posts_df.columns:
            posts_df['time_bonus'] = 1.0
        
        # Add empty top_comments field (comments fetched live via API)
        print("💬 Adding empty comments field (will be fetched live via API)...")
        posts_df['top_comments'] = json.dumps([])
        
        # Add sentiment analysis and travel subcategory
        print("📊 Adding sentiment analysis and travel subcategories...")
        try:
            posts_with_sentiment = self.sentiment_analyzer.analyze_sentiment(posts_df)
            if not posts_with_sentiment.empty:
                posts_df = posts_with_sentiment
            else:
                # Fallback if analyzer returns empty DataFrame
                posts_df['sentiment_score'] = 0.0
                posts_df['sentiment_label'] = 'neutral'
                posts_df['travel_subcategory'] = ''
        except Exception as e:
            print(f"⚠️  Sentiment analysis failed: {e}")
            posts_df['sentiment_score'] = 0.0
            posts_df['sentiment_label'] = 'neutral'
            posts_df['travel_subcategory'] = ''
        
        # Add travel city tracking
        print("🌍 Adding travel city tracking...")
        try:
            posts_with_cities = self.city_tracker.track_cities(posts_df)
            if not posts_with_cities.empty:
                # Merge city information into travel_subcategory if available
                for idx, row in posts_with_cities.iterrows():
                    if hasattr(row, 'cities') and row['cities']:
                        existing_subcategory = posts_df.at[idx, 'travel_subcategory']
                        city_info = f" ({', '.join(row['cities'][:3])})"  # Limit to 3 cities
                        posts_df.at[idx, 'travel_subcategory'] = existing_subcategory + city_info
        except Exception as e:
            print(f"⚠️  City tracking failed: {e}")
        
        # Add category_id (map category name to ID for database)
        category_id_mapping = {
            'Regional Travel': 1,
            'Travel Tips & Advice': 2,
            'Solo Travel': 3,
            'Budget Travel': 4,
            'General Travel': 5
        }
        posts_df['category_id'] = posts_df['category'].map(category_id_mapping).fillna(5)
        
        # Add timestamps
        posts_df['extracted_at'] = datetime.now()
        posts_df['updated_at'] = datetime.now()
        
        print(f"✅ Computed fields added to {len(posts_df)} posts")
        
        return posts_df

if __name__ == "__main__":
    import sys
    import json
    
    # Check if time filter is provided as argument  
    time_filter = 'week'  # default
    if len(sys.argv) > 1:
        if sys.argv[1] in ['day', 'daily']:
            time_filter = 'day'
        elif sys.argv[1] in ['week', 'weekly']:
            time_filter = 'week'
    
    print(f"🚀 Running Travel Database Extraction ({time_filter})...")
    
    extractor = TravelDatabaseExtractor()
    result = extractor.extract_and_save_to_database(time_filter=time_filter, base_limit=15)
    
    print(f"\n" + "=" * 60)
    print("📋 FINAL SUMMARY")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))