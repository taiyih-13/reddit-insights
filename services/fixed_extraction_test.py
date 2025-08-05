#!/usr/bin/env python3
"""
Fixed Extraction Test - Test extraction with schema-compatible database operations
"""

import sys
import os
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.finance_database_extractor import FinanceDatabaseExtractor
from services.fixed_database_service import save_posts_basic_schema
from services.enhanced_database_service import get_enhanced_db_service

def test_fixed_extraction():
    """Test extraction with fixed database schema handling"""
    
    print("🔧 TESTING FIXED EXTRACTION")
    print("=" * 50)
    
    # Get current post count
    db = get_enhanced_db_service()
    initial_finance_posts = db.get_posts_by_domain('finance', 'week')
    initial_count = len(initial_finance_posts)
    
    print(f"Initial finance posts in database: {initial_count}")
    
    # Test extracting from one subreddit
    extractor = FinanceDatabaseExtractor()
    reddit = extractor.reddit
    
    print("\n📡 Extracting new posts from r/investing...")
    
    # Get existing IDs to avoid duplicates
    existing_ids = set(initial_finance_posts['id'].tolist()) if not initial_finance_posts.empty else set()
    print(f"Found {len(existing_ids)} existing post IDs")
    
    # Extract posts manually with basic schema
    new_posts = []
    subreddit = reddit.subreddit('investing')
    
    try:
        for post in subreddit.hot(limit=10):
            if post.id not in existing_ids:
                # Convert timestamp to ISO format for Supabase
                created_datetime = datetime.fromtimestamp(post.created_utc)
                
                post_data = {
                    'id': post.id,
                    'subreddit': post.subreddit.display_name,
                    'title': post.title,
                    'author': str(post.author) if post.author else '[deleted]',
                    'score': post.score,
                    'upvote_ratio': getattr(post, 'upvote_ratio', 0.0),
                    'num_comments': post.num_comments,
                    'created_utc': created_datetime.isoformat(),  # Convert to ISO string
                    'url': post.url,
                    'selftext': post.selftext or '',
                    'link_flair_text': getattr(post, 'link_flair_text', '') or '',
                    'time_filter': 'week',
                    'category_id': 1,  # Default category
                    'classification_confidence': 0.8,
                    'popularity_score': float(post.score),
                    'engagement_ratio': float(post.num_comments) / max(float(post.score), 1.0),
                    'time_bonus': 1.0,
                    'extracted_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                new_posts.append(post_data)
                
                print(f"  ✅ New post: {post.id} - \"{post.title[:50]}...\"")
                
                if len(new_posts) >= 5:  # Limit test to 5 posts
                    break
    
        if new_posts:
            print(f"\n💾 Saving {len(new_posts)} new posts with fixed schema...")
            
            # Convert to DataFrame
            new_posts_df = pd.DataFrame(new_posts)
            
            # Save with fixed schema
            result = save_posts_basic_schema(new_posts_df)
            
            print(f"\n📊 RESULTS:")
            print(f"  Posts processed: {result.get('total_processed', 0)}")
            print(f"  Posts inserted: {result.get('inserted_count', 0)}")
            print(f"  Errors: {result.get('error_count', 0)}")
            
            if result.get('error_message'):
                print(f"  Error details: {result['error_message']}")
            
            # Verify the posts were added
            updated_finance_posts = db.get_posts_by_domain('finance', 'week')
            final_count = len(updated_finance_posts)
            new_posts_added = final_count - initial_count
            
            print(f"\n🎯 VERIFICATION:")
            print(f"  Initial posts: {initial_count}")
            print(f"  Final posts: {final_count}")
            print(f"  New posts added: {new_posts_added}")
            
            if new_posts_added > 0:
                print("  ✅ SUCCESS: New posts were added to database!")
            else:
                print("  ❌ ISSUE: No new posts were added")
                
        else:
            print("  ⚠️  No new posts found (all are duplicates)")
            
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fixed_extraction()