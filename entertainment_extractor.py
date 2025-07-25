import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from popularity_ranker_v2 import PopularityRankerV2

load_dotenv()

class EntertainmentRedditExtractor:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        self.entertainment_subreddits = [
            # Platform-specific (6)
            'netflix', 'hulu', 'DisneyPlus', 'PrimeVideo', 'HBOMax', 'AppleTVPlus',
            # General content (3) 
            'movies', 'television', 'letterboxd',
            # Genre-specific (4)
            'anime', 'animesuggest', 'horror', 'horrormovies', 
            # Recommendation-focused (3)
            'MovieSuggestions', 'televisionsuggestions', 'NetflixBestOf',
            # Documentary (1)
            'documentaries'
        ]
    
    def get_weekly_post_count(self, subreddit_name, limit=1000):
        """Get total number of posts from past week for counting"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            posts = list(subreddit.new(limit=limit))
            return len(posts)
        except Exception as e:
            print(f"Error counting posts from r/{subreddit_name}: {e}")
            return 0
    
    def extract_comprehensive_data(self, time_filter='week', limit_per_subreddit=100):
        """Extract comprehensive data from all entertainment subreddits"""
        print(f"Extracting posts from past {time_filter} across all entertainment subreddits...")
        print(f"Targeting {len(self.entertainment_subreddits)} subreddits with {limit_per_subreddit} posts each")
        print("=" * 60)
        
        total_posts = 0
        subreddit_stats = {}
        
        # First pass: Count posts from each subreddit
        for subreddit_name in self.entertainment_subreddits:
            print(f"Counting posts from r/{subreddit_name}...")
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    post_count = len(list(subreddit.top(time_filter='week', limit=limit_per_subreddit)))
                elif time_filter == 'day':
                    post_count = len(list(subreddit.top(time_filter='day', limit=limit_per_subreddit)))
                
                subreddit_stats[subreddit_name] = post_count
                total_posts += post_count
                print(f"  Found {post_count} posts")
                
            except Exception as e:
                print(f"  Error with r/{subreddit_name}: {e}")
                subreddit_stats[subreddit_name] = 0
        
        print(f"\nTotal posts available: {total_posts}")
        print("=" * 60)
        
        # Second pass: Extract actual post data
        all_posts = []
        
        for subreddit_name in self.entertainment_subreddits:
            if subreddit_stats[subreddit_name] == 0:
                continue
                
            print(f"Extracting from r/{subreddit_name}...")
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = list(subreddit.top(time_filter='week', limit=limit_per_subreddit))
                elif time_filter == 'day':
                    posts = list(subreddit.top(time_filter='day', limit=limit_per_subreddit))
                
                for post in posts:
                    post_data = {
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
                        'post_id': post.id
                    }
                    all_posts.append(post_data)
                
                print(f"  Extracted {len(posts)} posts")
                
            except Exception as e:
                print(f"  Error extracting from r/{subreddit_name}: {e}")
        
        if all_posts:
            df = pd.DataFrame(all_posts)
            print(f"\nSuccessfully extracted {len(df)} total posts")
            return df
        else:
            print("No posts extracted!")
            return pd.DataFrame()

def test_entertainment_extraction():
    """Test function for entertainment extraction"""
    print("🎬 Testing Entertainment Reddit Extractor")
    print("=" * 50)
    
    extractor = EntertainmentRedditExtractor()
    
    print(f"📺 Configured {len(extractor.entertainment_subreddits)} entertainment subreddits:")
    for i, subreddit in enumerate(extractor.entertainment_subreddits, 1):
        print(f"  {i:2d}. r/{subreddit}")
    
    print(f"\n🔍 Testing API connectivity...")
    
    # Test with a small extraction
    try:
        df = extractor.extract_comprehensive_data(time_filter='week', limit_per_subreddit=10)
        
        if len(df) > 0:
            print(f"\n✅ Success! Extracted {len(df)} sample posts")
            print(f"\nSample titles:")
            for i, title in enumerate(df['title'].head(5), 1):
                print(f"  {i}. {title[:80]}...")
                
            print(f"\nSubreddit breakdown:")
            subreddit_counts = df['subreddit'].value_counts()
            for subreddit, count in subreddit_counts.items():
                print(f"  r/{subreddit}: {count} posts")
        else:
            print("❌ No posts extracted!")
            
    except Exception as e:
        print(f"❌ Extraction failed: {e}")

if __name__ == "__main__":
    test_entertainment_extraction()