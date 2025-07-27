import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from utils.popularity_ranker import PopularityRankerV2
from utils.comment_fetcher import fetch_top_comments

load_dotenv()

class RegionalTravelExtractor:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        self.regional_travel_subreddits = [
            # Europe (4 subreddits)
            'europe', 'travel_Europe', 'ItalyTravel', 'germany',
            # Asia (5 subreddits)
            'JapanTravel', 'Thailand', 'IndiaTravel', 'SouthEastAsia', 'china',
            # Americas (4 subreddits)
            'MexicoTravel', 'canada', 'VisitingIceland', 'argentina',
            # Other regions (3 subreddits)
            'australia', 'newzealand', 'southafrica'
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
        """Extract comprehensive data from all regional travel subreddits"""
        print(f"Extracting posts from past {time_filter} across all regional travel subreddits...")
        print(f"Targeting {len(self.regional_travel_subreddits)} subreddits with {limit_per_subreddit} posts each")
        print("=" * 60)
        
        total_posts = 0
        subreddit_stats = {}
        
        # First pass: Count posts from each subreddit
        for subreddit_name in self.regional_travel_subreddits:
            print(f"Counting posts from r/{subreddit_name}...")
            try:
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = list(subreddit.top(time_filter='week', limit=limit_per_subreddit))
                elif time_filter == 'day':
                    posts = list(subreddit.top(time_filter='day', limit=limit_per_subreddit))
                
                post_count = len(posts)
                subreddit_stats[subreddit_name] = {
                    'post_count': post_count,
                    'subscribers': subreddit.subscribers
                }
                total_posts += post_count
                print(f"  r/{subreddit_name}: {post_count} posts ({subreddit.subscribers:,} subscribers)")
                
            except Exception as e:
                print(f"  Error with r/{subreddit_name}: {e}")
                subreddit_stats[subreddit_name] = {'post_count': 0, 'subscribers': 0}
        
        print("=" * 60)
        print(f"TOTAL POSTS FROM PAST {time_filter.upper()}: {total_posts:,}")
        print("=" * 60)
        
        # Summary by subreddit
        print("\nSUBREDDIT BREAKDOWN:")
        sorted_stats = sorted(subreddit_stats.items(), key=lambda x: x[1]['post_count'], reverse=True)
        for subreddit, stats in sorted_stats:
            print(f"  r/{subreddit}: {stats['post_count']} posts | {stats['subscribers']:,} subs")
        
        return total_posts, subreddit_stats
    
    def extract_and_rank_posts(self, time_filter='week', limit_per_subreddit=25, top_n=100):
        """Extract posts and rank them by popularity"""
        print(f"Extracting and ranking posts from past {time_filter}...")
        
        all_posts = []
        for subreddit_name in self.regional_travel_subreddits:
            try:
                print(f"Extracting from r/{subreddit_name}...")
                subreddit = self.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = subreddit.top(time_filter='week', limit=limit_per_subreddit)
                elif time_filter == 'day':
                    posts = subreddit.top(time_filter='day', limit=limit_per_subreddit)
                
                for post in posts:
                    post_data = {
                        'subreddit': subreddit_name,
                        'title': post.title,
                        'author': str(post.author),
                        'score': post.score,
                        'upvote_ratio': post.upvote_ratio,
                        'num_comments': post.num_comments,
                        'created_utc': datetime.fromtimestamp(post.created_utc),
                        'url': post.url,
                        'selftext': post.selftext[:1000],  # First 1000 chars
                        'link_flair_text': post.link_flair_text,
                        'distinguished': post.distinguished,
                        'stickied': post.stickied,
                        'over_18': post.over_18,
                        'spoiler': post.spoiler,
                        'domain': post.domain,
                        'post_id': post.id,
                        'top_comments': '[]'  # Disabled for API efficiency (would be 1 call per post)
                    }
                    all_posts.append(post_data)
                    
            except Exception as e:
                print(f"Error extracting from r/{subreddit_name}: {e}")
        
        if not all_posts:
            print("No posts extracted!")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_posts)
        print(f"\nExtracted {len(df)} total posts")
        
        # Rank posts by popularity using absolute scoring
        ranker = PopularityRankerV2()
        top_posts = ranker.get_top_posts(df, min_threshold=10, top_n=top_n)
        
        # Analysis
        ranker.analyze_ranking_distribution(top_posts)
        
        return top_posts

class RegionalTravelBalancedExtractor:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Travel-focused subreddits organized by region - active travel communities only
        self.subreddit_categories = {
            'asian_travel': ['JapanTravel', 'ThailandTourism', 'IndiaTravel', 'SouthEastAsia', 'koreatravel', 'chinatravel', 'VietnamTravel'],
            'european_travel': ['travel_Europe', 'ItalyTravel', 'VisitingIceland', 'uktravel', 'Spain', 'france', 'germany'],
            'american_travel': ['MexicoTravel', 'usatravel', 'CanadaTravel', 'caribbeantravel', 'braziltravel', 'argentina'],
            'adventure_travel': ['backpacking', 'camping', 'hiking', 'Outdoors', 'WildernessBackpacking', 'solotravel', 'Shoestring']
        }
        
        # Flatten all subreddits
        self.all_subreddits = []
        for category_subs in self.subreddit_categories.values():
            self.all_subreddits.extend(category_subs)
    
    def extract_balanced_posts(self, time_filter='week', base_limit=25):
        """Extract balanced posts across all regional travel subcategories"""
        print(f"🌍 Regional Travel Balanced Extraction - {time_filter}")
        print("=" * 50)
        
        all_posts = []
        category_stats = {}
        
        for category, subreddits in self.subreddit_categories.items():
            print(f"\n🗺️ {category.upper().replace('_', ' & ')}:")
            category_posts = []
            
            for subreddit_name in subreddits:
                try:
                    print(f"  Extracting from r/{subreddit_name}...")
                    subreddit = self.reddit.subreddit(subreddit_name)
                    
                    if time_filter == 'week':
                        posts = list(subreddit.top(time_filter='week', limit=base_limit))
                    elif time_filter == 'day':
                        posts = list(subreddit.top(time_filter='day', limit=base_limit))
                    
                    for post in posts:
                        post_data = {
                            'subreddit': subreddit_name,
                            'title': post.title,
                            'author': str(post.author),
                            'score': post.score,
                            'upvote_ratio': post.upvote_ratio,
                            'num_comments': post.num_comments,
                            'created_utc': datetime.fromtimestamp(post.created_utc),
                            'url': post.url,
                            'selftext': post.selftext[:1000],
                            'link_flair_text': post.link_flair_text,
                            'distinguished': post.distinguished,
                            'stickied': post.stickied,
                            'over_18': post.over_18,
                            'spoiler': post.spoiler,
                            'domain': post.domain,
                            'post_id': post.id,
                            'top_comments': '[]',  # Disabled for API efficiency (would be 1 call per post)
                            'regional_subcategory': category  # Add subcategory for classification
                        }
                        category_posts.append(post_data)
                    
                    print(f"    ✅ {len(posts)} posts from r/{subreddit_name}")
                    
                except Exception as e:
                    print(f"    ❌ Error with r/{subreddit_name}: {e}")
            
            category_stats[category] = len(category_posts)
            all_posts.extend(category_posts)
            print(f"  📊 {category.upper().replace('_', ' & ')}: {len(category_posts)} total posts")
        
        if not all_posts:
            print("❌ No posts extracted!")
            return pd.DataFrame()
        
        # Convert to DataFrame and rank
        df = pd.DataFrame(all_posts)
        print(f"\n📈 Extracted {len(df)} total posts")
        
        # Apply popularity ranking (high top_n limit to get full dataset)
        ranker = PopularityRankerV2()
        top_posts = ranker.get_top_posts(df, min_threshold=5, top_n=10000)
        
        # Category distribution analysis
        print(f"\n📋 REGIONAL DISTRIBUTION:")
        for category, count in category_stats.items():
            percentage = (count / len(df)) * 100 if len(df) > 0 else 0
            print(f"  {category.title().replace('_', ' & ')}: {count} posts ({percentage:.1f}%)")
        
        return top_posts

if __name__ == "__main__":
    # Test the balanced extractor
    extractor = RegionalTravelBalancedExtractor()
    
    print("=== EXTRACTING REGIONAL TRAVEL POSTS ===")
    posts_df = extractor.extract_balanced_posts(
        time_filter='week',
        base_limit=30
    )
    
    if not posts_df.empty:
        print(f"\n✅ Successfully extracted {len(posts_df)} quality regional travel posts")
        print(f"📁 Ready to save to assets/week_regional_travel_posts.csv")
    else:
        print("\n❌ No posts met quality standards")