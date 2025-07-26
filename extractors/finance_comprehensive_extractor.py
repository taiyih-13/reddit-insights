import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from utils.popularity_ranker import PopularityRankerV2

load_dotenv()

class FinanceComprehensiveExtractor:
    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        self.finance_subreddits = [
            'wallstreetbets', 'investing', 'stocks', 'SecurityAnalysis', 
            'ValueInvesting', 'options', 'pennystocks', 'daytrading', 
            'SwingTrading', 'forex', 'cryptocurrency', 'Bitcoin', 
            'CryptoMarkets', 'thetagang', 'SPACs', 'financialindependence',
            'personalfinance'
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
        """Extract comprehensive data from all finance subreddits"""
        print(f"Extracting posts from past {time_filter} across all finance subreddits...")
        print(f"Targeting {len(self.finance_subreddits)} subreddits with {limit_per_subreddit} posts each")
        print("=" * 60)
        
        total_posts = 0
        subreddit_stats = {}
        
        # First pass: Count posts from each subreddit
        for subreddit_name in self.finance_subreddits:
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
        for subreddit_name in self.finance_subreddits:
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
                        'post_id': post.id
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
        
        # Rank posts by popularity using new absolute scoring
        ranker = PopularityRankerV2()
        top_posts = ranker.get_top_posts(df, min_threshold=100, top_n=top_n)
        
        # Analysis
        ranker.analyze_ranking_distribution(top_posts)
        
        # Save results
        top_posts.to_csv('assets/top_reddit_posts_ranked.csv', index=False)
        print(f"\nTop {len(top_posts)} posts saved to 'top_reddit_posts_ranked.csv'")
        
        return top_posts

if __name__ == "__main__":
    extractor = FinanceComprehensiveExtractor()
    
    # Extract and rank top posts from past week - FULL DATASET
    print("=== EXTRACTING TOP HIGH-SIGNAL POSTS FROM FULL DATASET ===")
    top_posts = extractor.extract_and_rank_posts(
        time_filter='week', 
        limit_per_subreddit=100,  # Full extraction 
        top_n=150  # Get more posts since we're filtering better
    )