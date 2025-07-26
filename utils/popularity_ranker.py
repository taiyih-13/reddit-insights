import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class PopularityRankerV2:
    def __init__(self):
        # New scoring formula: upvotes * 1.0 + comments * (5.0 * subreddit_multiplier)
        self.upvote_weight = 1.0
        self.base_comment_weight = 5.0
        self.time_decay_factor = 0.05
        
        # Subreddit-specific comment multipliers based on comment scarcity
        self.subreddit_multipliers = {
            # Finance subreddits
            'Bitcoin': 3.0,                    # Comments very rare (16.9 avg ratio)
            'cryptocurrency': 2.5,             # Comments rare (8.9 avg ratio)
            'stocks': 2.0,                     # Comments somewhat rare (6.2 avg ratio)
            'daytrading': 1.8,                 # Comments somewhat rare (5.1 avg ratio)
            'forex': 1.8,                      # Comments somewhat rare (5.1 avg ratio)
            'pennystocks': 1.5,                # Comments moderately common (3.9 avg ratio)
            'investing': 1.2,                  # Comments fairly common (2.8 avg ratio)
            'ValueInvesting': 1.0,             # Comments common (2.1 avg ratio)
            'financialindependence': 0.8,      # Comments very common (1.5 avg ratio)
            'CryptoMarkets': 0.6,              # Comments extremely common (1.0 avg ratio)
            'wallstreetbets': 1.5,             # Default for meme-heavy content
            'SecurityAnalysis': 1.0,           # Default for analysis content
            'thetagang': 1.2,                  # Default for strategy content
            'personalfinance': 0.8,            # Default for help content
            
            # Entertainment subreddits
            'movies': 1.8,                     # General movies discussion
            'television': 1.8,                 # General TV discussion
            'netflix': 1.2,                    # Platform-specific content
            'hulu': 1.2,                       # Platform-specific content
            'DisneyPlus': 1.5,                 # Platform-specific content
            'PrimeVideo': 1.5,                 # Platform-specific content
            'HBOMax': 1.5,                     # Platform-specific content
            'AppleTVPlus': 1.8,                # Smaller platform, comments rarer
            'anime': 1.0,                      # Active community with regular comments
            'animesuggest': 0.8,               # Recommendation posts get many comments
            'horror': 1.5,                     # Genre-specific discussion
            'horrormovies': 1.5,               # Genre-specific discussion
            'MovieSuggestions': 0.8,           # Recommendation posts get many comments
            'televisionsuggestions': 0.8,      # Recommendation posts get many comments
            'NetflixBestOf': 1.0,              # Curated content with decent engagement
            'documentaries': 1.8,              # Niche content, fewer comments
            'tipofmytongue': 0.6,              # Help posts get many responses
            'ifyoulikeblank': 0.8,             # Recommendation posts get many comments
            'criterion': 2.0,                  # Niche cinephile community
            'truefilm': 2.0,                   # Serious film discussion, fewer comments
            'flicks': 1.5,                     # General movie discussion
            'letterboxd': 1.8                  # Film diary/review community
        }
        
    def calculate_popularity_score(self, df):
        """
        Calculate popularity score using subreddit-aware formula:
        - Upvotes * 1.0 + Comments * (5.0 * subreddit_multiplier)
        - Time decay bonus for newer posts
        - Engagement bonus for high comment-to-upvote ratio
        """
        df = df.copy()
        
        # Calculate subreddit-adjusted comment weights
        df['comment_multiplier'] = df['subreddit'].map(self.subreddit_multipliers).fillna(1.0)
        df['adjusted_comment_weight'] = self.base_comment_weight * df['comment_multiplier']
        
        # Basic absolute score with subreddit adjustment
        df['base_score'] = (df['score'] * self.upvote_weight) + (df['num_comments'] * df['adjusted_comment_weight'])
        
        # Calculate engagement ratio (comments per upvote)
        df['engagement_ratio'] = df['num_comments'] / (df['score'] + 1)  # +1 to avoid division by zero
        
        # Time decay: newer posts get slight boost
        now = datetime.now()
        df['hours_old'] = df['created_utc'].apply(
            lambda x: (now - x).total_seconds() / 3600 if pd.notnull(x) else 0
        )
        
        # Time bonus (1.0 for new posts, slight decay with age)
        df['time_bonus'] = np.exp(-self.time_decay_factor * df['hours_old'] / 24)
        
        # Apply time bonus to base score
        df['popularity_score'] = df['base_score'] * df['time_bonus']
        
        # Add engagement bonus for posts with high discussion relative to upvotes
        # Posts with >0.2 comments/upvote get 10% bonus
        engagement_bonus = np.where(df['engagement_ratio'] > 0.2, df['base_score'] * 0.1, 0)
        df['popularity_score'] += engagement_bonus
        
        return df
    
    def apply_filters(self, df, min_popularity=100):
        """
        Filter posts using absolute popularity threshold
        
        Args:
            min_popularity: Minimum absolute popularity score
                          (roughly equivalent to min upvotes*0.7 + comments*0.3)
        """
        initial_count = len(df)
        
        # Apply single popularity filter
        filtered_df = df[df['popularity_score'] >= min_popularity].copy()
        
        filtered_count = len(filtered_df)
        print(f"Applied popularity threshold: {min_popularity}")
        print(f"Filtered out {initial_count - filtered_count} low-engagement posts")
        print(f"Remaining: {filtered_count} high-signal posts")
        
        return filtered_df
    
    def get_top_posts(self, df, min_threshold=100, top_n=100):
        """
        Get top N posts using absolute threshold filtering
        
        Args:
            min_threshold: Minimum popularity score to qualify
            top_n: Maximum number of posts to return
        """
        # Calculate popularity scores
        df_scored = self.calculate_popularity_score(df)
        
        # Apply absolute threshold filter
        df_filtered = self.apply_filters(df_scored, min_popularity=min_threshold)
        
        # Sort by popularity score and get top N
        if len(df_filtered) > top_n:
            top_posts = df_filtered.nlargest(top_n, 'popularity_score')
        else:
            top_posts = df_filtered.sort_values('popularity_score', ascending=False)
        
        return top_posts
    
    def analyze_ranking_distribution(self, df):
        """
        Analyze the distribution of ranking scores and metrics
        """
        print("\n=== POPULARITY RANKING ANALYSIS (V2) ===")
        print(f"Total posts analyzed: {len(df)}")
        
        print(f"\nUpvote Distribution:")
        print(f"  Mean: {df['score'].mean():.1f}")
        print(f"  Median: {df['score'].median():.1f}")
        print(f"  Range: {df['score'].min()} - {df['score'].max()}")
        
        print(f"\nComment Distribution:")
        print(f"  Mean: {df['num_comments'].mean():.1f}")
        print(f"  Median: {df['num_comments'].median():.1f}")
        print(f"  Range: {df['num_comments'].min()} - {df['num_comments'].max()}")
        
        print(f"\nPopularity Score Distribution:")
        print(f"  Mean: {df['popularity_score'].mean():.1f}")
        print(f"  Median: {df['popularity_score'].median():.1f}")
        print(f"  Range: {df['popularity_score'].min():.1f} - {df['popularity_score'].max():.1f}")
        
        print(f"\nEngagement Analysis:")
        print(f"  Mean engagement ratio: {df['engagement_ratio'].mean():.3f}")
        print(f"  High engagement posts (>0.2): {(df['engagement_ratio'] > 0.2).sum()}")
        
        # Show score thresholds with new formula
        print(f"\nScore Threshold Examples (with new formula):")
        print(f"  Score 100 ≈ 100 upvotes OR 20 comments (1.0x) OR 7 comments (3.0x) OR mix")
        print(f"  Score 200 ≈ 200 upvotes OR 40 comments (1.0x) OR 13 comments (3.0x) OR mix") 
        print(f"  Score 500 ≈ 500 upvotes OR 100 comments (1.0x) OR 33 comments (3.0x) OR mix")
        
        # Show top 10 posts
        print(f"\nTop 10 Posts by Popularity Score:")
        top_10 = df.nlargest(10, 'popularity_score')[['title', 'score', 'num_comments', 'popularity_score']].round(1)
        for idx, row in top_10.iterrows():
            print(f"  {row['popularity_score']:.1f}: {row['title'][:50]}... ({row['score']} ↑, {row['num_comments']} 💬)")

def test_new_scoring():
    """Quick test of the new subreddit-aware scoring system"""
    sample_data = {
        'title': [
            'Bitcoin discussion',
            'Investing advice', 
            'High engagement post',
            'Financial independence'
        ],
        'subreddit': ['Bitcoin', 'investing', 'wallstreetbets', 'financialindependence'],
        'score': [100, 200, 800, 50],
        'num_comments': [5, 20, 100, 50],
        'created_utc': [datetime.now() - timedelta(hours=i) for i in range(4)]
    }
    
    df = pd.DataFrame(sample_data)
    ranker = PopularityRankerV2()
    df_scored = ranker.calculate_popularity_score(df)
    
    print("=== SUBREDDIT-AWARE SCORING TEST ===")
    print(df_scored[['title', 'subreddit', 'score', 'num_comments', 'comment_multiplier', 'base_score', 'popularity_score']].round(1))
    print(f"\nPosts passing threshold 100: {(df_scored['popularity_score'] >= 100).sum()}/4")
    
    print("\n=== SCORING BREAKDOWN ===")
    for _, row in df_scored.iterrows():
        upvote_contrib = row['score'] * 1.0
        comment_contrib = row['num_comments'] * row['adjusted_comment_weight']
        print(f"{row['title']}: {upvote_contrib:.0f} (upvotes) + {comment_contrib:.0f} (comments) = {row['base_score']:.0f}")

if __name__ == "__main__":
    test_new_scoring()