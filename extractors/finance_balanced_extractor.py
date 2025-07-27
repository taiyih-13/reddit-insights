import pandas as pd
import time
from .finance_extractor import FinanceComprehensiveExtractor
from utils.popularity_ranker import PopularityRankerV2
from classifiers.finance_classifier import FinanceClassifier
from utils.comment_fetcher import fetch_top_comments

class FinanceBalancedExtractor:
    def __init__(self):
        self.extractor = FinanceComprehensiveExtractor()
        self.ranker = PopularityRankerV2()
        self.classifier = FinanceClassifier()
        
        # Target minimums for each category
        self.category_minimums = {
            'Analysis & Education': 25,
            'Market News & Politics': 25,
            'Questions & Help': 20,
            'Memes & Entertainment': 20,
            'Community Discussion': 15,
            'Personal Trading Stories': 50  # Still allow plenty
        }
        
        # Category-specific popularity thresholds (updated for new scoring system)
        # With new formula: upvotes * 1.0 + comments * (5.0 * subreddit_multiplier)
        # Middle ground between original (400) and low (200) thresholds
        self.category_thresholds = {
            'Personal Trading Stories': 300,  # Keep high standards (middle of 400/200)
            'Analysis & Education': 150,      # Lower for valuable content (middle of 200/100)
            'Market News & Politics': 225,    # (middle of 300/150)
            'Questions & Help': 112,          # (middle of 150/75)
            'Memes & Entertainment': 187,     # (middle of 250/125)
            'Community Discussion': 90        # Lowest threshold (middle of 120/60)
        }
        
        # Subreddits that are rich in underrepresented categories
        self.category_rich_subreddits = {
            'Analysis & Education': ['SecurityAnalysis', 'ValueInvesting', 'investing', 'thetagang'],
            'Market News & Politics': ['stocks', 'cryptocurrency', 'Bitcoin'],
            'Questions & Help': ['personalfinance', 'investing'],
            'Memes & Entertainment': ['wallstreetbets', 'cryptocurrency'],
            'Community Discussion': ['wallstreetbets', 'pennystocks']
        }
    
    def extract_balanced_posts(self, time_filter='week', base_limit=100):
        """
        Extract posts with balanced category representation
        """
        print("=== BALANCED EXTRACTION SYSTEM ===")
        print("Phase 1: Initial high-quality extraction...")
        
        # Phase 1: Normal extraction with high standards
        all_posts = []
        subreddit_counts = {}
        total_raw_posts = 0
        
        for subreddit_name in self.extractor.finance_subreddits:
            subreddit_post_count = 0
            max_retries = 2
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        print(f"  Retrying r/{subreddit_name} (attempt {attempt + 1})...")
                        time.sleep(2)  # Brief delay before retry
                    else:
                        print(f"  Extracting from r/{subreddit_name}...")
                    
                    subreddit = self.extractor.reddit.subreddit(subreddit_name)
                    
                    if time_filter == 'week':
                        posts = list(subreddit.top(time_filter='week', limit=base_limit))
                    elif time_filter == 'day':
                        posts = list(subreddit.top(time_filter='day', limit=base_limit))
                    
                    subreddit_post_count = len(posts)
                    print(f"    Got {subreddit_post_count} posts")
                    
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
                            'selftext': post.selftext[:1000],
                            'link_flair_text': post.link_flair_text,
                            'post_id': post.id,
                            'top_comments': '[]'  # Fetched on-demand via post_id
                        }
                        all_posts.append(post_data)
                    
                    break  # Success, break out of retry loop
                    
                except Exception as e:
                    if attempt == max_retries:
                        print(f"    Error with r/{subreddit_name} after {max_retries + 1} attempts: {e}")
                        subreddit_post_count = 0
                    else:
                        print(f"    Attempt {attempt + 1} failed: {e}")
            
            subreddit_counts[subreddit_name] = subreddit_post_count
            total_raw_posts += subreddit_post_count
        
        print(f"\nRaw extraction summary:")
        print(f"Total raw posts extracted: {total_raw_posts}")
        if total_raw_posts < len(self.extractor.finance_subreddits) * base_limit * 0.8:  # Less than 80% expected
            print("⚠️  Warning: Lower than expected raw post count - possible API issues")
        
        if not all_posts:
            print("No posts extracted!")
            return pd.DataFrame()
        
        # Convert to DataFrame and get initial high-quality posts
        df_all = pd.DataFrame(all_posts)
        df_scored = self.ranker.calculate_popularity_score(df_all)
        
        # Show filtering impact
        print(f"Applied popularity threshold: 300")
        df_initial = self.ranker.apply_filters(df_scored, min_popularity=300)
        filtered_out = len(df_scored) - len(df_initial)
        print(f"Filtered out {filtered_out} low-engagement posts")
        print(f"Remaining: {len(df_initial)} high-signal posts")
        
        print(f"\nPhase 1 complete: {len(df_initial)} high-quality posts extracted")
        
        # Classify posts
        df_classified = self.classifier.classify_dataframe(df_initial)
        
        # Analyze current distribution
        current_counts = df_classified['category'].value_counts()
        print(f"\nCurrent category distribution:")
        for category, count in current_counts.items():
            target = self.category_minimums.get(category, 0)
            status = "✅" if count >= target else f"❌ Need {target - count} more"
            print(f"  {category}: {count} posts (target: {target}) {status}")
        
        # Phase 2: Fill gaps for underrepresented categories
        print(f"\nPhase 2: Filling category gaps...")
        final_posts = df_classified.copy()
        
        for category, target_count in self.category_minimums.items():
            current_count = len(final_posts[final_posts['category'] == category])
            
            if current_count < target_count:
                needed = target_count - current_count
                print(f"\n  Seeking {needed} more {category} posts...")
                
                # Get category-specific threshold
                threshold = self.category_thresholds.get(category, 50)
                
                # Target specific subreddits for this category
                target_subreddits = self.category_rich_subreddits.get(category, self.extractor.finance_subreddits)
                
                category_posts = self._extract_category_specific(
                    target_subreddits, 
                    category, 
                    threshold,
                    needed * 2,  # Extract more to have options
                    time_filter,
                    existing_ids=set(final_posts['post_id'])
                )
                
                if len(category_posts) > 0:
                    # Add best posts for this category
                    best_category_posts = category_posts.nlargest(needed, 'popularity_score')
                    final_posts = pd.concat([final_posts, best_category_posts], ignore_index=True)
                    print(f"    Added {len(best_category_posts)} {category} posts")
                else:
                    print(f"    No additional {category} posts found")
        
        # Final ranking and deduplication
        final_posts = final_posts.drop_duplicates(subset=['post_id'])
        final_posts = final_posts.sort_values('popularity_score', ascending=False)
        
        print(f"\n=== FINAL BALANCED RESULTS ===")
        print(f"Total posts: {len(final_posts)}")
        
        final_counts = final_posts['category'].value_counts()
        for category, count in final_counts.items():
            target = self.category_minimums.get(category, 0)
            percentage = (count / len(final_posts)) * 100
            status = "✅" if count >= target else "❌"
            print(f"  {category}: {count} posts ({percentage:.1f}%) {status}")
        
        return final_posts
    
    def _extract_category_specific(self, subreddits, target_category, threshold, limit, time_filter, existing_ids):
        """Extract posts targeting a specific category"""
        category_posts = []
        
        for subreddit_name in subreddits:
            try:
                subreddit = self.extractor.reddit.subreddit(subreddit_name)
                
                if time_filter == 'week':
                    posts = subreddit.top(time_filter='week', limit=limit)
                elif time_filter == 'day':
                    posts = subreddit.top(time_filter='day', limit=limit)
                
                for post in posts:
                    # Skip if we already have this post
                    if post.id in existing_ids:
                        continue
                    
                    post_data = {
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
                        'post_id': post.id
                    }
                    
                    # Score the post
                    df_temp = pd.DataFrame([post_data])
                    df_scored = self.ranker.calculate_popularity_score(df_temp)
                    
                    # Check if it meets threshold
                    if df_scored.iloc[0]['popularity_score'] >= threshold:
                        # Classify it
                        df_classified = self.classifier.classify_dataframe(df_scored)
                        
                        # Check if it's the category we want
                        if df_classified.iloc[0]['category'] == target_category:
                            category_posts.append(df_classified.iloc[0])
                            
            except Exception as e:
                print(f"      Error extracting from r/{subreddit_name}: {e}")
        
        return pd.DataFrame(category_posts) if category_posts else pd.DataFrame()

def run_balanced_extraction():
    """Run the balanced extraction system"""
    extractor = BalancedExtractor()
    
    # Extract balanced posts
    balanced_df = extractor.extract_balanced_posts(time_filter='week', base_limit=100)
    
    if len(balanced_df) > 0:
        # Save results
        balanced_df.to_csv('assets/balanced_reddit_posts.csv', index=False)
        print(f"\nBalanced extraction complete!")
        print(f"Results saved to 'balanced_reddit_posts.csv'")
        
        # Show top posts from each category
        print(f"\n=== TOP POST FROM EACH CATEGORY ===")
        for category in balanced_df['category'].unique():
            top_post = balanced_df[balanced_df['category'] == category].iloc[0]
            print(f"{category}:")
            print(f"  '{top_post['title'][:60]}...' ({top_post['score']} ↑, {top_post['num_comments']} 💬)")
    else:
        print("No posts extracted!")

if __name__ == "__main__":
    import sys
    
    # Check if time filter is provided as argument
    time_filter = 'week'  # default
    if len(sys.argv) > 1:
        if sys.argv[1] in ['day', 'daily']:
            time_filter = 'day'
        elif sys.argv[1] in ['week', 'weekly']:
            time_filter = 'week'
    
    print(f"Running {time_filter} extraction...")
    
    extractor = BalancedExtractor()
    balanced_df = extractor.extract_balanced_posts(time_filter=time_filter, base_limit=100)
    
    if len(balanced_df) > 0:
        # Save results with time filter in filename
        filename = f'{time_filter}_reddit_posts.csv'
        balanced_df.to_csv(f'assets/{filename}', index=False)
        print(f"\nBalanced extraction complete!")
        print(f"Results saved to '{filename}'")
        
        # Show top posts from each category
        print(f"\n=== TOP POST FROM EACH CATEGORY ===")
        for category in balanced_df['category'].unique():
            top_post = balanced_df[balanced_df['category'] == category].iloc[0]
            print(f"{category}:")
            print(f"  '{top_post['title'][:60]}...' ({top_post['score']} ↑, {top_post['num_comments']} 💬)")
    else:
        print("No posts extracted!")