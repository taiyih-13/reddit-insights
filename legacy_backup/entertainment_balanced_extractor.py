import pandas as pd
import time
from entertainment_extractor import EntertainmentRedditExtractor
from popularity_ranker_v2 import PopularityRankerV2
from entertainment_classifier import EntertainmentClassifier

class EntertainmentBalancedExtractor:
    def __init__(self):
        self.extractor = EntertainmentRedditExtractor()
        self.ranker = PopularityRankerV2()
        self.classifier = EntertainmentClassifier()
        
        # Target minimums for each entertainment genre
        self.category_minimums = {
            'Horror': 15,
            'Comedy': 15, 
            'Action': 12,
            'Drama': 20,     # Allow more drama (common fallback)
            'Sci-Fi': 10,
            'Fantasy': 8,
            'Romance': 8,
            'Anime': 12,
            'Documentary': 10,
            'Bad Movies': 5  # Niche category
        }
        
        # Genre-specific popularity thresholds
        # Entertainment posts generally have lower engagement than finance
        self.category_thresholds = {
            'Horror': 80,        # Horror fans are engaged
            'Comedy': 100,       # Comedy gets good engagement  
            'Action': 120,       # Action/blockbusters popular
            'Drama': 60,         # Drama is common, lower bar
            'Sci-Fi': 90,        # Sci-fi has dedicated fanbase
            'Fantasy': 85,       # Fantasy fans engaged
            'Romance': 70,       # Romance niche but engaged
            'Anime': 110,        # Anime very engaged community
            'Documentary': 75,   # Documentary fans engaged
            'Bad Movies': 50     # Niche but fun content
        }
        
        # Subreddits rich in specific genres
        self.category_rich_subreddits = {
            'Horror': ['horror', 'horrormovies'],
            'Comedy': ['netflix', 'hulu', 'television'],
            'Action': ['movies', 'DisneyPlus', 'PrimeVideo'],
            'Drama': ['television', 'netflix', 'HBOMax'],
            'Sci-Fi': ['movies', 'television'],
            'Fantasy': ['movies', 'television'],
            'Romance': ['netflix', 'hulu'],
            'Anime': ['anime', 'animesuggest'],
            'Documentary': ['documentaries'],
            'Bad Movies': ['movies']
        }
    
    def extract_balanced_posts(self, time_filter='week', base_limit=50):
        """
        Extract entertainment posts with balanced genre representation
        """
        print("=== ENTERTAINMENT BALANCED EXTRACTION SYSTEM ===")
        print("Phase 1: Initial media-filtered extraction...")
        
        # Phase 1: Extract and classify posts
        df_all = self.extractor.extract_comprehensive_data(time_filter, base_limit)
        print(f"Extracted {len(df_all)} total posts")
        
        # Filter for media content only
        df_media = self.classifier.classify_dataframe(df_all)
        filtered_out = len(df_all) - len(df_media)
        print(f"Media posts: {len(df_media)} (filtered out {filtered_out} non-media posts)")
        
        if len(df_media) == 0:
            print("No media posts found!")
            return pd.DataFrame()
        
        # Rank posts by popularity
        df_ranked = self.ranker.rank_posts(df_media)
        
        print(f"\nPhase 2: Analyzing genre distribution...")
        self._analyze_initial_distribution(df_ranked)
        
        # Phase 2: Apply balanced selection
        df_balanced = self._apply_balanced_selection(df_ranked)
        
        print(f"\n=== FINAL RESULTS ===")
        print(f"Selected {len(df_balanced)} posts for balanced entertainment dataset")
        self._analyze_final_distribution(df_balanced)
        
        return df_balanced
    
    def _analyze_initial_distribution(self, df):
        """Analyze initial genre distribution"""
        category_counts = df['category'].value_counts()
        print("Initial genre distribution:")
        for category, count in category_counts.items():
            threshold = self.category_thresholds.get(category, 100)
            above_threshold = len(df[(df['category'] == category) & (df['popularity_score'] >= threshold)])
            print(f"  {category}: {count} total ({above_threshold} above threshold {threshold})")
    
    def _apply_balanced_selection(self, df):
        """Apply balanced selection logic"""
        print(f"\nPhase 3: Applying balanced selection...")
        selected_posts = []
        
        # Track what we've selected per category
        category_selected = {cat: 0 for cat in self.category_minimums.keys()}
        
        # Sort by popularity score (descending)
        df_sorted = df.sort_values('popularity_score', ascending=False)
        
        # First pass: Select high-quality posts for each category
        for category in self.category_minimums.keys():
            minimum = self.category_minimums[category]
            threshold = self.category_thresholds[category]
            
            category_posts = df_sorted[
                (df_sorted['category'] == category) & 
                (df_sorted['popularity_score'] >= threshold)
            ].head(minimum)
            
            selected_posts.append(category_posts)
            category_selected[category] = len(category_posts)
            print(f"  {category}: Selected {len(category_posts)}/{minimum} posts (threshold: {threshold})")
        
        # Second pass: Fill gaps with lower-threshold posts
        print(f"\nPhase 4: Filling gaps with lower-threshold posts...")
        for category in self.category_minimums.keys():
            current_count = category_selected[category]
            minimum = self.category_minimums[category]
            
            if current_count < minimum:
                gap = minimum - current_count
                lower_threshold = max(20, self.category_thresholds[category] // 2)
                
                # Get additional posts from category-rich subreddits
                rich_subreddits = self.category_rich_subreddits.get(category, [])
                additional_posts = df_sorted[
                    (df_sorted['category'] == category) & 
                    (df_sorted['popularity_score'] >= lower_threshold) &
                    (df_sorted['subreddit'].isin(rich_subreddits)) &
                    (~df_sorted.index.isin([idx for posts in selected_posts for idx in posts.index]))
                ].head(gap)
                
                if len(additional_posts) > 0:
                    selected_posts.append(additional_posts)
                    category_selected[category] += len(additional_posts)
                    print(f"  {category}: Added {len(additional_posts)} posts from rich subreddits (threshold: {lower_threshold})")
        
        # Combine all selected posts
        if selected_posts:
            df_final = pd.concat(selected_posts, ignore_index=True)
            # Remove duplicates and sort by popularity
            df_final = df_final.drop_duplicates(subset=['post_id']).sort_values('popularity_score', ascending=False)
            return df_final
        else:
            return pd.DataFrame()
    
    def _analyze_final_distribution(self, df):
        """Analyze final distribution"""
        if len(df) == 0:
            print("No posts in final dataset")
            return
            
        category_counts = df['category'].value_counts()
        print("Final genre distribution:")
        total_posts = len(df)
        
        for category, minimum in self.category_minimums.items():
            count = category_counts.get(category, 0)
            percentage = (count / total_posts) * 100 if total_posts > 0 else 0
            status = "✓" if count >= minimum else "✗"
            print(f"  {status} {category}: {count}/{minimum} posts ({percentage:.1f}%)")
        
        print(f"\nPopularity score distribution:")
        print(f"  Mean: {df['popularity_score'].mean():.1f}")
        print(f"  Median: {df['popularity_score'].median():.1f}")
        print(f"  Min: {df['popularity_score'].min():.1f}")
        print(f"  Max: {df['popularity_score'].max():.1f}")

def test_entertainment_balanced_extractor():
    """Test the entertainment balanced extractor"""
    extractor = EntertainmentBalancedExtractor()
    
    print("Testing Entertainment Balanced Extractor...")
    df_balanced = extractor.extract_balanced_posts(time_filter='week', base_limit=30)
    
    if len(df_balanced) > 0:
        print(f"\nSample posts from balanced dataset:")
        for i, (_, row) in enumerate(df_balanced.head(5).iterrows()):
            print(f"{i+1}. [{row['category']}] {row['title'][:60]}... (Score: {row['popularity_score']:.1f})")
    
    return df_balanced

if __name__ == "__main__":
    test_entertainment_balanced_extractor()