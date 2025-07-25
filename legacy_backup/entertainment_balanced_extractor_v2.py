import pandas as pd
import time
from entertainment_extractor import EntertainmentRedditExtractor
from popularity_ranker_v2 import PopularityRankerV2
from entertainment_classifier_v2 import EntertainmentClassifierV2

class EntertainmentBalancedExtractorV2:
    def __init__(self):
        self.extractor = EntertainmentRedditExtractor()
        self.ranker = PopularityRankerV2()
        self.classifier = EntertainmentClassifierV2()
        
        # Target minimums for each discussion type
        self.category_minimums = {
            'Recommendation Requests': 25,      # Most common type, allow more
            'Reviews & Discussions': 20,       # Common discussion posts
            'News & Announcements': 15,        # Movie/TV news posts
            'Lists & Rankings': 10,            # Curated lists and rankings
            'Identification & Help': 8,        # "What movie is this?" posts
        }
        
        # Discussion-type specific popularity thresholds
        # Entertainment posts generally have lower engagement than finance
        self.category_thresholds = {
            'Recommendation Requests': 50,      # Many low-engagement requests
            'Reviews & Discussions': 80,        # Discussion posts get more engagement
            'News & Announcements': 100,        # News tends to get high engagement
            'Lists & Rankings': 70,             # Lists get decent engagement
            'Identification & Help': 30,        # Help posts often low engagement
        }
        
        # Subreddits rich in specific discussion types
        self.category_rich_subreddits = {
            'Recommendation Requests': [
                'animesuggest', 'MovieSuggestions', 'televisionsuggestions', 
                'NetflixBestOf', 'ifyoulikeblank'
            ],
            'Reviews & Discussions': [
                'netflix', 'hulu', 'DisneyPlus', 'HBOMax', 'letterboxd', 'flicks'
            ],
            'News & Announcements': [
                'movies', 'television', 'anime'
            ],
            'Lists & Rankings': [
                'criterion', 'truefilm', 'letterboxd'
            ],
            'Identification & Help': [
                'tipofmytongue', 'ifyoulikeblank'
            ]
        }
    
    def extract_balanced_posts(self, time_filter='week', base_limit=100):
        """
        Extract entertainment posts with balanced discussion-type representation
        """
        print("=== ENTERTAINMENT DISCUSSION-TYPE BALANCED EXTRACTION SYSTEM ===")
        print("Phase 1: Initial media-filtered extraction...")
        
        # Phase 1: Extract and classify posts
        df_all = self.extractor.extract_comprehensive_data(time_filter, base_limit)
        print(f"Extracted {len(df_all)} total posts from {len(self.extractor.entertainment_subreddits)} subreddits")
        
        # Apply popularity ranking
        df_ranked = self.ranker.calculate_popularity_score(df_all)
        
        # Apply global popularity filter (matching finance workflow)
        print(f"Applied popularity threshold: 100")
        df_filtered = self.ranker.apply_filters(df_ranked, min_popularity=100)
        score_filtered_out = len(df_ranked) - len(df_filtered)
        print(f"Filtered out {score_filtered_out} low-engagement posts")
        print(f"Remaining: {len(df_filtered)} high-signal posts")
        
        # Filter for media content only using new classifier
        df_media = self.classifier.classify_dataframe(df_filtered)
        media_filtered_out = len(df_filtered) - len(df_media)
        print(f"Media posts: {len(df_media)} (filtered out {media_filtered_out} non-media posts - {media_filtered_out/len(df_filtered)*100:.1f}%)")
        
        if len(df_media) == 0:
            print("No media posts found!")
            return pd.DataFrame()
        
        print(f"\\nPhase 2: Analyzing discussion-type distribution...")
        self._analyze_initial_distribution(df_media)
        
        # Phase 2: Apply balanced selection
        df_balanced = self._apply_balanced_selection(df_media)
        
        print(f"\\n=== FINAL RESULTS ==")
        print(f"Selected {len(df_balanced)} posts for balanced entertainment dataset")
        self._analyze_final_distribution(df_balanced)
        
        return df_balanced
    
    def _analyze_initial_distribution(self, df):
        """Analyze initial discussion-type distribution"""
        category_counts = df['category'].value_counts()
        print("Initial discussion-type distribution:")
        for category, count in category_counts.items():
            threshold = self.category_thresholds.get(category, 50)
            above_threshold = len(df[(df['category'] == category) & (df['popularity_score'] >= threshold)])
            print(f"  {category}: {count} total ({above_threshold} above threshold {threshold})")
    
    def _apply_balanced_selection(self, df):
        """Apply balanced selection logic for discussion types"""
        print(f"\\nPhase 3: Applying balanced selection...")
        selected_posts = []
        
        # Track what we've selected per category
        category_selected = {cat: 0 for cat in self.category_minimums.keys()}
        
        # Sort by popularity score (descending)
        df_sorted = df.sort_values('popularity_score', ascending=False)
        
        # First pass: Select ALL high-quality posts for each category (no limit)
        for category in self.category_minimums.keys():
            minimum = self.category_minimums[category]
            threshold = self.category_thresholds[category]
            
            category_posts = df_sorted[
                (df_sorted['category'] == category) & 
                (df_sorted['popularity_score'] >= threshold)
            ]  # Removed .head(minimum) to get ALL posts above threshold
            
            selected_posts.append(category_posts)
            category_selected[category] = len(category_posts)
            print(f"  {category}: Selected {len(category_posts)} posts (minimum: {minimum}, threshold: {threshold})")
        
        # Second pass: Fill gaps with lower-threshold posts
        print(f"\\nPhase 4: Filling gaps with lower-threshold posts...")
        for category in self.category_minimums.keys():
            current_count = category_selected[category]
            minimum = self.category_minimums[category]
            
            if current_count < minimum:
                gap = minimum - current_count
                lower_threshold = max(15, self.category_thresholds[category] // 2)
                
                # Get additional posts from category-rich subreddits
                rich_subreddits = self.category_rich_subreddits.get(category, [])
                
                # Get indices of already selected posts
                selected_indices = set()
                for posts in selected_posts:
                    selected_indices.update(posts.index)
                
                additional_posts = df_sorted[
                    (df_sorted['category'] == category) & 
                    (df_sorted['popularity_score'] >= lower_threshold) &
                    (df_sorted['subreddit'].isin(rich_subreddits)) &
                    (~df_sorted.index.isin(selected_indices))
                ].head(gap)
                
                if len(additional_posts) > 0:
                    selected_posts.append(additional_posts)
                    category_selected[category] += len(additional_posts)
                    print(f"  {category}: Added {len(additional_posts)} posts from rich subreddits (threshold: {lower_threshold})")
                
                # If still short, get any posts from that category
                if category_selected[category] < minimum:
                    still_needed = minimum - category_selected[category]
                    
                    # Update selected indices
                    selected_indices = set()
                    for posts in selected_posts:
                        selected_indices.update(posts.index)
                    
                    fallback_posts = df_sorted[
                        (df_sorted['category'] == category) & 
                        (df_sorted['popularity_score'] >= 10) &  # Very low threshold
                        (~df_sorted.index.isin(selected_indices))
                    ].head(still_needed)
                    
                    if len(fallback_posts) > 0:
                        selected_posts.append(fallback_posts)
                        category_selected[category] += len(fallback_posts)
                        print(f"  {category}: Added {len(fallback_posts)} fallback posts (threshold: 10)")
        
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
        print("Final discussion-type distribution:")
        total_posts = len(df)
        
        for category, minimum in self.category_minimums.items():
            count = category_counts.get(category, 0)
            percentage = (count / total_posts) * 100 if total_posts > 0 else 0
            status = "✓" if count >= minimum else "✗"
            print(f"  {status} {category}: {count}/{minimum} posts ({percentage:.1f}%)")
        
        print(f"\\nPopularity score distribution:")
        print(f"  Mean: {df['popularity_score'].mean():.1f}")
        print(f"  Median: {df['popularity_score'].median():.1f}")
        print(f"  Min: {df['popularity_score'].min():.1f}")
        print(f"  Max: {df['popularity_score'].max():.1f}")
        
        print(f"\\nConfidence distribution:")
        confidence_counts = df['classification_confidence'].value_counts()
        for confidence, count in confidence_counts.items():
            percentage = (count / total_posts) * 100
            print(f"  {confidence}: {count} posts ({percentage:.1f}%)")

def test_entertainment_balanced_extractor_v2():
    """Test the new discussion-type balanced extractor"""
    extractor = EntertainmentBalancedExtractorV2()
    
    print("Testing Entertainment Discussion-Type Balanced Extractor V2...")
    df_balanced = extractor.extract_balanced_posts(time_filter='week', base_limit=50)
    
    if len(df_balanced) > 0:
        print(f"\\nSample posts from balanced dataset:")
        for category in extractor.category_minimums.keys():
            category_posts = df_balanced[df_balanced['category'] == category]
            if len(category_posts) > 0:
                print(f"\\n{category}:")
                for i, (_, row) in enumerate(category_posts.head(2).iterrows(), 1):
                    print(f"  {i}. [{row['subreddit']}] {row['title'][:60]}... (Score: {row['popularity_score']:.1f})")
    
    return df_balanced

if __name__ == "__main__":
    test_entertainment_balanced_extractor_v2()