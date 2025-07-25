#!/usr/bin/env python3

import pandas as pd
from entertainment_extractor import EntertainmentRedditExtractor
from entertainment_classifier_v2 import EntertainmentClassifierV2
from popularity_ranker_v2 import PopularityRankerV2

def test_full_weekly_dataset():
    """Test the new discussion-type classifier on full weekly dataset"""
    
    print("=== TESTING DISCUSSION-TYPE CLASSIFIER ON FULL WEEKLY DATASET ===")
    print("Extracting posts from all entertainment subreddits...")
    
    # Extract posts from all subreddits
    extractor = EntertainmentRedditExtractor()
    df_raw = extractor.extract_comprehensive_data(time_filter='week', limit_per_subreddit=100)
    
    print(f"Extracted {len(df_raw)} total posts")
    
    if len(df_raw) == 0:
        print("No posts extracted!")
        return
    
    # Apply popularity filtering first (score > 5 to get meaningful posts)
    ranker = PopularityRankerV2()  
    df_ranked = ranker.calculate_popularity_score(df_raw)
    df_filtered = df_ranked[df_ranked['popularity_score'] >= 20]  # Lower score limit
    
    print(f"Posts passing popularity filter (score >= 20): {len(df_filtered)}")
    
    # Classify with new discussion-type system
    classifier = EntertainmentClassifierV2()
    df_classified = classifier.classify_dataframe(df_filtered)
    
    print(f"\n=== CLASSIFICATION RESULTS ===")
    classifier.analyze_classification(df_classified, df_filtered)
    
    # Show sample posts from each category
    print(f"\n=== SAMPLE POSTS BY DISCUSSION TYPE ===")
    category_counts = df_classified['category'].value_counts()
    
    for category in category_counts.index:
        print(f"\n{category} ({category_counts[category]} posts):")
        samples = df_classified[df_classified['category'] == category].nlargest(3, 'popularity_score')
        for i, (_, row) in enumerate(samples.iterrows(), 1):
            print(f"  {i}. [{row['subreddit']}] {row['title'][:70]}{'...' if len(row['title']) > 70 else ''}")
            print(f"     Score: {row['popularity_score']:.1f}, Confidence: {row['classification_confidence']}")
    
    return df_classified

if __name__ == "__main__":
    test_full_weekly_dataset()