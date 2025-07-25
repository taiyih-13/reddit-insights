#!/usr/bin/env python3

import pandas as pd
from entertainment_extractor import EntertainmentRedditExtractor
from entertainment_classifier_v2 import EntertainmentClassifierV2
from popularity_ranker_v2 import PopularityRankerV2

def test_balanced_subreddits():
    """Test the entertainment system with added subreddits for better balance"""
    
    print("=== TESTING BALANCED SUBREDDIT SYSTEM ===")
    print("Now includes 22 subreddits total:")
    
    extractor = EntertainmentRedditExtractor()
    print(f"Configured subreddits:")
    for i, subreddit in enumerate(extractor.entertainment_subreddits, 1):
        print(f"  {i:2d}. r/{subreddit}")
    
    print(f"\nExtracting posts from all {len(extractor.entertainment_subreddits)} subreddits...")
    
    # Extract smaller sample first to test
    df_raw = extractor.extract_comprehensive_data(time_filter='week', limit_per_subreddit=25)
    
    print(f"Extracted {len(df_raw)} total posts")
    
    if len(df_raw) == 0:
        print("No posts extracted!")
        return
    
    # Apply popularity filtering
    ranker = PopularityRankerV2()  
    df_ranked = ranker.calculate_popularity_score(df_raw)
    df_filtered = df_ranked[df_ranked['popularity_score'] >= 15]  # Even lower threshold
    
    print(f"Posts passing popularity filter (score >= 15): {len(df_filtered)}")
    
    # Classify with new discussion-type system
    classifier = EntertainmentClassifierV2()
    df_classified = classifier.classify_dataframe(df_filtered)
    
    print(f"\n=== CLASSIFICATION RESULTS WITH NEW SUBREDDITS ===")
    classifier.analyze_classification(df_classified, df_filtered)
    
    # Analyze by subreddit
    print(f"\n=== POSTS BY SUBREDDIT ===")
    subreddit_counts = df_classified['subreddit'].value_counts()
    for subreddit, count in subreddit_counts.items():
        percentage = (count / len(df_classified)) * 100 if len(df_classified) > 0 else 0
        print(f"  r/{subreddit}: {count} posts ({percentage:.1f}%)")
    
    # Show samples from new subreddits
    new_subreddits = ['tipofmytongue', 'ifyoulikeblank', 'criterion', 'truefilm', 'flicks']
    print(f"\n=== SAMPLES FROM NEW SUBREDDITS ===")
    for subreddit in new_subreddits:
        posts = df_classified[df_classified['subreddit'] == subreddit]
        if len(posts) > 0:
            print(f"\nr/{subreddit} ({len(posts)} posts):")
            for i, (_, row) in enumerate(posts.head(3).iterrows(), 1):
                print(f"  {i}. [{row['category']}] {row['title'][:70]}{'...' if len(row['title']) > 70 else ''}")
                print(f"     Score: {row['popularity_score']:.1f}, Confidence: {row['classification_confidence']}")
        else:
            print(f"\nr/{subreddit}: No posts found")
    
    return df_classified

if __name__ == "__main__":
    test_balanced_subreddits()