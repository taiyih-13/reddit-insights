#!/usr/bin/env python3

import pandas as pd
from entertainment_balanced_extractor_v2 import EntertainmentBalancedExtractorV2

def generate_entertainment_csvs():
    """Generate entertainment CSV files for dashboard integration"""
    
    print("=== GENERATING ENTERTAINMENT DATA FOR DASHBOARD ===")
    
    extractor = EntertainmentBalancedExtractorV2()
    
    # Generate weekly data
    print("Generating weekly entertainment data...")
    df_weekly = extractor.extract_balanced_posts(time_filter='week', base_limit=100)
    
    if len(df_weekly) > 0:
        df_weekly.to_csv('week_entertainment_posts.csv', index=False)
        print(f"✅ Weekly data saved: {len(df_weekly)} posts in week_entertainment_posts.csv")
    else:
        print("❌ No weekly entertainment data generated")
    
    # Generate daily data  
    print("\nGenerating daily entertainment data...")
    df_daily = extractor.extract_balanced_posts(time_filter='day', base_limit=100)
    
    if len(df_daily) > 0:
        df_daily.to_csv('day_entertainment_posts.csv', index=False)
        print(f"✅ Daily data saved: {len(df_daily)} posts in day_entertainment_posts.csv")
    else:
        print("❌ No daily entertainment data generated")
    
    print("\n=== ENTERTAINMENT DATA GENERATION COMPLETE ===")
    return df_weekly, df_daily

if __name__ == "__main__":
    generate_entertainment_csvs()