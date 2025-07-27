#!/usr/bin/env python3
"""
Complete Data Generation Script
Generates both daily and weekly data for all categories: finance, entertainment, and travel
"""

import subprocess
import sys
import time
import pandas as pd
import os
from datetime import datetime, timedelta

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"\n🚀 {description}")
    print("=" * 60)
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=False)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with error code: {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during {description}: {e}")
        return False

def extract_daily_from_weekly():
    """Extract daily data by filtering weekly data for last 24 hours"""
    print(f"\n🔄 Extracting Daily Data from Weekly")
    print("=" * 60)
    
    try:
        # Calculate 24-hour cutoff
        cutoff_time = datetime.now() - timedelta(days=1)
        print(f"Filtering posts from: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} onwards")
        
        # Define all categories
        categories = ['finance', 'entertainment', 'travel_tips', 'nature_adventure', 'regional_travel']
        
        for category in categories:
            weekly_path = f'assets/week_{category}_posts.csv'
            daily_path = f'assets/day_{category}_posts.csv'
            
            if os.path.exists(weekly_path):
                df_weekly = pd.read_csv(weekly_path)
                df_weekly['created_utc'] = pd.to_datetime(df_weekly['created_utc'])
                
                # Filter for last 24 hours
                df_daily = df_weekly[df_weekly['created_utc'] >= cutoff_time]
                
                # Save daily data
                df_daily.to_csv(daily_path, index=False)
                print(f"✅ {category.replace('_', ' ').title()}: Extracted {len(df_daily)} daily posts from {len(df_weekly)} weekly posts")
            else:
                print(f"⚠️  {category.replace('_', ' ').title()} weekly file not found: {weekly_path}")
        
        print(f"✅ Daily data extraction completed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Error during daily extraction: {e}")
        return False

def main():
    """Generate all data and dashboard"""
    start_time = datetime.now()
    
    print("🎯 COMPLETE REDDIT DATA GENERATION")
    print("=" * 60)
    print("Generating weekly data and extracting daily data from it")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    total_steps = 3
    
    # Step 1: Generate Weekly Data
    if run_command("python services/unified_update_pipeline.py --weekly", "Weekly Data Generation"):
        success_count += 1
    else:
        print("⚠️  Weekly data generation failed - continuing anyway...")
    
    # Step 2: Extract Daily Data from Weekly (no API calls needed)
    if extract_daily_from_weekly():
        success_count += 1
    else:
        print("⚠️  Daily data extraction failed - continuing with dashboard...")
    
    # Step 3: Generate Dashboard
    if run_command("python utils/dashboard_generator.py", "Dashboard Generation"):
        success_count += 1
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("📊 GENERATION COMPLETE")
    print("=" * 60)
    print(f"✅ Completed: {success_count}/{total_steps} steps")
    print(f"⏱️  Total time: {duration}")
    print(f"🌐 Dashboard ready: assets/reddit_dashboard.html")
    
    if success_count == total_steps:
        print("\n🎉 All data generated successfully!")
        print("📂 Files created:")
        print("   • assets/week_finance_posts.csv (weekly finance)")
        print("   • assets/day_finance_posts.csv (daily finance - filtered from weekly)")  
        print("   • assets/week_entertainment_posts.csv (weekly entertainment)")
        print("   • assets/day_entertainment_posts.csv (daily entertainment - filtered from weekly)")
        print("   • assets/week_travel_tips_posts.csv (weekly travel tips)")
        print("   • assets/day_travel_tips_posts.csv (daily travel tips - filtered from weekly)")
        print("   • assets/week_nature_adventure_posts.csv (weekly nature & adventure)")
        print("   • assets/day_nature_adventure_posts.csv (daily nature & adventure - filtered from weekly)")
        print("   • assets/week_regional_travel_posts.csv (weekly regional travel)")
        print("   • assets/day_regional_travel_posts.csv (daily regional travel - filtered from weekly)")
        print("   • assets/reddit_dashboard.html (unified dashboard)")
        print("\n✨ Optimization: Daily data extracted from weekly data (no duplicate API calls)")
        print("\n🤖 To start AI summarization:")
        print("   python services/ai_summarizer.py")
    else:
        print(f"\n⚠️  {total_steps - success_count} step(s) failed - check output above")
        
    return success_count == total_steps

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)