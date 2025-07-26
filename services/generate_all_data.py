#!/usr/bin/env python3
"""
Complete Data Generation Script
Generates both daily and weekly data for finance and entertainment categories
"""

import subprocess
import sys
import time
from datetime import datetime

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

def main():
    """Generate all data and dashboard"""
    start_time = datetime.now()
    
    print("🎯 COMPLETE REDDIT DATA GENERATION")
    print("=" * 60)
    print("Generating both daily and weekly data for finance and entertainment")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    total_steps = 3
    
    # Step 1: Generate Weekly Data
    if run_command("python services/unified_update_pipeline.py --weekly", "Weekly Data Generation"):
        success_count += 1
    else:
        print("⚠️  Weekly data generation failed - continuing with daily...")
    
    # Brief pause between extractions to avoid rate limiting
    print("\n⏱️  Pausing 5 seconds to avoid rate limiting...")
    time.sleep(5)
    
    # Step 2: Generate Daily Data  
    if run_command("python services/unified_update_pipeline.py --daily", "Daily Data Generation"):
        success_count += 1
    else:
        print("⚠️  Daily data generation failed - continuing with dashboard...")
    
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
        print("   • assets/week_reddit_posts.csv (weekly finance)")
        print("   • assets/day_reddit_posts.csv (daily finance)")  
        print("   • assets/week_entertainment_posts.csv (weekly entertainment)")
        print("   • assets/day_entertainment_posts.csv (daily entertainment)")
        print("   • assets/reddit_dashboard.html (unified dashboard)")
        print("\n🤖 To start AI summarization:")
        print("   python services/ai_summarizer.py")
    else:
        print(f"\n⚠️  {total_steps - success_count} step(s) failed - check output above")
        
    return success_count == total_steps

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)