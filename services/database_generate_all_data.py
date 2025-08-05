#!/usr/bin/env python3
"""
Database-First Generate All Data Script
Complete data generation using pure Supabase operations - no CSV dependencies
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

def main():
    """Generate all data using database-first approach"""
    start_time = datetime.now()
    
    print("🎯 DATABASE-FIRST REDDIT DATA GENERATION")
    print("=" * 60)
    print("🚀 Pure Supabase operations - no CSV files")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    success_count = 0
    total_steps = 5
    
    # Step 1: Run Database Unified Pipeline (Weekly)
    print(f"\n📋 Step 1/5: Weekly Database Extraction")
    if run_command("python services/database_unified_pipeline.py week", "Weekly Database Extraction"):
        success_count += 1
    else:
        print("⚠️  Weekly extraction failed - continuing anyway...")
    
    # Brief pause between extractions
    print("\n⏳ Waiting 60 seconds before daily extraction...")
    time.sleep(60)
    
    # Step 2: Run Database Unified Pipeline (Daily)
    print(f"\n📋 Step 2/5: Daily Database Extraction")
    if run_command("python services/database_unified_pipeline.py day", "Daily Database Extraction"):
        success_count += 1
    else:
        print("⚠️  Daily extraction failed - continuing anyway...")
    
    # Step 3: Generate Pure Database Dashboard
    print(f"\n📋 Step 3/5: Pure Database Dashboard Generation")
    if run_command("python utils/original_style_database_dashboard.py", "Pure Database Dashboard Generation"):
        success_count += 1
    
    # Step 4: Start Comment API Service (background)
    print("\n📋 Step 4/5: Starting Comment API Service")
    print("=" * 60)
    try:
        comment_process = subprocess.Popen(
            [sys.executable, "utils/live_comment_fetcher.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2)  # Give it time to start
        if comment_process.poll() is None:  # Still running
            print("✅ Comment API Service started on http://localhost:5001")
            success_count += 1
        else:
            print("❌ Comment API Service failed to start")
    except Exception as e:
        print(f"❌ Error starting Comment API: {e}")
    
    # Step 5: Start AI Summarizer Service (background)
    print("\n📋 Step 5/5: Starting AI Summarizer Service")
    print("=" * 60)
    
    # Check if GROQ_API_KEY is set
    if not os.getenv('GROQ_API_KEY'):
        print("❌ GROQ_API_KEY environment variable not set")
        print("   Please set it before running: export GROQ_API_KEY='your-key-here'")
        print("   AI Summarizer will not work without this key")
    else:
        try:
            ai_process = subprocess.Popen(
                [sys.executable, "services/ai_summarizer.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(3)  # Give it more time to start
            if ai_process.poll() is None:  # Still running
                print("✅ AI Summarizer Service started on http://localhost:5002")
                success_count += 1
            else:
                # Get error output
                stdout, stderr = ai_process.communicate()
                print("❌ AI Summarizer Service failed to start")
                if stderr:
                    print(f"   Error: {stderr.decode()}")
        except Exception as e:
            print(f"❌ Error starting AI Summarizer: {e}")
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 60)
    print("📊 DATABASE GENERATION COMPLETE")
    print("=" * 60)
    print(f"✅ Completed: {success_count}/{total_steps} steps")
    print(f"⏱️  Total time: {duration}")
    print(f"🌐 Dashboard ready: assets/reddit_dashboard.html")
    print(f"💾 Data source: Supabase database (no CSV files)")
    
    if success_count == total_steps:
        print("\n🎉 All data generated successfully!")
        print("📂 Database Operations Completed:")
        print("   • Finance posts extracted and stored in Supabase")
        print("   • Entertainment posts extracted and stored in Supabase")
        print("   • Travel posts extracted and stored in Supabase") 
        print("   • Pure database dashboard generated")
        print("\n✨ Services running:")
        print("   • Comment API: http://localhost:5001")
        print("   • AI Summarizer: http://localhost:5002")
        print("\n🚀 Next-Generation Features:")
        print("   • Zero CSV dependencies - pure database operations")
        print("   • Real-time data consistency across all components")
        print("   • Scalable architecture ready for production")
        print("\n🌐 Dashboard ready: assets/reddit_dashboard.html")
    else:
        print(f"\n⚠️  {total_steps - success_count} step(s) failed - check output above")
        
    return success_count == total_steps

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)