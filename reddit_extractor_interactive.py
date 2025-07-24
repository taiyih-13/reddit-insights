#!/usr/bin/env python3
"""
Interactive Reddit Finance Extractor
Allows users to choose between daily and weekly extraction
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from balanced_extractor import BalancedExtractor
from dashboard_generator_clean import CleanRedditDashboard

def display_menu():
    """Display the main menu options"""
    print("\n" + "="*60)
    print("🚀 REDDIT FINANCE INTELLIGENCE EXTRACTOR")
    print("="*60)
    print("Choose your extraction time frame:")
    print()
    print("1. 📅 Daily Posts    - Last 24 hours (~170 posts)")
    print("2. 📊 Weekly Posts   - Last 7 days (~430 posts)")
    print("3. ❌ Exit")
    print()

def get_user_choice():
    """Get and validate user choice"""
    while True:
        try:
            choice = input("Enter your choice (1-3): ").strip()
            if choice in ['1', '2', '3']:
                return choice
            else:
                print("❌ Invalid choice. Please enter 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            sys.exit(0)

def show_extraction_summary(time_filter):
    """Show what the extraction will include"""
    if time_filter == 'day':
        print("\n📅 DAILY EXTRACTION SUMMARY:")
        print("• Time Frame: Last 24 hours")
        print("• Expected Posts: ~170 posts")
        print("• Quality: High (300+ popularity score)")
        print("• Content: Most recent trending discussions")
        print("• Processing Time: ~30 seconds")
    else:
        print("\n📊 WEEKLY EXTRACTION SUMMARY:")
        print("• Time Frame: Last 7 days") 
        print("• Expected Posts: ~430 posts")
        print("• Quality: High (300+ popularity score)")
        print("• Content: Best content from entire week")
        print("• Processing Time: ~1-2 minutes")

def confirm_extraction():
    """Ask user to confirm extraction"""
    while True:
        confirm = input("\n🚀 Proceed with extraction? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            return True
        elif confirm in ['n', 'no']:
            return False
        else:
            print("❌ Please enter 'y' or 'n'")

def run_extraction(time_filter):
    """Run the extraction with chosen time filter"""
    print(f"\n🔄 Starting {time_filter} extraction...")
    print("This may take a moment - extracting from 17 finance subreddits...")
    
    try:
        # Initialize extractor
        extractor = BalancedExtractor()
        
        # Run extraction
        balanced_df = extractor.extract_balanced_posts(time_filter=time_filter, base_limit=100)
        
        if len(balanced_df) > 0:
            # Save data
            filename = f'{time_filter}_reddit_posts.csv'
            balanced_df.to_csv(filename, index=False)
            
            # Generate dashboard
            dashboard_filename = f'reddit_dashboard_{time_filter}.html'
            dashboard = CleanRedditDashboard(filename)
            dashboard.generate_dashboard(dashboard_filename)
            
            # Show success summary
            print(f"\n🎉 EXTRACTION COMPLETE!")
            print(f"📊 Extracted {len(balanced_df)} high-quality posts")
            print(f"💾 Data saved to: {filename}")
            print(f"🌐 Dashboard created: {dashboard_filename}")
            
            # Show category breakdown
            print(f"\n📈 Category Breakdown:")
            category_counts = balanced_df['category'].value_counts()
            for category, count in category_counts.items():
                percentage = (count / len(balanced_df)) * 100
                print(f"  • {category}: {count} posts ({percentage:.1f}%)")
            
            # Show top post
            top_post = balanced_df.iloc[0]
            print(f"\n🏆 Top Post:")
            print(f"  '{top_post['title'][:60]}...'")
            print(f"  r/{top_post['subreddit']} | {top_post['score']} ↑ | {top_post['num_comments']} 💬")
            print(f"  Popularity Score: {top_post['popularity_score']:.0f}")
            
            return True
            
        else:
            print("❌ No posts extracted. Please check your Reddit API credentials.")
            return False
            
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return False

def main():
    """Main interactive loop"""
    print("Welcome to the Reddit Finance Intelligence Extractor!")
    
    while True:
        display_menu()
        choice = get_user_choice()
        
        if choice == '3':
            print("\n👋 Thank you for using Reddit Finance Extractor!")
            break
        
        # Map choice to time filter
        time_filter = 'day' if choice == '1' else 'week'
        
        # Show summary and confirm
        show_extraction_summary(time_filter)
        
        if confirm_extraction():
            success = run_extraction(time_filter)
            
            if success:
                # Ask if user wants to extract another timeframe
                while True:
                    another = input("\n🔄 Extract another timeframe? (y/n): ").strip().lower()
                    if another in ['y', 'yes']:
                        break
                    elif another in ['n', 'no']:
                        print("\n👋 Thank you for using Reddit Finance Extractor!")
                        return
                    else:
                        print("❌ Please enter 'y' or 'n'")
            else:
                # Ask if user wants to try again
                while True:
                    retry = input("\n🔄 Try again? (y/n): ").strip().lower()
                    if retry in ['y', 'yes']:
                        break
                    elif retry in ['n', 'no']:
                        print("\n👋 Thank you for using Reddit Finance Extractor!")
                        return
                    else:
                        print("❌ Please enter 'y' or 'n'")
        else:
            print("❌ Extraction cancelled.")

if __name__ == "__main__":
    main()