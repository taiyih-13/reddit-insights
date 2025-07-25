import schedule
import time
from update_pipeline import RealTimeUpdater

def run_scheduled_updates():
    """Run scheduled updates using the schedule library"""
    
    updater = RealTimeUpdater()
    
    # Schedule automated extraction system
    schedule.every(6).hours.do(lambda: updater.automated_extraction_system())
    
    # Daily cleanup at 3 AM
    schedule.every().day.at("03:00").do(lambda: updater.full_refresh())
    
    print("📅 REDDIT PIPELINE SCHEDULER STARTED")
    print("🔄 Update Schedule:")
    print("   • Automated extraction system: Every 6 hours")
    print("   • Daily cleanup: 3:00 AM")
    print("⏹️  Press Ctrl+C to stop\n")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print(f"\n🛑 Scheduler stopped by user")

if __name__ == "__main__":
    run_scheduled_updates()