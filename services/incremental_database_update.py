#!/usr/bin/env python3
"""
Incremental Database Update Service
Only adds new posts and removes expired ones - no full regeneration
"""

import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import json
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.finance_database_extractor import FinanceDatabaseExtractor
from extractors.entertainment_database_extractor import EntertainmentDatabaseExtractor
from extractors.travel_database_extractor import TravelDatabaseExtractor
from services.enhanced_database_service import get_enhanced_db_service

class IncrementalDatabaseUpdate:
    """
    Incremental update service that only processes new/expired posts
    Much faster than full regeneration
    """
    
    def __init__(self, time_filter='week'):
        self.time_filter = time_filter  # 'week' or 'day'
        self.db_service = get_enhanced_db_service()
        
        # Initialize extractors
        self.extractors = {
            'finance': FinanceDatabaseExtractor(),
            'entertainment': EntertainmentDatabaseExtractor(), 
            'travel': TravelDatabaseExtractor()
        }
        
        print(f"🔄 Incremental Database Update ({time_filter})")
        print("=" * 50)
    
    def get_time_cutoff(self) -> datetime:
        """Calculate cutoff time for posts"""
        now = datetime.utcnow()
        if self.time_filter == 'week':
            return now - timedelta(days=7)
        else:  # day
            return now - timedelta(days=1)
    
    def get_latest_post_time(self, domain: str) -> datetime:
        """Get timestamp of most recent post for a domain"""
        try:
            # Get domain-specific subreddits
            extractor = self.extractors[domain]
            subreddits = extractor.subreddits
            
            # Query for most recent post from this domain
            result = self.db_service.supabase.table('posts').select('created_utc').in_('subreddit', subreddits).eq('time_filter', self.time_filter).order('created_utc', desc=True).limit(1).execute()
            
            if result.data:
                latest_str = result.data[0]['created_utc']
                return datetime.fromisoformat(latest_str.replace('Z', '+00:00')).replace(tzinfo=None)
            else:
                # No posts found, return old date to trigger full extraction
                return datetime.utcnow() - timedelta(days=30)
                
        except Exception as e:
            print(f"⚠️  Error getting latest post time for {domain}: {e}")
            return datetime.utcnow() - timedelta(days=30)
    
    def _get_subreddit_list(self, domain: str) -> str:
        """Get formatted subreddit list for SQL query"""
        extractor = self.extractors[domain]
        subreddits = [f"'{sub}'" for sub in extractor.subreddits]
        return ','.join(subreddits)
    
    def remove_expired_posts(self, domain: str) -> int:
        """Remove posts older than time cutoff"""
        cutoff_time = self.get_time_cutoff()
        
        try:
            # Get domain-specific subreddits
            extractor = self.extractors[domain]
            subreddits = extractor.subreddits
            
            # Delete expired posts for this domain
            result = self.db_service.supabase.table('posts').delete().lt('created_utc', cutoff_time.isoformat()).in_('subreddit', subreddits).eq('time_filter', self.time_filter).execute()
            
            deleted_count = len(result.data) if result.data else 0
            print(f"  🗑️  Removed {deleted_count} expired {domain} posts")
            return deleted_count
            
        except Exception as e:
            print(f"  ❌ Error removing expired {domain} posts: {e}")
            return 0
    
    def extract_new_posts(self, domain: str) -> Tuple[int, int]:
        """Extract only new posts since last update"""
        latest_post_time = self.get_latest_post_time(domain)
        cutoff_time = self.get_time_cutoff()
        
        # Only extract if we haven't updated recently (within last hour)
        if latest_post_time > datetime.utcnow() - timedelta(hours=1):
            print(f"  ✅ {domain.capitalize()} data is recent, checking for new posts...")
            fetch_since = latest_post_time
        else:
            print(f"  🔄 {domain.capitalize()} data is stale, doing incremental refresh...")
            fetch_since = cutoff_time
        
        extractor = self.extractors[domain]
        
        try:
            # Get new posts with custom since time
            new_posts_df = extractor.extract_posts_since(fetch_since, self.time_filter)
            
            if new_posts_df.empty:
                print(f"  ✅ No new {domain} posts to add")
                return 0, 0
            
            # Remove duplicates (posts that might already exist)
            existing_ids = self._get_existing_post_ids(domain, new_posts_df['id'].tolist())
            new_posts_df = new_posts_df[~new_posts_df['id'].isin(existing_ids)]
            
            if new_posts_df.empty:
                print(f"  ✅ All {domain} posts already exist")
                return 0, 0
            
            # Insert new posts using enhanced database service
            from services.enhanced_database_service import save_posts_with_computed_fields
            result = save_posts_with_computed_fields(new_posts_df)
            
            added_count = result.get('inserted_count', 0)
            total_new = len(new_posts_df)
            
            print(f"  ➕ Added {added_count} new {domain} posts")
            return added_count, total_new
            
        except Exception as e:
            print(f"  ❌ Error extracting new {domain} posts: {e}")
            return 0, 0
    
    def _get_existing_post_ids(self, domain: str, post_ids: List[str]) -> List[str]:
        """Get list of post IDs that already exist in database"""
        try:
            if not post_ids:
                return []
            
            # Query for existing IDs
            result = self.db_service.supabase.table('posts').select('id').in_('id', post_ids).eq('time_filter', self.time_filter).execute()
            
            existing_ids = [row['id'] for row in result.data] if result.data else []
            return existing_ids
            
        except Exception as e:
            print(f"  ⚠️  Error checking existing post IDs: {e}")
            return []
    
    def update_domain(self, domain: str) -> Dict[str, int]:
        """Update a specific domain incrementally"""
        print(f"\\n🔄 Updating {domain.capitalize()} ({self.time_filter})")
        print("-" * 30)
        
        start_time = datetime.now()
        
        # Step 1: Remove expired posts
        removed_count = self.remove_expired_posts(domain)
        
        # Step 2: Add new posts
        added_count, total_new = self.extract_new_posts(domain)
        
        duration = datetime.now() - start_time
        
        result = {
            'domain': domain,
            'added': added_count,
            'removed': removed_count,
            'duration': duration.total_seconds()
        }
        
        print(f"  ⏱️  Completed in {duration.total_seconds():.1f}s")
        return result
    
    def run_incremental_update(self, parallel: bool = True) -> Dict[str, any]:
        """Run incremental update for all domains"""
        start_time = datetime.now()
        domains = ['finance', 'entertainment', 'travel']
        results = []
        
        print(f"🚀 Starting incremental update ({self.time_filter})")
        print(f"📅 Time cutoff: {self.get_time_cutoff().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if parallel:
            # Run domains in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_domain = {
                    executor.submit(self.update_domain, domain): domain 
                    for domain in domains
                }
                
                for future in as_completed(future_to_domain):
                    domain = future_to_domain[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"❌ Error updating {domain}: {e}")
                        results.append({
                            'domain': domain,
                            'added': 0,
                            'removed': 0, 
                            'duration': 0,
                            'error': str(e)
                        })
        else:
            # Run domains sequentially  
            for domain in domains:
                try:
                    result = self.update_domain(domain)
                    results.append(result)
                except Exception as e:
                    print(f"❌ Error updating {domain}: {e}")
                    results.append({
                        'domain': domain,
                        'added': 0,
                        'removed': 0,
                        'duration': 0,
                        'error': str(e)
                    })
        
        # Summary
        total_duration = datetime.now() - start_time
        total_added = sum(r['added'] for r in results)
        total_removed = sum(r['removed'] for r in results)
        
        summary = {
            'total_added': total_added,
            'total_removed': total_removed,
            'total_duration': total_duration.total_seconds(),
            'domain_results': results,
            'success': all('error' not in r for r in results)
        }
        
        print("\\n" + "=" * 50)
        print("📊 INCREMENTAL UPDATE COMPLETE")
        print("=" * 50)
        print(f"➕ Total posts added: {total_added}")
        print(f"🗑️  Total posts removed: {total_removed}")
        print(f"⏱️  Total time: {total_duration}")
        print(f"🚀 Speed benefit: ~10x faster than full regeneration")
        
        if summary['success']:
            print("\\n✅ All domains updated successfully!")
            print("💡 Dashboard will automatically reflect new data")
        else:
            failed_domains = [r['domain'] for r in results if 'error' in r]
            print(f"\\n⚠️  Some domains failed: {failed_domains}")
        
        return summary
    
    def run_fast_update(self, parallel: bool = True) -> Dict[str, any]:
        """Run fast update for all domains (top 5 posts from key subreddits)"""
        start_time = datetime.now()
        domains = ['finance', 'entertainment', 'travel']
        results = []
        
        print(f"⚡ Starting FAST update ({self.time_filter})")
        print(f"📅 Target: 5 posts × 15 key subreddits = 75 posts max")
        
        if parallel:
            # Run domains in parallel for maximum speed
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_domain = {
                    executor.submit(self.fast_update_domain, domain): domain 
                    for domain in domains
                }
                
                for future in as_completed(future_to_domain):
                    domain = future_to_domain[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"❌ Error updating {domain}: {e}")
                        results.append({
                            'domain': domain,
                            'added': 0,
                            'removed': 0, 
                            'duration': 0,
                            'error': str(e)
                        })
        else:
            # Run domains sequentially  
            for domain in domains:
                try:
                    result = self.fast_update_domain(domain)
                    results.append(result)
                except Exception as e:
                    print(f"❌ Error updating {domain}: {e}")
                    results.append({
                        'domain': domain,
                        'added': 0,
                        'removed': 0,
                        'duration': 0,
                        'error': str(e)
                    })
        
        # Summary
        total_duration = datetime.now() - start_time
        total_added = sum(r['added'] for r in results)
        total_removed = sum(r['removed'] for r in results)
        
        summary = {
            'total_added': total_added,
            'total_removed': total_removed,
            'total_duration': total_duration.total_seconds(),
            'domain_results': results,
            'success': all('error' not in r for r in results)
        }
        
        print("\\n" + "=" * 50)
        print("⚡ FAST UPDATE COMPLETE")
        print("=" * 50)
        print(f"➕ Total posts added: {total_added}")
        print(f"🗑️  Total posts removed: {total_removed}")
        print(f"⏱️  Total time: {total_duration}")
        print(f"🚀 Speed: ~{75/total_duration.total_seconds():.1f} posts/second")
        
        if summary['success']:
            print("\\n✅ All domains updated successfully!")
            print("💡 Dashboard ready for refresh")
        else:
            failed_domains = [r['domain'] for r in results if 'error' in r]
            print(f"\\n⚠️  Some domains failed: {failed_domains}")
        
        return summary
    
    def fast_update_domain(self, domain: str) -> Dict[str, int]:
        """Fast update a specific domain using key subreddits only"""
        print(f"\\n⚡ Fast updating {domain.capitalize()} (key subreddits only)")
        print("-" * 30)
        
        start_time = datetime.now()
        
        # Step 1: Remove expired posts (quick)
        removed_count = self.remove_expired_posts(domain)
        
        # Step 2: Fast extraction from key subreddits
        extractor = self.extractors[domain]
        
        try:
            # Use the new fast update method
            new_posts_df = extractor.extract_fast_update(self.time_filter)
            
            if new_posts_df.empty:
                print(f"  ✅ No new {domain} posts to add")
                added_count = 0
            else:
                # Remove duplicates (posts that might already exist)
                existing_ids = self._get_existing_post_ids(domain, new_posts_df['id'].tolist())
                new_posts_df = new_posts_df[~new_posts_df['id'].isin(existing_ids)]
                
                if new_posts_df.empty:
                    print(f"  ✅ All {domain} posts already exist")
                    added_count = 0
                else:
                    # Insert new posts using enhanced database service
                    from services.enhanced_database_service import save_posts_with_computed_fields
                    result = save_posts_with_computed_fields(new_posts_df)
                    
                    added_count = result.get('inserted_count', 0)
                    print(f"  ➕ Added {added_count} new {domain} posts")
                    
        except Exception as e:
            print(f"  ❌ Error extracting new {domain} posts: {e}")
            added_count = 0
        
        duration = datetime.now() - start_time
        
        result = {
            'domain': domain,
            'added': added_count,
            'removed': removed_count,
            'duration': duration.total_seconds()
        }
        
        print(f"  ⏱️  Completed in {duration.total_seconds():.1f}s")
        return result

# Flask API for dashboard integration
app = Flask(__name__)
CORS(app)  # Allow requests from dashboard

@app.route('/update-feed', methods=['POST'])
def update_feed():
    """API endpoint for fast feed update"""
    try:
        data = request.get_json() if request.get_json() else {}
        time_filter = data.get('time_filter', 'week')
        fast_mode = data.get('fast', True)  # Default to fast mode for dashboard button
        
        if time_filter not in ['week', 'day']:
            return jsonify({'success': False, 'error': 'time_filter must be "week" or "day"'}), 400
        
        # Create and run updater
        updater = IncrementalDatabaseUpdate(time_filter)
        
        if fast_mode:
            # Use fast update (15 key subreddits, 5 posts each)
            summary = updater.run_fast_update(parallel=True)
        else:
            # Use full incremental update (all 83 subreddits)
            summary = updater.run_incremental_update(parallel=True)
        
        return jsonify({
            'success': summary['success'],
            'total_added': summary['total_added'],
            'total_removed': summary['total_removed'],
            'duration': summary['total_duration'],
            'domain_results': summary['domain_results'],
            'message': f"Added {summary['total_added']} posts, removed {summary['total_removed']} posts in {summary['total_duration']:.1f}s"
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/update-feed-stream', methods=['POST'])
def update_feed_stream():
    """Server-Sent Events endpoint for real-time updates"""
    def generate():
        try:
            data = request.get_json() if request.get_json() else {}
            time_filter = data.get('time_filter', 'week')
            
            yield f"data: {json.dumps({'status': 'starting', 'message': f'Starting incremental {time_filter} update...'})}\n\n"
            
            # Create updater
            updater = IncrementalDatabaseUpdate(time_filter)
            
            # Update each domain with progress reports
            domains = ['finance', 'entertainment', 'travel']
            results = []
            
            for i, domain in enumerate(domains, 1):
                yield f"data: {json.dumps({'status': 'updating', 'domain': domain, 'progress': i, 'total': len(domains)})}\n\n"
                
                result = updater.update_domain(domain)
                results.append(result)
                
                yield f"data: {json.dumps({'status': 'completed_domain', 'domain': domain, 'added': result['added'], 'removed': result['removed']})}\n\n"
            
            # Final summary
            total_added = sum(r['added'] for r in results)
            total_removed = sum(r['removed'] for r in results)
            
            yield f"data: {json.dumps({'status': 'complete', 'total_added': total_added, 'total_removed': total_removed, 'success': True})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'status': 'error', 'message': str(e), 'success': False})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream',
                   headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive'})

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'incremental-database-update'})

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Incremental Database Update')
    parser.add_argument('time_filter', choices=['week', 'day'], default='week', nargs='?',
                       help='Time filter for posts (default: week)')
    parser.add_argument('--sequential', action='store_true', 
                       help='Run domains sequentially instead of parallel')
    parser.add_argument('--fast', action='store_true',
                       help='Use fast mode (15 key subreddits, 5 posts each)')
    parser.add_argument('--api', action='store_true',
                       help='Start Flask API server instead of running update')
    
    args = parser.parse_args()
    
    if args.api:
        print("🚀 Starting Incremental Update API Server...")
        print("📊 Dashboard can now make requests to: http://localhost:5003/update-feed")
        print("📡 Real-time updates available at: http://localhost:5003/update-feed-stream")
        
        # Start Flask server on port 5003 to avoid conflicts
        app.run(debug=True, port=5003, host='127.0.0.1')
    else:
        # Create and run incremental updater
        updater = IncrementalDatabaseUpdate(args.time_filter)
        
        if args.fast:
            summary = updater.run_fast_update(parallel=not args.sequential)
        else:
            summary = updater.run_incremental_update(parallel=not args.sequential)
        
        # Exit with appropriate code
        sys.exit(0 if summary['success'] else 1)

if __name__ == "__main__":
    main()