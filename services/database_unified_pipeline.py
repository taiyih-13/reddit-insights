#!/usr/bin/env python3
"""
Database-First Unified Pipeline
Replaces CSV-based pipeline with direct Supabase operations
"""

import pandas as pd
import os
import json
from datetime import datetime, timedelta
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.finance_database_extractor import FinanceDatabaseExtractor
from extractors.entertainment_database_extractor import EntertainmentDatabaseExtractor
from extractors.travel_database_extractor import TravelDatabaseExtractor
from services.enhanced_database_service import get_enhanced_db_service

class DatabaseUnifiedPipeline:
    """
    Unified pipeline that extracts and saves directly to Supabase database
    No CSV dependencies - pure database operations
    """
    
    def __init__(self, time_filter='week'):
        self.time_filter = time_filter
        self.db_service = get_enhanced_db_service()
        
        # Initialize extractors
        self.finance_extractor = FinanceDatabaseExtractor()
        self.entertainment_extractor = EntertainmentDatabaseExtractor()
        self.travel_extractor = TravelDatabaseExtractor()
        
        # Extraction configuration
        self.extraction_config = {
            'finance': {
                'extractor': self.finance_extractor,
                'base_limit': 50 if time_filter == 'week' else 20
            },
            'entertainment': {
                'extractor': self.entertainment_extractor,
                'base_limit': 30 if time_filter == 'week' else 15
            },
            'travel': {
                'extractor': self.travel_extractor,
                'base_limit': 20 if time_filter == 'week' else 10
            }
        }
    
    def run_full_extraction(self, parallel=True) -> Dict[str, Any]:
        """
        Run complete extraction for all domains
        
        Args:
            parallel: Whether to run extractors in parallel (faster but more API intensive)
            
        Returns:
            Dictionary with comprehensive results for all domains
        """
        
        print(f"🚀 DATABASE UNIFIED PIPELINE ({self.time_filter.upper()})")
        print("=" * 70)
        print(f"Timestamp: {datetime.now()}")
        print(f"Parallel execution: {parallel}")
        print(f"Target domains: finance, entertainment, travel")
        print(f"Database: Supabase (no CSV files)")
        
        pipeline_start = datetime.now()
        results = {}
        
        if parallel:
            results = self._run_parallel_extraction()
        else:
            results = self._run_sequential_extraction()
        
        pipeline_time = (datetime.now() - pipeline_start).total_seconds()
        
        # Compile comprehensive results
        total_stats = self._compile_results(results, pipeline_time)
        
        # Print final summary
        self._print_final_summary(total_stats)
        
        return total_stats
    
    def _run_parallel_extraction(self) -> Dict[str, Any]:
        """Run all extractors in parallel using ThreadPoolExecutor"""
        
        print(f"\n🔄 PARALLEL EXTRACTION MODE")
        print("⚡ Running all domains simultaneously...")
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all extraction tasks
            future_to_domain = {}
            
            for domain, config in self.extraction_config.items():
                future = executor.submit(
                    config['extractor'].extract_and_save_to_database,
                    time_filter=self.time_filter,
                    base_limit=config['base_limit']
                )
                future_to_domain[future] = domain
            
            # Collect results as they complete
            for future in as_completed(future_to_domain):
                domain = future_to_domain[future]
                try:
                    result = future.result()
                    results[domain] = result
                    print(f"✅ {domain.title()} extraction completed")
                except Exception as e:
                    print(f"❌ {domain.title()} extraction failed: {e}")
                    results[domain] = {
                        'total_posts': 0,
                        'inserted_count': 0,
                        'error_count': 1,
                        'error_message': str(e)
                    }
        
        return results
    
    def _run_sequential_extraction(self) -> Dict[str, Any]:
        """Run extractors sequentially (safer for API limits)"""
        
        print(f"\n🔄 SEQUENTIAL EXTRACTION MODE")
        print("📝 Running domains one at a time...")
        
        results = {}
        
        for domain, config in self.extraction_config.items():
            print(f"\n{'='*20} {domain.upper()} EXTRACTION {'='*20}")
            
            try:
                result = config['extractor'].extract_and_save_to_database(
                    time_filter=self.time_filter,
                    base_limit=config['base_limit']
                )
                results[domain] = result
                print(f"✅ {domain.title()} extraction completed")
                
                # Add delay between domains to be respectful to Reddit API
                print("⏳ Waiting 30 seconds before next domain...")
                time.sleep(30)
                
            except Exception as e:
                print(f"❌ {domain.title()} extraction failed: {e}")
                results[domain] = {
                    'total_posts': 0,
                    'inserted_count': 0,
                    'error_count': 1,
                    'error_message': str(e)
                }
        
        return results
    
    def _compile_results(self, results: Dict[str, Any], pipeline_time: float) -> Dict[str, Any]:
        """Compile comprehensive pipeline statistics"""
        
        total_posts = sum(r.get('total_posts', 0) for r in results.values())
        total_inserted = sum(r.get('inserted_count', 0) for r in results.values())
        total_errors = sum(r.get('error_count', 0) for r in results.values())
        total_api_calls = sum(r.get('api_calls_made', 0) for r in results.values())
        
        # Calculate overall success rate
        overall_success_rate = (total_inserted / total_posts * 100) if total_posts > 0 else 0
        
        # Check computed fields support
        computed_fields_supported = any(r.get('computed_fields_supported', False) for r in results.values())
        
        return {
            'pipeline_time': pipeline_time,
            'timestamp': datetime.now().isoformat(),
            'time_filter': self.time_filter,
            'total_posts_extracted': total_posts,
            'total_posts_saved': total_inserted,
            'total_errors': total_errors,
            'overall_success_rate': overall_success_rate,
            'total_api_calls': total_api_calls,
            'computed_fields_supported': computed_fields_supported,
            'domain_results': results,
            'extraction_method': 'database_direct'
        }
    
    def _print_final_summary(self, stats: Dict[str, Any]):
        """Print comprehensive pipeline summary"""
        
        print(f"\n" + "=" * 70)
        print("📊 DATABASE PIPELINE FINAL SUMMARY")
        print("=" * 70)
        
        print(f"⏱️  Pipeline Duration: {stats['pipeline_time']:.1f} seconds")
        print(f"📅 Time Filter: {stats['time_filter']}")
        print(f"🎯 Extraction Method: {stats['extraction_method']}")
        print(f"💾 Database: Supabase (no CSV files)")
        
        print(f"\n📈 OVERALL STATISTICS:")
        print(f"   Posts Extracted: {stats['total_posts_extracted']}")
        print(f"   Posts Saved: {stats['total_posts_saved']}")
        print(f"   Errors: {stats['total_errors']}")
        print(f"   Success Rate: {stats['overall_success_rate']:.1f}%")
        print(f"   API Calls Made: {stats['total_api_calls']}")
        print(f"   Computed Fields: {'✅ Supported' if stats['computed_fields_supported'] else '❌ Not supported yet'}")
        
        print(f"\n📋 DOMAIN BREAKDOWN:")
        for domain, result in stats['domain_results'].items():
            posts = result.get('total_posts', 0)
            saved = result.get('inserted_count', 0)
            errors = result.get('error_count', 0)
            success_rate = result.get('success_rate', 0)
            extraction_time = result.get('extraction_time', 0)
            
            status = "✅" if errors == 0 else f"⚠️  ({errors} errors)"
            
            print(f"   {domain.title()}: {saved}/{posts} posts saved ({success_rate:.1f}%) {status}")
            print(f"      Extraction time: {extraction_time:.1f}s")
            
            # Show category breakdown if available
            if 'category_breakdown' in result:
                categories = result['category_breakdown']
                if categories:
                    top_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)[:3]
                    category_str = ", ".join([f"{cat}: {count}" for cat, count in top_categories])
                    print(f"      Top categories: {category_str}")
        
        print(f"\n✨ NEXT STEPS:")
        if not stats['computed_fields_supported']:
            print("   1. Add computed_fields column to Supabase posts table")
            print("   2. Re-run pipeline to store sentiment scores and other computed fields")
        print(f"   3. Generate dashboard: python utils/dashboard_generator.py")
        print(f"   4. View results: open assets/reddit_dashboard.html")
        
        print("=" * 70)
    
    def cleanup_old_posts(self, retention_days: int = None) -> int:
        """Clean up old posts from database"""
        
        if retention_days is None:
            retention_days = 7 if self.time_filter == 'week' else 1
        
        print(f"\n🧹 CLEANING UP OLD POSTS (older than {retention_days} days)")
        
        try:
            deleted_count = self.db_service.cleanup_old_posts(retention_days)
            print(f"✅ Cleaned up {deleted_count} old posts")
            return deleted_count
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            return 0
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        
        print(f"\n📊 DATABASE STATISTICS")
        print("-" * 30)
        
        # Get table counts
        table_counts = self.db_service.get_table_counts()
        
        # Get domain stats
        domain_stats = {}
        for domain in ['finance', 'entertainment', 'travel']:
            stats = self.db_service.get_domain_stats(domain, self.time_filter)
            domain_stats[domain] = stats
        
        # Test connection
        connection_status = self.db_service.test_connection()
        
        db_stats = {
            'table_counts': table_counts,
            'domain_stats': domain_stats,
            'connection_status': connection_status,
            'computed_fields_supported': self.db_service.computed_fields_supported
        }
        
        # Print stats
        print(f"Connection: {'✅ Active' if connection_status['read_connection'] else '❌ Failed'}")
        print(f"Total posts: {table_counts.get('posts', 0)}")
        
        for domain, stats in domain_stats.items():
            total = stats.get('total_posts', 0)
            avg_score = stats.get('avg_score', 0)
            print(f"   {domain.title()}: {total} posts (avg score: {avg_score:.1f})")
        
        return db_stats

def main():
    """Main execution function"""
    
    # Parse command line arguments
    time_filter = 'week'  # default
    parallel = True  # default
    
    if len(sys.argv) > 1:
        if sys.argv[1] in ['day', 'daily']:
            time_filter = 'day'
        elif sys.argv[1] in ['week', 'weekly']:
            time_filter = 'week'
    
    if len(sys.argv) > 2:
        if sys.argv[2] in ['sequential', 'seq']:
            parallel = False
    
    # Initialize and run pipeline
    pipeline = DatabaseUnifiedPipeline(time_filter=time_filter)
    
    # Get initial database stats
    pipeline.get_database_stats()
    
    # Run extraction
    results = pipeline.run_full_extraction(parallel=parallel)
    
    # Clean up old posts
    pipeline.cleanup_old_posts()
    
    # Save results to file for reference
    results_file = f'assets/database_pipeline_results_{time_filter}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n📄 Detailed results saved to: {results_file}")
    
    return results

if __name__ == "__main__":
    main()