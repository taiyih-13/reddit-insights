#!/usr/bin/env python3
"""
Ultimate Performance Pipeline
Combines all performance optimizations: parallel processing, caching, resilience, and memory optimization
"""

from resilient_extractor import ResilientRedditExtractor
from base_classifier import BaseClassifier
from base_pipeline import BasePipeline
from config_manager import ConfigManager
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time
import psutil
import os

class UltimateFinanceExtractor(ResilientRedditExtractor):
    """Ultimate performance finance extractor"""
    
    def __init__(self, max_workers=6):
        super().__init__(max_workers=max_workers, cache_ttl_hours=2)
        self.config_manager = ConfigManager()
        self.domain_config = self.config_manager.get_domain_config('finance')
    
    @property
    def subreddits(self):
        return self.domain_config.subreddits
    
    @property
    def domain_name(self):
        return 'finance'
    
    @property
    def min_popularity_threshold(self):
        return self.domain_config.min_popularity_threshold

class UltimateEntertainmentExtractor(ResilientRedditExtractor):
    """Ultimate performance entertainment extractor"""
    
    def __init__(self, max_workers=6):
        super().__init__(max_workers=max_workers, cache_ttl_hours=2)
        self.config_manager = ConfigManager()
        self.domain_config = self.config_manager.get_domain_config('entertainment')
    
    @property
    def subreddits(self):
        return self.domain_config.subreddits
    
    @property
    def domain_name(self):
        return 'entertainment'
    
    @property
    def min_popularity_threshold(self):
        return self.domain_config.min_popularity_threshold

class PerformanceClassifier(BaseClassifier):
    """Enhanced classifier with performance optimizations"""
    
    def __init__(self, domain_config):
        super().__init__()
        self.domain_config = domain_config
    
    @property
    def classification_rules(self):
        return self.domain_config.classification_rules
    
    @property
    def category_minimums(self):
        return self.domain_config.category_minimums
    
    @property
    def domain_name(self):
        return self.domain_config.name
    
    def process_classification_pipeline_optimized(self, df, chunk_size=5000):
        """
        Optimized classification pipeline for large datasets
        """
        if df.empty:
            return df
        
        print(f"\n🏭 Starting optimized {self.domain_name} classification...")
        start_time = time.time()
        
        # Process in chunks for large datasets
        if len(df) > chunk_size:
            print(f"📊 Processing {len(df)} posts in chunks of {chunk_size}")
            
            classified_chunks = []
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i+chunk_size].copy()
                
                # Apply filters to chunk
                chunk_filtered = self.apply_content_filters(chunk, filter_media=True, filter_low_quality=True)
                if not chunk_filtered.empty:
                    # Classify chunk
                    chunk_classified = self.classify_posts(chunk_filtered)
                    classified_chunks.append(chunk_classified)
            
            if classified_chunks:
                df_classified = pd.concat(classified_chunks, ignore_index=True)
            else:
                df_classified = pd.DataFrame()
        else:
            # Standard processing for smaller datasets
            df_filtered = self.apply_content_filters(df, filter_media=True, filter_low_quality=True)
            df_classified = self.classify_posts(df_filtered) if not df_filtered.empty else pd.DataFrame()
        
        if df_classified.empty:
            print("❌ No posts after classification")
            return df_classified
        
        # Apply balanced selection
        df_balanced = self.apply_balanced_selection(df_classified)
        
        elapsed_time = time.time() - start_time
        print(f"⚡ Optimized classification complete in {elapsed_time:.1f}s")
        
        return df_balanced

class UltimatePipeline:
    """
    Ultimate performance pipeline combining all optimizations
    """
    
    def __init__(self, max_workers=6):
        print("🚀 Initializing Ultimate Performance Pipeline...")
        
        # Initialize extractors
        self.finance_extractor = UltimateFinanceExtractor(max_workers=max_workers)
        self.entertainment_extractor = UltimateEntertainmentExtractor(max_workers=max_workers)
        
        # Initialize classifiers
        config_manager = ConfigManager()
        finance_config = config_manager.get_domain_config('finance')
        entertainment_config = config_manager.get_domain_config('entertainment')
        
        self.finance_classifier = PerformanceClassifier(finance_config)
        self.entertainment_classifier = PerformanceClassifier(entertainment_config)
        
        print("✅ Ultimate pipeline ready")
    
    def run_single_domain_ultimate(self, domain, time_filter='week', base_limit=100):
        """Run ultimate performance pipeline for single domain"""
        print(f"\n🎯 Ultimate {domain} {time_filter} extraction...")
        
        # Select components
        if domain == 'finance':
            extractor = self.finance_extractor
            classifier = self.finance_classifier
        elif domain == 'entertainment':
            extractor = self.entertainment_extractor
            classifier = self.entertainment_classifier
        else:
            raise ValueError(f"Unknown domain: {domain}")
        
        # Track performance
        total_start = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Step 1: Resilient extraction
        print("🛡️  Phase 1: Resilient extraction with caching...")
        df_extracted = extractor.extract_with_full_resilience(time_filter, base_limit)
        
        if df_extracted.empty:
            print("❌ No posts extracted")
            return pd.DataFrame()
        
        # Step 2: Apply scoring and filtering
        print("🧮 Phase 2: Optimized scoring and filtering...")
        df_scored = extractor.ranker.calculate_popularity_score(df_extracted)
        df_filtered = extractor.ranker.apply_filters(df_scored, min_popularity=extractor.min_popularity_threshold)
        
        if df_filtered.empty:
            print("❌ No posts passed filtering")
            return pd.DataFrame()
        
        # Step 3: Optimized classification
        print("🏷️  Phase 3: Optimized classification...")
        df_final = classifier.process_classification_pipeline_optimized(df_filtered)
        
        # Performance summary
        total_time = time.time() - total_start
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_used = final_memory - initial_memory
        
        print(f"\n🎯 Ultimate {domain} pipeline complete!")
        print(f"⏱️  Total time: {total_time:.1f}s")
        print(f"💾 Memory used: {memory_used:.1f}MB")
        print(f"📊 Final posts: {len(df_final)}")
        
        # Show resilience stats
        extractor.get_resilience_stats()
        
        return df_final
    
    def run_both_domains_ultimate(self, time_filter='week', base_limit=100):
        """Run ultimate performance pipeline for both domains in parallel"""
        print(f"\n🚀 Ultimate Dual-Domain {time_filter.title()} Pipeline")
        print("=" * 60)
        
        start_time = time.time()
        results = {'finance': pd.DataFrame(), 'entertainment': pd.DataFrame()}
        
        # Run both domains in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_domain = {
                executor.submit(self.run_single_domain_ultimate, 'finance', time_filter, base_limit): 'finance',
                executor.submit(self.run_single_domain_ultimate, 'entertainment', time_filter, base_limit): 'entertainment'
            }
            
            for future in future_to_domain:
                domain = future_to_domain[future]
                try:
                    result = future.result()
                    results[domain] = result
                    print(f"✅ {domain.title()} complete: {len(result)} posts")
                except Exception as e:
                    print(f"❌ {domain.title()} failed: {e}")
        
        total_time = time.time() - start_time
        total_posts = sum(len(df) for df in results.values())
        
        print(f"\n🎯 Ultimate dual-domain pipeline complete!")
        print(f"⏱️  Total time: {total_time:.1f}s")
        print(f"📊 Total posts: {total_posts}")
        
        return results
    
    def run_full_ultimate_pipeline(self, base_limit=100):
        """Run complete ultimate pipeline: both domains, both time periods"""
        print("\n🚀 ULTIMATE FULL PIPELINE")
        print("=" * 50)
        print(f"🎯 Target: {base_limit} posts per subreddit")
        print(f"🔧 Optimizations: Parallel + Caching + Resilience + Memory")
        
        overall_start = time.time()
        
        # Run all combinations in parallel
        all_results = {
            'finance': {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()},
            'entertainment': {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
        }
        
        # Maximum parallelization: all 4 combinations at once
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(self.run_single_domain_ultimate, 'finance', 'week', base_limit): ('finance', 'weekly'),
                executor.submit(self.run_single_domain_ultimate, 'finance', 'day', base_limit): ('finance', 'daily'),
                executor.submit(self.run_single_domain_ultimate, 'entertainment', 'week', base_limit): ('entertainment', 'weekly'),
                executor.submit(self.run_single_domain_ultimate, 'entertainment', 'day', base_limit): ('entertainment', 'daily')
            }
            
            for future in futures:
                domain, period = futures[future]
                try:
                    result = future.result()
                    all_results[domain][period] = result
                    print(f"✅ {domain.title()} {period} complete: {len(result)} posts")
                except Exception as e:
                    print(f"❌ {domain.title()} {period} failed: {e}")
        
        # Save results to CSV
        for domain in ['finance', 'entertainment']:
            for period, time_filter in [('weekly', 'week'), ('daily', 'day')]:
                df = all_results[domain][period]
                if not df.empty:
                    filename = f"{time_filter}_reddit_posts.csv"
                    df.to_csv(filename, index=False)
                    print(f"💾 Saved {len(df)} {domain} {period} posts to {filename}")
        
        overall_time = time.time() - overall_start
        total_posts = sum(
            len(df) for domain_results in all_results.values() 
            for df in domain_results.values()
        )
        
        print(f"\n🎯 ULTIMATE PIPELINE COMPLETE!")
        print(f"⏱️  Total time: {overall_time:.1f}s")
        print(f"📊 Total posts processed: {total_posts}")
        print(f"⚡ Average speed: {total_posts/overall_time:.1f} posts/second")
        
        return all_results

def benchmark_ultimate_pipeline():
    """Benchmark the ultimate pipeline performance"""
    print("🏁 Ultimate Pipeline Performance Benchmark")
    print("=" * 50)
    
    # Test with moderate dataset
    test_limit = 25
    
    pipeline = UltimatePipeline(max_workers=6)
    
    print(f"\n🧪 Testing with {test_limit} posts per subreddit...")
    
    # Test single domain
    start_time = time.time()
    finance_result = pipeline.run_single_domain_ultimate('finance', 'week', test_limit)
    single_time = time.time() - start_time
    
    print(f"\n📈 Single Domain Performance:")
    print(f"  ⚡ Finance extraction: {single_time:.1f}s ({len(finance_result)} posts)")
    
    # Test dual domain
    start_time = time.time()
    dual_results = pipeline.run_both_domains_ultimate('week', test_limit)
    dual_time = time.time() - start_time
    dual_posts = sum(len(df) for df in dual_results.values())
    
    print(f"\n📈 Dual Domain Performance:")
    print(f"  ⚡ Both domains: {dual_time:.1f}s ({dual_posts} posts)")
    print(f"  🚀 Parallel efficiency: {single_time/dual_time:.1f}x speedup")

if __name__ == "__main__":
    benchmark_ultimate_pipeline()