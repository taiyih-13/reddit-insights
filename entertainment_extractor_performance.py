#!/usr/bin/env python3
"""
High-Performance Entertainment Reddit Extractor
Combines base architecture with performance optimizations
"""

from performance_extractor import PerformanceRedditExtractor
from config_manager import ConfigManager
import time

class EntertainmentRedditExtractorPerformance(PerformanceRedditExtractor):
    """
    High-performance entertainment extractor with parallel processing and caching
    """
    
    def __init__(self, max_workers=8, cache_ttl_hours=1):
        super().__init__(max_workers=max_workers, cache_ttl_hours=cache_ttl_hours)
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

def benchmark_performance():
    """Benchmark entertainment extractor performance"""
    print("🎬 Entertainment Extractor Performance Benchmark")
    print("=" * 55)
    
    # Test with smaller dataset first
    test_limit = 10
    
    print(f"\n🧪 Testing with {test_limit} posts per subreddit...")
    
    # Performance extractor
    perf_extractor = EntertainmentRedditExtractorPerformance(max_workers=8)
    
    print(f"📺 Will extract from {len(perf_extractor.subreddits)} entertainment subreddits")
    
    # Benchmark performance extractor
    start_time = time.time()
    df_performance = perf_extractor.extract_and_process_performance('week', test_limit)
    perf_time = time.time() - start_time
    
    print(f"\n📈 Performance Results:")
    print(f"  ⚡ High-performance: {perf_time:.1f}s ({len(df_performance)} posts)")
    
    # Show cache stats
    perf_extractor.get_cache_stats()
    
    if len(df_performance) > 0:
        print(f"\n✅ Sample posts from performance extractor:")
        for i, (_, row) in enumerate(df_performance.head(3).iterrows(), 1):
            print(f"  {i}. {row['title'][:60]}... (Score: {row['popularity_score']:.1f})")

if __name__ == "__main__":
    benchmark_performance()