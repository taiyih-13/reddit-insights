#!/usr/bin/env python3
"""
Unified Pipeline v2
Uses base architecture for both finance and entertainment data processing
"""

from base_pipeline import BasePipeline
from finance_extractor import FinanceRedditExtractor
from finance_classifier import FinanceClassifier
from entertainment_extractor_new import EntertainmentRedditExtractor
from entertainment_classifier_new import EntertainmentClassifier
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

class FinancePipeline(BasePipeline):
    """Finance-specific pipeline using base architecture"""
    
    def __init__(self):
        self._extractor = FinanceRedditExtractor()
        self._classifier = FinanceClassifier()
        super().__init__()
    
    @property
    def domain_name(self):
        return 'finance'
    
    @property
    def extractor(self):
        return self._extractor
    
    @property
    def classifier(self):
        return self._classifier

class EntertainmentPipeline(BasePipeline):
    """Entertainment-specific pipeline using base architecture"""
    
    def __init__(self):
        self._extractor = EntertainmentRedditExtractor()
        self._classifier = EntertainmentClassifier()
        super().__init__()
    
    @property
    def domain_name(self):
        return 'entertainment'
    
    @property
    def extractor(self):
        return self._extractor
    
    @property
    def classifier(self):
        return self._classifier

class UnifiedPipelineManager:
    """
    Manages both finance and entertainment pipelines
    Provides unified interface for running both domains
    """
    
    def __init__(self):
        self.finance_pipeline = FinancePipeline()
        self.entertainment_pipeline = EntertainmentPipeline()
    
    def run_single_domain(self, domain, time_filter='week', base_limit=100):
        """Run pipeline for a single domain"""
        if domain == 'finance':
            return self.finance_pipeline.extract_and_classify_data(time_filter, base_limit)
        elif domain == 'entertainment':
            return self.entertainment_pipeline.extract_and_classify_data(time_filter, base_limit)
        else:
            raise ValueError(f"Unknown domain: {domain}")
    
    def run_both_domains_parallel(self, time_filter='week', base_limit=100):
        """Run both domains in parallel for a single time period"""
        print(f"\n🚀 Running unified {time_filter} extraction for both domains...")
        
        results = {'finance': pd.DataFrame(), 'entertainment': pd.DataFrame()}
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_domain = {
                executor.submit(self.finance_pipeline.extract_and_classify_data, time_filter, base_limit): 'finance',
                executor.submit(self.entertainment_pipeline.extract_and_classify_data, time_filter, base_limit): 'entertainment'
            }
            
            for future in future_to_domain:
                domain = future_to_domain[future]
                try:
                    result = future.result()
                    results[domain] = result
                    print(f"✅ {domain.title()} {time_filter} extraction completed: {len(result)} posts")
                except Exception as e:
                    print(f"❌ {domain.title()} {time_filter} extraction failed: {e}")
        
        return results
    
    def run_full_pipeline(self, base_limit=100, parallel=True):
        """Run complete pipeline for both domains and both time periods"""
        print("\n🔄 Starting complete unified pipeline...")
        print(f"📊 Base limit per subreddit: {base_limit}")
        print(f"⚡ Parallel processing: {'enabled' if parallel else 'disabled'}")
        
        all_results = {
            'finance': {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()},
            'entertainment': {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
        }
        
        if parallel:
            # Run both time periods for both domains in parallel
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(self.finance_pipeline.extract_and_classify_data, 'week', base_limit): ('finance', 'weekly'),
                    executor.submit(self.finance_pipeline.extract_and_classify_data, 'day', base_limit): ('finance', 'daily'),
                    executor.submit(self.entertainment_pipeline.extract_and_classify_data, 'week', base_limit): ('entertainment', 'weekly'),
                    executor.submit(self.entertainment_pipeline.extract_and_classify_data, 'day', base_limit): ('entertainment', 'daily')
                }
                
                for future in futures:
                    domain, period = futures[future]
                    try:
                        result = future.result()
                        all_results[domain][period] = result
                        print(f"✅ {domain.title()} {period} completed: {len(result)} posts")
                    except Exception as e:
                        print(f"❌ {domain.title()} {period} failed: {e}")
        else:
            # Run sequentially
            for domain in ['finance', 'entertainment']:
                pipeline = getattr(self, f"{domain}_pipeline")
                for time_filter, period in [('week', 'weekly'), ('day', 'daily')]:
                    result = pipeline.extract_and_classify_data(time_filter, base_limit)
                    all_results[domain][period] = result
                    print(f"✅ {domain.title()} {period} completed: {len(result)} posts")
        
        return all_results
    
    def get_combined_status(self):
        """Get status for both pipelines"""
        print("\n📊 Unified Pipeline Status")
        print("=" * 50)
        
        print("\n💰 Finance Pipeline:")
        self.finance_pipeline.get_pipeline_status()
        
        print("\n🎬 Entertainment Pipeline:")
        self.entertainment_pipeline.get_pipeline_status()

def main():
    """Main function demonstrating unified pipeline usage"""
    print("🚀 Unified Reddit Data Pipeline v2")
    print("=" * 50)
    
    manager = UnifiedPipelineManager()
    
    # Show current status
    manager.get_combined_status()
    
    # Run a test extraction
    print("\n🧪 Running test extraction (5 posts per subreddit)...")
    results = manager.run_both_domains_parallel(time_filter='week', base_limit=5)
    
    total_posts = sum(len(df) for df in results.values())
    print(f"\n✅ Test complete: {total_posts} total posts processed")

if __name__ == "__main__":
    main()