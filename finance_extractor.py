#!/usr/bin/env python3
"""
Finance Reddit Extractor
Specialized extractor for finance-related Reddit content
"""

from base_extractor import BaseRedditExtractor
from config_manager import ConfigManager

class FinanceRedditExtractor(BaseRedditExtractor):
    """
    Finance-specific Reddit extractor
    Inherits common functionality from BaseRedditExtractor
    """
    
    def __init__(self):
        super().__init__()
        self.config_manager = ConfigManager()
        self.domain_config = self.config_manager.get_domain_config('finance')
    
    @property
    def subreddits(self):
        """Finance subreddits from configuration"""
        return self.domain_config.subreddits
    
    @property
    def domain_name(self):
        """Domain identifier"""
        return 'finance'
    
    @property
    def min_popularity_threshold(self):
        """Finance-specific popularity threshold"""
        return self.domain_config.min_popularity_threshold

def test_finance_extractor():
    """Test function for finance extractor"""
    print("💰 Testing Finance Reddit Extractor with Base Architecture")
    print("=" * 60)
    
    extractor = FinanceRedditExtractor()
    
    print(f"📊 Configured {len(extractor.subreddits)} finance subreddits:")
    for i, subreddit in enumerate(extractor.subreddits, 1):
        print(f"  {i:2d}. r/{subreddit}")
    
    print(f"\n🎯 Popularity threshold: {extractor.min_popularity_threshold}")
    
    # Test with small extraction
    print(f"\n🔍 Testing extraction pipeline...")
    df_processed = extractor.extract_and_process(time_filter='week', base_limit=5)
    
    if not df_processed.empty:
        print(f"✅ Successfully processed {len(df_processed)} posts")
        print(f"\nSample processed posts:")
        for i, (_, row) in enumerate(df_processed.head(3).iterrows(), 1):
            print(f"  {i}. {row['title'][:60]}... (Score: {row['popularity_score']:.1f})")
    else:
        print("❌ No posts processed")

if __name__ == "__main__":
    test_finance_extractor()