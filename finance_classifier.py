#!/usr/bin/env python3
"""
Finance Content Classifier
Specialized classifier for finance-related Reddit content
"""

from base_classifier import BaseClassifier
from config_manager import ConfigManager

class FinanceClassifier(BaseClassifier):
    """
    Finance-specific content classifier
    Inherits common functionality from BaseClassifier
    """
    
    def __init__(self):
        super().__init__()
        self.config_manager = ConfigManager()
        self.domain_config = self.config_manager.get_domain_config('finance')
    
    @property
    def classification_rules(self):
        """Finance classification rules from configuration"""
        return self.domain_config.classification_rules
    
    @property
    def category_minimums(self):
        """Finance category minimums from configuration"""
        return self.domain_config.category_minimums
    
    @property
    def domain_name(self):
        """Domain identifier"""
        return 'finance'

def test_finance_classifier():
    """Test function for finance classifier"""
    print("💰 Testing Finance Content Classifier with Base Architecture")
    print("=" * 60)
    
    classifier = FinanceClassifier()
    
    print(f"🏷️  Configured {len(classifier.classification_rules)} finance categories:")
    for category, rules in classifier.classification_rules.items():
        print(f"  • {category}: {len(rules.get('keywords', []))} keywords, {len(rules.get('patterns', []))} patterns")
    
    print(f"\n📊 Category minimums:")
    for category, minimum in classifier.category_minimums.items():
        print(f"  • {category}: {minimum} posts minimum")
    
    # Test classification on sample posts
    test_posts = [
        ("AAPL earnings beat expectations", "Apple reported strong Q4 earnings...", "stocks"),
        ("SPY calls printing", "My SPY calls are up 200%", "options"),
        ("Bitcoin hits new high", "BTC just broke $50k resistance", "cryptocurrency"),
        ("Day trading strategies", "What are your best scalping techniques?", "daytrading")
    ]
    
    print(f"\n🧪 Testing classification on sample posts:")
    for title, content, subreddit in test_posts:
        category = classifier.classify_single_post(title, content, subreddit)
        print(f"  '{title[:40]}...' → {category}")

if __name__ == "__main__":
    test_finance_classifier()