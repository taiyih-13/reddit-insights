#!/usr/bin/env python3
"""
Entertainment Content Classifier (New Architecture)
Specialized classifier for entertainment-related Reddit content using base architecture
"""

from base_classifier import BaseClassifier
from config_manager import ConfigManager

class EntertainmentClassifier(BaseClassifier):
    """
    Entertainment-specific content classifier
    Inherits common functionality from BaseClassifier
    """
    
    def __init__(self):
        super().__init__()
        self.config_manager = ConfigManager()
        self.domain_config = self.config_manager.get_domain_config('entertainment')
    
    @property
    def classification_rules(self):
        """Entertainment classification rules from configuration"""
        return self.domain_config.classification_rules
    
    @property
    def category_minimums(self):
        """Entertainment category minimums from configuration"""
        return self.domain_config.category_minimums
    
    @property
    def domain_name(self):
        """Domain identifier"""
        return 'entertainment'

def test_entertainment_classifier():
    """Test function for entertainment classifier"""
    print("🎬 Testing Entertainment Content Classifier with Base Architecture")
    print("=" * 65)
    
    classifier = EntertainmentClassifier()
    
    print(f"🏷️  Configured {len(classifier.classification_rules)} entertainment categories:")
    for category, rules in classifier.classification_rules.items():
        subreddit_count = len(rules.get('subreddits', []))
        keyword_count = len(rules.get('keywords', []))
        pattern_count = len(rules.get('patterns', []))
        print(f"  • {category}: {subreddit_count} subreddits, {keyword_count} keywords, {pattern_count} patterns")
    
    print(f"\n📊 Category minimums:")
    for category, minimum in classifier.category_minimums.items():
        print(f"  • {category}: {minimum} posts minimum")
    
    # Test classification on sample posts
    test_posts = [
        ("Best Netflix shows 2024", "What are the top Netflix originals this year?", "netflix"),
        ("Marvel movie review", "Just watched the new Marvel film, thoughts inside", "movies"),
        ("Stranger Things discussion", "Season 4 finale was incredible", "television"),
        ("Horror movie recommendations", "Looking for scary movies like Hereditary", "horror"),
        ("Anime suggestions please", "Need recommendations for action anime", "animesuggest"),
        ("Tip of my tongue movie", "Movie about time travel from the 90s", "tipofmytongue")
    ]
    
    print(f"\n🧪 Testing classification on sample posts:")
    for title, content, subreddit in test_posts:
        category = classifier.classify_single_post(title, content, subreddit)
        print(f"  '{title[:40]}...' → {category}")

if __name__ == "__main__":
    test_entertainment_classifier()