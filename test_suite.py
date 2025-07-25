#!/usr/bin/env python3
"""
Automated Testing Suite for Reddit Insights
Comprehensive testing framework for the base architecture and performance optimizations
"""

import unittest
import pandas as pd
import os
import time
import tempfile
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta

# Import modules to test
from config_manager import ConfigManager
from environment_manager import EnvironmentManager
from base_extractor import BaseRedditExtractor
from base_classifier import BaseClassifier
from base_pipeline import BasePipeline
from finance_extractor import FinanceRedditExtractor
from finance_classifier import FinanceClassifier

class TestConfigManager(unittest.TestCase):
    """Test the centralized configuration system"""
    
    def setUp(self):
        self.temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        self.temp_config.close()
        self.config_manager = ConfigManager(self.temp_config.name)
    
    def tearDown(self):
        if os.path.exists(self.temp_config.name):
            os.unlink(self.temp_config.name)
    
    def test_config_initialization(self):
        """Test configuration manager initialization"""
        self.assertIsInstance(self.config_manager, ConfigManager)
        self.assertIn('domains', self.config_manager.config)
        self.assertIn('finance', self.config_manager.config['domains'])
        self.assertIn('entertainment', self.config_manager.config['domains'])
    
    def test_domain_config_retrieval(self):
        """Test retrieving domain configurations"""
        finance_config = self.config_manager.get_domain_config('finance')
        
        self.assertEqual(finance_config.name, 'finance')
        self.assertIsInstance(finance_config.subreddits, list)
        self.assertGreater(len(finance_config.subreddits), 0)
        self.assertIsInstance(finance_config.min_popularity_threshold, int)
        self.assertIsInstance(finance_config.category_minimums, dict)
    
    def test_config_validation(self):
        """Test configuration validation"""
        self.assertTrue(self.config_manager.validate_config())
    
    def test_domain_update(self):
        """Test updating domain configuration"""
        original_threshold = self.config_manager.get_domain_config('finance').min_popularity_threshold
        new_threshold = original_threshold + 100
        
        self.config_manager.update_domain_config('finance', min_popularity_threshold=new_threshold)
        updated_config = self.config_manager.get_domain_config('finance')
        
        self.assertEqual(updated_config.min_popularity_threshold, new_threshold)

class TestEnvironmentManager(unittest.TestCase):
    """Test environment variable management"""
    
    def setUp(self):
        self.temp_env = tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False)
        self.temp_env.write("TEST_VAR=test_value\\nNUMERIC_VAR=42\\nBOOL_VAR=true\\n")
        self.temp_env.close()
        
        self.env_manager = EnvironmentManager(self.temp_env.name)
    
    def tearDown(self):
        if os.path.exists(self.temp_env.name):
            os.unlink(self.temp_env.name)
    
    def test_env_loading(self):
        """Test environment variable loading"""
        # Test string variable
        value = self.env_manager.get_var('TEST_VAR', 'default')
        self.assertEqual(value, 'test_value')
        
        # Test numeric variable
        numeric_value = self.env_manager.get_var('NUMERIC_VAR', 0, int)
        self.assertEqual(numeric_value, 42)
        
        # Test boolean variable
        bool_value = self.env_manager.get_var('BOOL_VAR', False, bool)
        self.assertTrue(bool_value)
    
    def test_env_validation(self):
        """Test environment validation"""
        validation = self.env_manager.validate_environment()
        
        self.assertIsInstance(validation, dict)
        self.assertIn('valid', validation)
        self.assertIn('errors', validation)
        self.assertIn('config_values', validation)
    
    def test_template_generation(self):
        """Test .env template generation"""
        template_file = tempfile.mktemp(suffix='.env.template')
        
        try:
            self.env_manager.generate_env_template(template_file)
            self.assertTrue(os.path.exists(template_file))
            
            with open(template_file, 'r') as f:
                content = f.read()
                self.assertIn('REDDIT_CLIENT_ID', content)
                self.assertIn('GROQ_API_KEY', content)
        finally:
            if os.path.exists(template_file):
                os.unlink(template_file)

class TestBaseExtractor(unittest.TestCase):
    """Test base extractor functionality"""
    
    def setUp(self):
        # Create a concrete implementation for testing
        class TestExtractor(BaseRedditExtractor):
            @property
            def subreddits(self):
                return ['test_subreddit']
            
            @property
            def domain_name(self):
                return 'test'
            
            @property
            def min_popularity_threshold(self):
                return 100
        
        with patch('base_extractor.praw.Reddit') as mock_reddit:
            self.extractor = TestExtractor()
            self.mock_reddit = mock_reddit.return_value
    
    def test_extractor_initialization(self):
        """Test extractor initialization"""
        self.assertEqual(self.extractor.domain_name, 'test')
        self.assertEqual(self.extractor.subreddits, ['test_subreddit'])
        self.assertEqual(self.extractor.min_popularity_threshold, 100)
    
    def test_post_data_extraction(self):
        """Test post data extraction"""
        # Mock a Reddit post
        mock_post = Mock()
        mock_post.title = "Test Post"
        mock_post.author = "test_user"
        mock_post.score = 500
        mock_post.num_comments = 25
        mock_post.created_utc = time.time()
        mock_post.url = "https://reddit.com/test"
        mock_post.selftext = "Test content"
        mock_post.id = "test123"
        
        post_data = self.extractor._extract_post_data(mock_post, 'test_subreddit')
        
        self.assertEqual(post_data['title'], "Test Post")
        self.assertEqual(post_data['subreddit'], 'test_subreddit')
        self.assertEqual(post_data['score'], 500)
        self.assertEqual(post_data['num_comments'], 25)

class TestBaseClassifier(unittest.TestCase):
    """Test base classifier functionality"""
    
    def setUp(self):
        # Create a concrete implementation for testing  
        class TestClassifier(BaseClassifier):
            @property
            def classification_rules(self):
                return {
                    'test_category': {
                        'keywords': ['test', 'sample'],
                        'patterns': [r'test.*pattern']
                    }
                }
            
            @property
            def category_minimums(self):
                return {'test_category': 5}
            
            @property
            def domain_name(self):
                return 'test'
        
        self.classifier = TestClassifier()
    
    def test_classifier_initialization(self):
        """Test classifier initialization"""
        self.assertEqual(self.classifier.domain_name, 'test')
        self.assertIn('test_category', self.classifier.classification_rules)
        self.assertEqual(self.classifier.category_minimums['test_category'], 5)
    
    def test_single_post_classification(self):
        """Test classifying a single post"""
        # Test keyword matching
        category = self.classifier.classify_single_post("This is a test post", "More test content")
        self.assertEqual(category, 'test_category')
        
        # Test pattern matching
        category = self.classifier.classify_single_post("test pattern example", "")
        self.assertEqual(category, 'test_category')
        
        # Test no match
        category = self.classifier.classify_single_post("Unrelated content", "Nothing relevant")
        self.assertEqual(category, 'other')
    
    def test_media_content_filtering(self):
        """Test media content filtering"""
        image_url = "https://i.imgur.com/test.jpg"
        video_url = "https://v.redd.it/test.mp4"
        text_url = "https://reddit.com/r/test"
        
        self.assertTrue(self.classifier.is_media_content(image_url))
        self.assertTrue(self.classifier.is_media_content(video_url))
        self.assertFalse(self.classifier.is_media_content(text_url))

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system"""
    
    def test_finance_extractor_integration(self):
        """Test finance extractor with configuration"""
        with patch('finance_extractor.praw.Reddit'):
            extractor = FinanceRedditExtractor()
            
            self.assertEqual(extractor.domain_name, 'finance')
            self.assertIsInstance(extractor.subreddits, list)
            self.assertGreater(len(extractor.subreddits), 0)
            self.assertIn('wallstreetbets', extractor.subreddits)
    
    def test_finance_classifier_integration(self):
        """Test finance classifier with configuration"""
        classifier = FinanceClassifier()
        
        self.assertEqual(classifier.domain_name, 'finance')
        self.assertIsInstance(classifier.classification_rules, dict)
        self.assertIn('stocks', classifier.classification_rules)
        self.assertIsInstance(classifier.category_minimums, dict)
    
    def test_pipeline_state_management(self):
        """Test pipeline state management"""
        class TestPipeline(BasePipeline):
            @property
            def domain_name(self):
                return 'test_pipeline'
            
            @property
            def extractor(self):
                return Mock()
            
            @property
            def classifier(self):
                return Mock()
        
        pipeline = TestPipeline()
        
        # Test state initialization
        self.assertIn('extractions', pipeline.state)
        self.assertIn('statistics', pipeline.state)
        
        # Test state updates
        pipeline.update_extraction_state('weekly', 'success', 100)
        self.assertEqual(pipeline.state['extractions']['weekly']['status'], 'success')
        self.assertEqual(pipeline.state['extractions']['weekly']['count'], 100)

class TestPerformance(unittest.TestCase):
    """Performance and benchmarking tests"""
    
    def test_dataframe_optimization(self):
        """Test DataFrame memory optimization"""
        from performance_extractor import PerformanceRedditExtractor
        
        class TestPerformanceExtractor(PerformanceRedditExtractor):
            @property
            def subreddits(self):
                return ['test']
            
            @property
            def domain_name(self):
                return 'test'
            
            @property
            def min_popularity_threshold(self):
                return 100
        
        with patch('performance_extractor.praw.Reddit'):
            extractor = TestPerformanceExtractor()
            
            # Create test DataFrame
            test_data = {
                'subreddit': ['test'] * 100,
                'score': range(100),
                'num_comments': range(100),
                'over_18': [False] * 100
            }
            df = pd.DataFrame(test_data)
            
            # Test optimization
            optimized_df = extractor._optimize_dataframe(df)
            
            # Check that categorical columns are converted
            self.assertEqual(optimized_df['subreddit'].dtype.name, 'category')
            self.assertEqual(optimized_df['over_18'].dtype, 'bool')
    
    def test_caching_functionality(self):
        """Test caching system"""
        from performance_extractor import RedditCache
        
        cache = RedditCache(cache_file=tempfile.mktemp(), ttl_hours=1)
        
        # Test cache set/get
        cache.set('test_key', {'data': 'test_value'})
        cached_data = cache.get('test_key')
        
        self.assertEqual(cached_data, {'data': 'test_value'})
        
        # Test TTL expiration (mock time)
        with patch('performance_extractor.datetime') as mock_datetime:
            # Set time to 2 hours in the future
            mock_datetime.now.return_value = datetime.now() + timedelta(hours=2)
            
            expired_data = cache.get('test_key')
            self.assertIsNone(expired_data)

class TestSuite:
    """Main test suite runner"""
    
    def __init__(self):
        self.test_modules = [
            TestConfigManager,
            TestEnvironmentManager,
            TestBaseExtractor,
            TestBaseClassifier,
            TestIntegration,
            TestPerformance
        ]
    
    def run_all_tests(self, verbose=True):
        """Run all test modules"""
        print("🧪 Reddit Insights Test Suite")
        print("=" * 50)
        
        total_tests = 0
        total_failures = 0
        total_errors = 0
        
        for test_class in self.test_modules:
            print(f"\\n🔍 Running {test_class.__name__}...")
            
            suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
            runner = unittest.TextTestRunner(verbosity=2 if verbose else 1, stream=open(os.devnull, 'w') if not verbose else None)
            result = runner.run(suite)
            
            total_tests += result.testsRun
            total_failures += len(result.failures)
            total_errors += len(result.errors)
            
            if result.wasSuccessful():
                print(f"  ✅ {result.testsRun} tests passed")
            else:
                print(f"  ❌ {len(result.failures)} failures, {len(result.errors)} errors")
                
                if verbose:
                    for test, error in result.failures + result.errors:
                        print(f"    • {test}: {error.split('\\n')[-2]}")
        
        print("\\n" + "=" * 50)
        print(f"📊 Test Summary:")
        print(f"  Total tests: {total_tests}")
        print(f"  Passed: {total_tests - total_failures - total_errors}")
        print(f"  Failed: {total_failures}")
        print(f"  Errors: {total_errors}")
        
        success_rate = ((total_tests - total_failures - total_errors) / total_tests * 100) if total_tests > 0 else 0
        print(f"  Success rate: {success_rate:.1f}%")
        
        if total_failures == 0 and total_errors == 0:
            print("\\n🎯 All tests passed! System is ready for production.")
            return True
        else:
            print(f"\\n⚠️  {total_failures + total_errors} tests failed. Please review issues above.")
            return False
    
    def run_quick_tests(self):
        """Run a subset of critical tests quickly"""
        print("⚡ Quick Test Suite")
        print("=" * 30)
        
        quick_tests = [TestConfigManager, TestEnvironmentManager]
        
        for test_class in quick_tests:
            suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
            runner = unittest.TextTestRunner(verbosity=1)
            result = runner.run(suite)
            
            if not result.wasSuccessful():
                return False
        
        print("✅ Quick tests passed!")
        return True

def main():
    """Main test runner"""
    import sys
    
    test_suite = TestSuite()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'quick':
        success = test_suite.run_quick_tests()
    else:
        success = test_suite.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()