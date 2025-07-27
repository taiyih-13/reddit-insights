#!/usr/bin/env python3
"""
Live Dashboard API
Modern backend for real-time Reddit insights dashboard
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import threading
import time
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from extractors.finance_balanced_extractor import FinanceBalancedExtractor
from extractors.entertainment_balanced_extractor import EntertainmentBalancedExtractorV2
from extractors.travel_tips_extractor import TravelTipsBalancedExtractor
from extractors.nature_adventure_extractor import NatureAdventureBalancedExtractor
from extractors.regional_travel_extractor import RegionalTravelBalancedExtractor
from utils.comment_fetcher import fetch_top_comments

# Import all classifiers for AI-powered categorization
from classifiers.finance_classifier import FinanceClassifier
from classifiers.entertainment_classifier import EntertainmentClassifier
from classifiers.travel_tips_classifier import TravelTipsClassifier  
from classifiers.nature_adventure_classifier import NatureAdventureClassifier
from classifiers.regional_travel_classifier import RegionalTravelClassifier

# Import popularity ranker for post scoring
from utils.popularity_ranker import PopularityRankerV2

# Import AI summarizer for intelligent content summaries
try:
    from services.ai_summarizer import RedditSummarizer
    AI_SUMMARIZER_AVAILABLE = True
except ImportError:
    AI_SUMMARIZER_AVAILABLE = False
    print("⚠️  AI Summarizer not available - check Groq API key")

app = Flask(__name__)
CORS(app)

class LiveDataManager:
    def __init__(self):
        self.extractors = {
            'finance': FinanceBalancedExtractor(),
            'entertainment': EntertainmentBalancedExtractorV2(),
            'travel_tips': TravelTipsBalancedExtractor(),
            'nature_adventure': NatureAdventureBalancedExtractor(),
            'regional_travel': RegionalTravelBalancedExtractor()
        }
        
        # Initialize AI classifiers for content categorization
        self.classifiers = {
            'finance': FinanceClassifier(),
            'entertainment': EntertainmentClassifier(),
            'travel_tips': TravelTipsClassifier(),
            'nature_adventure': NatureAdventureClassifier(),
            'regional_travel': RegionalTravelClassifier()
        }
        
        # Initialize popularity ranker for post scoring
        self.popularity_ranker = PopularityRankerV2()
        
        # Initialize AI summarizer if available
        if AI_SUMMARIZER_AVAILABLE:
            try:
                self.summarizer = RedditSummarizer()
                print("✅ AI Summarizer initialized successfully")
            except Exception as e:
                self.summarizer = None
                print(f"⚠️  Failed to initialize AI Summarizer: {e}")
        else:
            self.summarizer = None
        
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_duration = 300  # 5 minutes cache
        
        # Data storage
        self.assets_dir = project_root / 'assets'
        self.assets_dir.mkdir(exist_ok=True)
        
    def _is_cache_valid(self, key):
        """Check if cached data is still valid"""
        if key not in self.cache_timestamps:
            return False
        return (datetime.now() - self.cache_timestamps[key]).seconds < self.cache_duration
    
    def _load_csv_data(self, category, time_filter):
        """Load data from existing CSV files if available"""
        try:
            csv_path = self.assets_dir / f'{time_filter}_{category}_posts.csv'
            if csv_path.exists():
                df = pd.read_csv(csv_path)
                df['created_utc'] = pd.to_datetime(df['created_utc'])
                return df
        except Exception as e:
            print(f"Error loading CSV for {category} {time_filter}: {e}")
        return pd.DataFrame()
    
    def _fetch_live_data(self, category, time_filter, limit=50):
        """Fetch live data from Reddit API with AI classification and popularity ranking"""
        try:
            extractor = self.extractors[category]
            classifier = self.classifiers[category]
            
            print(f"🤖 Extracting {category} posts with AI classification...")
            
            if hasattr(extractor, 'extract_balanced_posts'):
                # Use balanced extractor
                df = extractor.extract_balanced_posts(
                    time_filter=time_filter,
                    base_limit=min(limit, 25)  # Limit to prevent rate limiting
                )
            else:
                # Fallback to regular extraction
                df = extractor.extract_and_rank_posts(
                    time_filter=time_filter,
                    limit_per_subreddit=min(limit//10, 25),
                    top_n=limit
                )
            
            # Apply AI classification to posts
            if not df.empty and hasattr(classifier, 'classify_posts'):
                print(f"🧠 Applying AI classification to {len(df)} posts...")
                df = classifier.classify_posts(df)
            
            # Apply popularity ranking
            if not df.empty:
                print(f"📊 Calculating popularity scores...")
                df = self.popularity_ranker.calculate_popularity_score(df)
                df = df.sort_values('popularity_score', ascending=False)
            
            return df
            
        except Exception as e:
            print(f"Error fetching live data for {category}: {e}")
            return pd.DataFrame()
    
    def get_posts(self, category, time_filter='week', limit=50, force_refresh=False):
        """Get posts with intelligent caching"""
        cache_key = f"{category}_{time_filter}_{limit}"
        
        # Check cache first
        if not force_refresh and self._is_cache_valid(cache_key):
            print(f"✅ Serving cached data for {category} {time_filter}")
            return self.cache[cache_key]
        
        print(f"🔄 Fetching fresh data for {category} {time_filter}")
        
        # Try to load from CSV first (faster)
        df = self._load_csv_data(category, time_filter)
        
        # If no CSV data or force refresh, fetch live
        if df.empty or force_refresh:
            df = self._fetch_live_data(category, time_filter, limit)
            
            # Save to CSV for future use
            if not df.empty:
                csv_path = self.assets_dir / f'{time_filter}_{category}_posts.csv'
                df.to_csv(csv_path, index=False)
        
        # Limit and format data
        if not df.empty:
            df = df.head(limit)
            
            # Convert to JSON-friendly format
            posts = []
            for _, post in df.iterrows():
                post_data = {
                    'title': post['title'],
                    'author': post.get('author', 'Unknown'),
                    'score': int(post['score']),
                    'upvote_ratio': float(post.get('upvote_ratio', 0)),
                    'num_comments': int(post['num_comments']),
                    'created_utc': post['created_utc'].isoformat() if pd.notnull(post['created_utc']) else None,
                    'url': post['url'],
                    'selftext': post.get('selftext', '')[:200],
                    'subreddit': post.get('subreddit', ''),
                    'post_id': post.get('post_id', ''),
                    'category': post.get('category', 'General'),
                    'popularity_score': float(post.get('popularity_score', 0))
                }
                posts.append(post_data)
            
            result = {
                'posts': posts,
                'total': len(posts),
                'category': category,
                'time_filter': time_filter,
                'last_updated': datetime.now().isoformat()
            }
            
            # Cache the result
            self.cache[cache_key] = result
            self.cache_timestamps[cache_key] = datetime.now()
            
            return result
        
        return {
            'posts': [],
            'total': 0,
            'category': category,
            'time_filter': time_filter,
            'last_updated': datetime.now().isoformat(),
            'error': 'No data available'
        }
    
    def get_dashboard_stats(self):
        """Get overall dashboard statistics"""
        stats = {}
        categories = ['finance', 'entertainment', 'travel_tips', 'nature_adventure', 'regional_travel']
        
        for category in categories:
            try:
                weekly_data = self.get_posts(category, 'week', limit=10)
                daily_data = self.get_posts(category, 'day', limit=10)
                
                stats[category] = {
                    'weekly_posts': weekly_data.get('total', 0),
                    'daily_posts': daily_data.get('total', 0), 
                    'last_updated': weekly_data.get('last_updated', 'Unknown')
                }
            except Exception as e:
                print(f"Error getting stats for {category}: {e}")
                stats[category] = {
                    'weekly_posts': 0,
                    'daily_posts': 0,
                    'last_updated': 'Error'
                }
        
        return {
            'categories': stats,
            'total_categories': len(categories),
            'generated_at': datetime.now().isoformat()
        }
    
    def generate_ai_summary(self, category, time_filter='week'):
        """Generate AI-powered summary for a category using live data"""
        if not self.summarizer:
            return {
                'success': False,
                'error': 'AI Summarizer not available - check Groq API key configuration'
            }
        
        try:
            # Get recent posts for the category
            data = self.get_posts(category, time_filter, limit=50, force_refresh=False)
            posts = data.get('posts', [])
            
            if not posts:
                return {
                    'success': False,
                    'error': f'No posts available for {category} {time_filter} summary'
                }
            
            # Convert posts to DataFrame format expected by summarizer
            df = pd.DataFrame(posts)
            
            # Detect domain for the category
            domain = self.summarizer._detect_domain_from_category(category)
            
            # Prepare posts for summary directly without category filtering
            # Since we already have posts for the specific category, include all of them
            if len(df) == 0:
                return {
                    'success': False,
                    'error': f'No posts found for {category}'
                }
            
            # Sort by popularity and limit to top 30 posts for summarization
            df_sorted = df.sort_values('popularity_score', ascending=False).head(30)
            
            # Create posts text for AI summarization
            posts_text = []
            for _, post in df_sorted.iterrows():
                title = post['title']
                content = str(post.get('selftext', ''))
                
                post_summary = f"Title: {title}"
                if content and content not in ['nan', '', 'None'] and len(content) > 10:
                    if len(content) > 1500:  # Truncate very long content
                        content = content[:1500] + "... [truncated]"
                    post_summary += f"\nContent: {content}"
                
                posts_text.append(post_summary)
            
            combined_posts_text = "\n\n---\n\n".join(posts_text[:25])  # Limit to 25 posts
            total_posts = len(df_sorted)
            
            # Create simplified prompt for testing
            time_filter_for_prompt = 'weekly' if time_filter == 'week' else 'daily'
            
            prompt = f"""
            Analyze the following {total_posts} Reddit posts from the {category} category ({time_filter_for_prompt} data) and provide:

            1. **Key Themes**: What are the main topics being discussed?
            2. **Trending Topics**: What's gaining attention in the community?
            3. **Community Sentiment**: What's the overall mood and direction?
            4. **Notable Insights**: Any interesting patterns or developments?

            Posts to analyze:
            {combined_posts_text}

            Please provide a concise, well-structured analysis in markdown format.
            """
            
            # Call Groq API
            response = self.summarizer.client.chat.completions.create(
                model=self.summarizer.model,
                messages=[
                    {"role": "system", "content": "You are an expert Reddit content analyst."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=4000,
                temperature=0.1
            )
            
            raw_summary = response.choices[0].message.content
            
            # Clean up the summary
            cleaned_summary = self.summarizer.remove_incomplete_sentences(raw_summary, category)
            
            return {
                'success': True,
                'category': category,
                'time_filter': time_filter,
                'domain': domain,
                'total_posts_analyzed': total_posts,
                'summary': cleaned_summary,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Error generating AI summary for {category}: {e}")
            return {
                'success': False,
                'error': f'Failed to generate summary: {str(e)}'
            }

# Initialize data manager
data_manager = LiveDataManager()

# API Routes
@app.route('/api/posts/<category>')
def get_posts(category):
    """Get posts for a specific category"""
    time_filter = request.args.get('time_filter', 'week')
    limit = int(request.args.get('limit', 50))
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    
    if category not in data_manager.extractors:
        return jsonify({'error': f'Category {category} not found'}), 404
    
    try:
        result = data_manager.get_posts(category, time_filter, limit, force_refresh)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/dashboard/stats')
def get_dashboard_stats():
    """Get overall dashboard statistics"""
    try:
        stats = data_manager.get_dashboard_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories')
def get_categories():
    """Get available categories"""
    categories = {
        'finance': 'Finance',
        'entertainment': 'Entertainment', 
        'travel_tips': 'Travel Tips & Advice',
        'nature_adventure': 'Nature & Adventure',
        'regional_travel': 'Regional Travel'
    }
    
    return jsonify({
        'categories': categories,
        'total': len(categories)
    })

@app.route('/api/summary/<category>')
def get_ai_summary(category):
    """Get AI-powered summary for a specific category"""
    if category not in data_manager.extractors:
        return jsonify({'error': f'Category {category} not found'}), 404
    
    time_filter = request.args.get('time_filter', 'week')
    if time_filter not in ['week', 'day']:
        return jsonify({'error': 'time_filter must be "week" or "day"'}), 400
    
    try:
        summary = data_manager.generate_ai_summary(category, time_filter)
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh/<category>')
def refresh_category(category):
    """Force refresh data for a specific category"""
    if category not in data_manager.extractors:
        return jsonify({'error': f'Category {category} not found'}), 404
    
    try:
        # Clear cache
        cache_keys_to_clear = [key for key in data_manager.cache.keys() if key.startswith(category)]
        for key in cache_keys_to_clear:
            del data_manager.cache[key]
            del data_manager.cache_timestamps[key]
        
        # Fetch fresh data
        weekly_data = data_manager.get_posts(category, 'week', limit=50, force_refresh=True)
        daily_data = data_manager.get_posts(category, 'day', limit=50, force_refresh=True)
        
        return jsonify({
            'success': True,
            'category': category,
            'weekly_posts': weekly_data['total'],
            'daily_posts': daily_data['total'],
            'refreshed_at': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'live-dashboard-api',
        'version': '1.0.0',
        'uptime': time.time()
    })

@app.route('/')
def serve_dashboard():
    """Serve the dynamic dashboard"""
    return send_from_directory(str(project_root / 'assets'), 'dynamic_dashboard.html')

if __name__ == '__main__':
    print("🚀 Starting Live Dashboard API...")
    print("=" * 50)
    print("📡 API Endpoints:")
    print("   GET /api/posts/<category>?time_filter=week&limit=50&refresh=false")
    print("   GET /api/summary/<category>?time_filter=week")
    print("   GET /api/dashboard/stats")
    print("   GET /api/categories") 
    print("   GET /api/refresh/<category>")
    print("   GET /api/health")
    print("🌐 Dashboard: http://127.0.0.1:8080/")
    print("🔧 Categories: finance, entertainment, travel_tips, nature_adventure, regional_travel")
    
    # Run the API server
    app.run(host='127.0.0.1', port=8080, debug=True, threaded=True)