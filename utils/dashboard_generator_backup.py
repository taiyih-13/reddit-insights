import pandas as pd
from datetime import datetime
import json
import html
import os
import sys

# Add the parent directory to the Python path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.sentiment_analyzer import StockSentimentAnalyzer
from utils.optimized_entertainment_sentiment_analyzer import OptimizedEntertainmentSentimentAnalyzer
from utils.travel_city_tracker import TravelCityTracker

# Database imports (database-first)
try:
    from services.enhanced_database_service import get_enhanced_db_service
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("⚠️  Database not available")

class CleanRedditDashboard:
    def __init__(self, assets_directory='assets', use_database=True):
        self.assets_directory = assets_directory
        self.datasets = {}
        self.use_database = use_database and DATABASE_AVAILABLE
        
        # Initialize database connection if available
        self.db_service = None
        if self.use_database:
            try:
                self.db_service = get_enhanced_db_service()
                print("✅ Using enhanced database for dashboard data")
            except Exception as e:
                print(f"❌ Database connection failed: {e}")
                self.use_database = False
        
        # Initialize sentiment analyzers
        self.sentiment_analyzer = StockSentimentAnalyzer()
        self.entertainment_sentiment_analyzer = OptimizedEntertainmentSentimentAnalyzer()
        self.travel_city_tracker = TravelCityTracker()
        
        # Auto-discover all available data sources
        self._discover_datasets()
        
        # Set default dataset for backwards compatibility
        if 'finance' in self.datasets and not self.datasets['finance']['weekly'].empty:
            self.df = self.datasets['finance']['weekly']
        elif 'finance' in self.datasets and not self.datasets['finance']['daily'].empty:
            self.df = self.datasets['finance']['daily']
        else:
            self.df = pd.DataFrame()
        
        # FORCE legacy attribute setup here (critical for UI compatibility)
        print("🔧 Setting up legacy attributes for UI compatibility...")
        self.weekly_df = self.datasets.get('finance', {}).get('weekly', pd.DataFrame())
        self.daily_df = self.datasets.get('finance', {}).get('daily', pd.DataFrame())
        self.weekly_entertainment_df = self.datasets.get('entertainment', {}).get('weekly', pd.DataFrame())
        self.daily_entertainment_df = self.datasets.get('entertainment', {}).get('daily', pd.DataFrame())
        self.weekly_travel_df = self.datasets.get('travel', {}).get('weekly', pd.DataFrame()) 
        self.daily_travel_df = self.datasets.get('travel', {}).get('daily', pd.DataFrame())
        
        print(f"✅ Legacy attributes set:")
        print(f"   • Finance: weekly={len(self.weekly_df)}, daily={len(self.daily_df)}")
        print(f"   • Entertainment: weekly={len(self.weekly_entertainment_df)}, daily={len(self.daily_entertainment_df)}")
        print(f"   • Travel: weekly={len(self.weekly_travel_df)}, daily={len(self.daily_travel_df)}")
    
    def _add_ui_compatibility_fields(self, df: pd.DataFrame, domain: str) -> pd.DataFrame:
        """Add fields required for UI compatibility"""
        
        # Ensure required columns exist with defaults
        required_columns = {
            'sentiment_score': 0.0,
            'sentiment_label': 'neutral',
            'hours_old': 0.0,
            'base_score': 0.0,
            'category': 'General',
            'top_comments': '[]',
            'post_id': ''
        }
        
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value
        
        # Use 'id' as 'post_id' if post_id is missing
        if 'post_id' not in df.columns and 'id' in df.columns:
            df['post_id'] = df['id']
        
        # Domain-specific attributes
        if domain == 'finance':
            if 'stock_tickers' not in df.columns:
                df['stock_tickers'] = ''
        elif domain == 'entertainment':
            if 'entertainment_titles' not in df.columns:
                df['entertainment_titles'] = ''
        elif domain == 'travel':
            if 'travel_subcategory' not in df.columns:
                df['travel_subcategory'] = ''
        
        # Ensure proper data types
        df['sentiment_score'] = pd.to_numeric(df['sentiment_score'], errors='coerce').fillna(0.0)
        df['hours_old'] = pd.to_numeric(df['hours_old'], errors='coerce').fillna(0.0)
        df['popularity_score'] = pd.to_numeric(df['popularity_score'], errors='coerce').fillna(0.0)
        
        return df
    
    def _discover_datasets(self):
        """Automatically discover and load all available datasets"""
        print("🔍 Discovering available datasets...")
        
        # Define expected categories - travel will be combined
        categories = ['finance', 'entertainment', 'travel']
        
        for category in categories:
            self.datasets[category] = {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
            
            if self.use_database and self.db_service:
                # Load from enhanced database service
                try:
                    weekly_df = self.db_service.get_posts_with_computed_fields(category, 'week')
                    if not weekly_df.empty:
                        # Ensure created_utc is datetime
                        weekly_df['created_utc'] = pd.to_datetime(weekly_df['created_utc'])
                        
                        # Add required fields for UI compatibility
                        weekly_df = self._add_ui_compatibility_fields(weekly_df, category)
                        
                        self.datasets[category]['weekly'] = weekly_df
                        print(f"✅ Loaded weekly {category} from database: {len(weekly_df)} posts")
                        
                    daily_df = self.db_service.get_posts_with_computed_fields(category, 'day')
                    if not daily_df.empty:
                        daily_df['created_utc'] = pd.to_datetime(daily_df['created_utc'])
                        
                        # Add required fields for UI compatibility
                        daily_df = self._add_ui_compatibility_fields(daily_df, category)
                        
                        self.datasets[category]['daily'] = daily_df
                        print(f"✅ Loaded daily {category} from database: {len(daily_df)} posts")
                except Exception as e:
                    print(f"❌ Enhanced database load failed for {category}: {e}")
                    # Continue to next category
                    self._load_csv_data(category)
            else:
                # Load from CSV files
                self._load_csv_data(category)
    
    def _load_csv_data(self, category):
        """Load data from CSV files (fallback method)"""
        # Try to load weekly data
        weekly_file = os.path.join(self.assets_directory, f'week_{category}_posts.csv')
        if os.path.exists(weekly_file):
            try:
                df = pd.read_csv(weekly_file)
                df['created_utc'] = pd.to_datetime(df['created_utc'])
                self.datasets[category]['weekly'] = df
                print(f"✅ Loaded weekly {category}: {len(df)} posts")
            except Exception as e:
                print(f"❌ Error loading weekly {category}: {e}")
        
        # Try to load daily data
        daily_file = os.path.join(self.assets_directory, f'day_{category}_posts.csv')
        if os.path.exists(daily_file):
            try:
                df = pd.read_csv(daily_file)
                df['created_utc'] = pd.to_datetime(df['created_utc'])
                self.datasets[category]['daily'] = df
                print(f"✅ Loaded daily {category}: {len(df)} posts")
            except Exception as e:
                print(f"❌ Error loading daily {category}: {e}")
        
        # Combine travel categories into one unified "travel" category
        # Travel categories are already unified in the new extractor
        
        # Summary
        categories = ['finance', 'entertainment', 'travel']  # Redeclare for scope
        try:
            available_categories = [cat for cat in categories if not self.datasets[cat]['weekly'].empty or not self.datasets[cat]['daily'].empty]
            # Travel is now unified, no need to combine categories
            print(f"📊 Available categories: {', '.join(available_categories)}")
        except Exception as e:
            print(f"❌ Error in summary section: {e}")
            available_categories = []
        
        # Legacy attribute mappings for backwards compatibility
        try:
            print("🔧 Setting legacy attributes...")
            if 'finance' in self.datasets:
                self.weekly_df = self.datasets['finance']['weekly']
                self.daily_df = self.datasets['finance']['daily']
                print(f"✅ Finance legacy attrs set: weekly={len(self.weekly_df)}, daily={len(self.daily_df)}")
            else:
                self.weekly_df = pd.DataFrame()
                self.daily_df = pd.DataFrame()
                print("⚠️ No finance data, using empty DataFrames")
            if 'entertainment' in self.datasets:
                self.weekly_entertainment_df = self.datasets['entertainment']['weekly']
                self.daily_entertainment_df = self.datasets['entertainment']['daily']
                print(f"✅ Entertainment legacy attrs set: weekly={len(self.weekly_entertainment_df)}, daily={len(self.daily_entertainment_df)}")
            else:
                self.weekly_entertainment_df = pd.DataFrame()
                self.daily_entertainment_df = pd.DataFrame()
                print("⚠️ No entertainment data, using empty DataFrames")
            if 'travel' in self.datasets:
                self.weekly_travel_df = self.datasets['travel']['weekly']
                self.daily_travel_df = self.datasets['travel']['daily']
                print(f"✅ Travel legacy attrs set: weekly={len(self.weekly_travel_df)}, daily={len(self.daily_travel_df)}")
            else:
                self.weekly_travel_df = pd.DataFrame()
                self.daily_travel_df = pd.DataFrame()
                print("⚠️ No travel data, using empty DataFrames")
            print("🎯 Legacy attributes setup complete!")
        except Exception as e:
            print(f"❌ Error setting legacy attributes: {e}")
            import traceback
            traceback.print_exc()
            # Set defaults
            self.weekly_df = pd.DataFrame()
            self.daily_df = pd.DataFrame()
            self.weekly_entertainment_df = pd.DataFrame()
            self.daily_entertainment_df = pd.DataFrame()
            self.weekly_travel_df = pd.DataFrame()
            self.daily_travel_df = pd.DataFrame()
    
    def _combine_travel_categories(self):
        """Combine travel_tips and regional_travel into one 'travel' category with 6 categories"""
        print("🔄 Combining travel categories into unified Travel category...")
        
        self.datasets['travel'] = {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
        
        for time_filter in ['weekly', 'daily']:
            combined_data = []
            
            # Combine travel_tips data
            if 'travel_tips' in self.datasets and not self.datasets['travel_tips'][time_filter].empty:
                df = self.datasets['travel_tips'][time_filter].copy()
                df['main_category'] = 'Travel'
                if 'travel_subcategory' in df.columns:
                    # Map travel tips categories with better names
                    travel_tips_mapping = {
                        'travel_advice': 'Travel Advice',
                        'general': 'Travel Advice'  # Legacy mapping
                    }
                    df['category'] = df['travel_subcategory'].map(travel_tips_mapping).fillna(df['travel_subcategory'].str.title())
                else:
                    df['category'] = 'Travel Advice'
                combined_data.append(df)
            
            # Combine regional_travel data - map new 6 regional categories
            if 'regional_travel' in self.datasets and not self.datasets['regional_travel'][time_filter].empty:
                df = self.datasets['regional_travel'][time_filter].copy()
                df['main_category'] = 'Travel'
                if 'regional_subcategory' in df.columns:
                    # Map the new travel-focused categories
                    category_mapping = {
                        'asian_travel': 'Asian Travel',
                        'european_travel': 'European Travel',
                        'north_america_travel': 'North America Travel',
                        'south_america_travel': 'South America Travel',
                        'oceania_africa_travel': 'Oceania Africa Travel'
                    }
                    df['category'] = df['regional_subcategory'].map(category_mapping).fillna(df['regional_subcategory'].str.title())
                else:
                    df['category'] = 'International'
                combined_data.append(df)
            
            # Combine all travel data
            if combined_data:
                combined_df = pd.concat(combined_data, ignore_index=True)
                self.datasets['travel'][time_filter] = combined_df
                print(f"✅ Combined {time_filter} travel data: {len(combined_df)} posts across {combined_df['category'].nunique()} categories")
    
    def _generate_category_options(self):
        """Generate dropdown options for available categories"""
        category_display_names = {
            'finance': 'Finance',
            'entertainment': 'Entertainment',
            'travel': 'Travel'
        }
        
        options = []
        # Use only main categories (finance, entertainment, travel)
        available_categories = ['finance', 'entertainment']
        if 'travel' in self.datasets and (not self.datasets['travel']['weekly'].empty or not self.datasets['travel']['daily'].empty):
            available_categories.append('travel')
        
        for i, category in enumerate(available_categories):
            display_name = category_display_names.get(category, category.replace('_', ' ').title())
            selected = ' selected' if i == 0 else ''  # Select first available category
            options.append(f'                        <option value="{category}"{selected}>{display_name}</option>')
        
        return '\n'.join(options)
    
    def _generate_all_category_content(self):
        """Generate content areas for all available categories dynamically"""
        category_id_map = {
            'finance': 'financeCategory',
            'entertainment': 'moviesShowsCategory',  # Keep legacy name for compatibility
            'travel': 'travelCategory'
        }
        
        # Use only main categories (finance, entertainment, travel)
        available_categories = ['finance', 'entertainment']
        if 'travel' in self.datasets and (not self.datasets['travel']['weekly'].empty or not self.datasets['travel']['daily'].empty):
            available_categories.append('travel')
        
        content_html = ""
        
        for i, category in enumerate(available_categories):
            category_id = category_id_map.get(category, f"{category}Category")
            active_class = "active" if i == 0 else ""  # First category is active
            
            # Generate time content IDs that match JavaScript expectations
            time_content_map = {
                'finance': {'weekly': 'weeklyContent', 'daily': 'dailyContent'},
                'entertainment': {'weekly': 'weeklyMoviesContent', 'daily': 'dailyMoviesContent'},
                'travel': {'weekly': 'weeklyTravelContent', 'daily': 'dailyTravelContent'}
            }
            
            weekly_id = time_content_map.get(category, {}).get('weekly', f'weekly{category.title().replace("_", "")}Content')
            daily_id = time_content_map.get(category, {}).get('daily', f'daily{category.title().replace("_", "")}Content')
            
            content_html += f"""
                <div id="{category_id}" class="category-content {active_class}">
                    <div id="{weekly_id}" class="time-content active">
                        {self._generate_category_posts_html(category, 'weekly')}
                    </div>
                    <div id="{daily_id}" class="time-content">
                        {self._generate_category_posts_html(category, 'daily')}
                    </div>
                </div>
                """
        
        return content_html
    
    def _generate_category_posts_html(self, category, time_filter='weekly'):
        """Generate HTML for posts in a specific category"""
        df = self.datasets[category][time_filter]
        if df.empty:
            return f"<div class='category-section'><h2>No {time_filter} {category.replace('_', ' ')} data available</h2><p>Please run: <code>python services/generate_all_data.py</code></p></div>"
        
        # Use existing method logic but for any category
        if category == 'finance':
            self.current_category = 'finance'
            return self._generate_posts_html(time_filter)
        elif category == 'entertainment':
            self.current_category = 'entertainment'
            return self._generate_entertainment_posts_html(time_filter)
        else:
            # For travel categories, use the same structure as finance
            self.current_category = 'travel'
            return self._generate_travel_posts_html(category, time_filter)
    
    def _generate_travel_posts_html(self, category, time_filter='weekly'):
        """Generate simple HTML for travel category posts - 6 flat regional categories"""
        df = self.datasets[category][time_filter]
        if df.empty:
            return f"<div class='category-section'><h2>No {time_filter} {category.replace('_', ' ')} data available</h2><p>Please run: <code>python services/generate_all_data.py</code></p></div>"
        
        # Add travel cities widget
        posts_html = f"""
            <div id="travelCitiesWidget-{time_filter}">
                {self._generate_travel_cities_widget(time_filter)}
            </div>
        """
        
        # Travel category priority - expanded regional structure
        category_priority = [
            # Travel Tips
            'Travel Advice',
            # Regional Travel
            'Asian Travel', 'European Travel', 'North America Travel', 'South America Travel', 'Oceania Africa Travel'
        ]
        
        # Determine the correct column name for categories
        if 'category' in df.columns:
            category_column = 'category'
        elif 'travel_subcategory' in df.columns:
            category_column = 'travel_subcategory'  
        elif 'regional_subcategory' in df.columns:
            category_column = 'regional_subcategory'
        else:
            # Fallback: create a single category
            df['category'] = category.replace('_', ' ').title()
            category_column = 'category'
            
        # Order categories by post count (most to least) to match sidebar order
        category_counts = df[category_column].value_counts()
        ordered_categories = list(category_counts.index)
        
        for category_name in ordered_categories:
            category_posts = df[df[category_column] == category_name].sort_values('popularity_score', ascending=False)
            safe_category = category_name.replace(' ', '_').replace('&', 'and').lower()
            
            posts_html += f'<div class="category-section" id="category-{safe_category}-{time_filter}">\n'
            posts_html += f'<div class="category-header-row">\n'
            posts_html += f'<h2 class="category-header">{category_name}</h2>\n'
            posts_html += f'<button class="summarize-btn" onclick="summarizeCategory(\'{category_name}\', \'{time_filter}\')" data-category="{category_name}" data-time-filter="{time_filter}">\n'
            posts_html += f'Summarize\n'
            posts_html += f'</button>\n'
            posts_html += f'</div>\n'
            posts_html += f'<div class="summary-container" id="summary-{safe_category}-{time_filter}" style="display: none;">\n'
            posts_html += f'<div class="summary-content"></div>\n'
            posts_html += f'</div>\n'
            
            # Generate all posts but mark them as visible/hidden for pagination
            post_count = 0
            for _, post in category_posts.iterrows():
                post_count += 1
                # First 10 posts are visible, rest are hidden
                visibility_class = 'post-visible' if post_count <= 10 else 'post-hidden'
                posts_html += self._generate_post_card(post, safe_category, visibility_class)
            
            # Add pagination buttons if there are more than 10 posts
            if len(category_posts) > 10:
                posts_html += f'<div class="pagination-container" id="pagination-{safe_category}-{time_filter}">\n'
                posts_html += f'<button class="show-more-btn" onclick="showMorePosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" data-shown="10" data-total="{len(category_posts)}">Show More</button>\n'
                posts_html += f'<button class="show-less-btn" onclick="showLessPosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" style="display: none;">Show Less</button>\n'
                posts_html += f'</div>\n'
            
            posts_html += '</div>\n'
        
        return posts_html
    
    def _generate_stats_data(self, category_stats):
        """Generate JavaScript statsData object for all categories dynamically"""
        # Map category names to JavaScript-friendly keys
        category_js_map = {
            'finance': 'finance',
            'entertainment': 'movies_shows',  # Keep legacy name for JS compatibility
            'travel': 'travel'  # Unified travel category
        }
        
        stats_js = []
        for category, stats in category_stats.items():
            if category in category_js_map:
                js_key = category_js_map[category]
                weekly_posts = stats.get('weekly', {}).get('total_posts', 0)
                weekly_upvotes = stats.get('weekly', {}).get('total_upvotes', 0)
                daily_posts = stats.get('daily', {}).get('total_posts', 0)
                daily_upvotes = stats.get('daily', {}).get('total_upvotes', 0)
                
                category_js = f"""
            {js_key}: {{
                weekly: {{
                    posts: {weekly_posts},
                    upvotes: {weekly_upvotes}
                }},
                daily: {{
                    posts: {daily_posts},
                    upvotes: {daily_upvotes}
                }}
            }}"""
                stats_js.append(category_js)
        
        return ','.join(stats_js)
    
    def _generate_stock_sentiment_widget(self, time_filter='weekly'):
        """Generate stock sentiment tracker widget for finance posts"""
        # Get finance data for the specified time filter
        finance_data = self.datasets.get('finance', {}).get(time_filter, pd.DataFrame())
        
        if finance_data.empty:
            return """
            <div class="stock-sentiment-card">
                <div class="card-header">
                    <h3>📈 Stock Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">No finance data available for sentiment analysis.</p>
                </div>
            </div>
            """
        
        # Check if sentiment data exists
        if 'stock_tickers' not in finance_data.columns:
            return """
            <div class="stock-sentiment-card">
                <div class="card-header">
                    <h3>📈 Stock Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">Sentiment analysis not yet available. Run data pipeline to generate sentiment data.</p>
                </div>
            </div>
            """
        
        # Aggregate stock sentiment
        stock_sentiment = self.sentiment_analyzer.aggregate_stock_sentiment(finance_data)
        
        if not stock_sentiment:
            return """
            <div class="stock-sentiment-card">
                <div class="card-header">
                    <h3>📈 Stock Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">No stock tickers found in recent posts.</p>
                </div>
            </div>
            """
        
        # Generate HTML for top 15 stocks with carousel (5 pages × 3 items each)
        stock_items = ""
        for i, stock in enumerate(stock_sentiment[:15]):  # Top 15 stocks
            sentiment_color = self._get_sentiment_color(stock['avg_sentiment'])
            sentiment_emoji = self._get_sentiment_emoji(stock['avg_sentiment'])
            
            stock_items += f"""
                <div class="stock-item" data-index="{i}">
                    <div class="stock-header">
                        <span class="stock-ticker">${stock['ticker']}</span>
                        <span class="sentiment-score" style="color: {sentiment_color}">
                            {sentiment_emoji} {stock['avg_sentiment']:+.3f}
                        </span>
                    </div>
                    <div class="stock-details">
                        <span class="post-count">{stock['post_count']} posts</span>
                        <span class="sentiment-label {stock['sentiment_label']}">{stock['sentiment_label'].title()}</span>
                    </div>
                </div>
            """
        
        # Generate pagination dots - three items per page
        pagination_dots = ""
        total_pages = (len(stock_sentiment[:15]) + 2) // 3  # Show 3 items per page
        for i in range(total_pages):
            active_class = "active" if i == 0 else ""
            pagination_dots += f'<span class="pagination-dot {active_class}" data-page="{i}"></span>'
        
        return f"""
        <div class="stock-sentiment-card">
            <div class="card-header">
                <h3>📈 Stock Sentiment Tracker</h3>
                <p class="card-subtitle">Community sentiment on mentioned stocks ({time_filter})</p>
            </div>
            <div class="card-content">
                <div class="stock-carousel-container">
                    <button class="carousel-nav prev" onclick="moveStockCarousel(-1)" aria-label="Previous stocks">‹</button>
                    <div class="stock-sentiment-carousel">
                        <div class="stock-carousel-track" id="stockCarouselTrack">
                            {stock_items}
                        </div>
                    </div>
                    <button class="carousel-nav next" onclick="moveStockCarousel(1)" aria-label="Next stocks">›</button>
                </div>
                <div class="carousel-pagination">
                    {pagination_dots}
                </div>
            </div>
        </div>
        """
    
    def _generate_entertainment_sentiment_widget(self, time_filter='weekly'):
        """Generate entertainment sentiment tracker widget for entertainment posts"""
        # Get entertainment data for the specified time filter
        entertainment_data = self.datasets.get('entertainment', {}).get(time_filter, pd.DataFrame())
        
        if entertainment_data.empty:
            return """
            <div class="entertainment-sentiment-card">
                <div class="card-header">
                    <h3>🎬 Entertainment Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">No entertainment data available for sentiment analysis.</p>
                </div>
            </div>
            """
        
        # Check if sentiment data exists
        if 'entertainment_titles' not in entertainment_data.columns:
            return """
            <div class="entertainment-sentiment-card">
                <div class="card-header">
                    <h3>🎬 Entertainment Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">Sentiment analysis not yet available. Run data pipeline to generate sentiment data.</p>
                </div>
            </div>
            """
        
        # Get balanced entertainment sentiment (5 movies, 5 TV shows, 5 anime)
        title_sentiment = self.entertainment_sentiment_analyzer.get_balanced_sentiment_display(entertainment_data, items_per_category=5)
        
        if not title_sentiment:
            return """
            <div class="entertainment-sentiment-card">
                <div class="card-header">
                    <h3>🎬 Entertainment Sentiment Tracker</h3>
                </div>
                <div class="card-content">
                    <p class="no-data">No entertainment titles found in recent posts.</p>
                </div>
            </div>
            """
        
        # Generate HTML for balanced 15 entertainment titles with carousel (5 pages × 3 items each)
        title_items = ""
        for i, title_data in enumerate(title_sentiment[:15]):  # Balanced 15 titles
            sentiment_color = self._get_sentiment_color(title_data['avg_sentiment'])
            sentiment_emoji = self._get_sentiment_emoji(title_data['avg_sentiment'])
            
            # Category emoji mapping
            category_emoji = {
                'movie': '🎬',
                'tv_show': '📺', 
                'anime': '🎌'
            }
            category_display = category_emoji.get(title_data.get('category', 'movie'), '🎬')
            
            title_items += f"""
                <div class="entertainment-item" data-index="{i}">
                    <div class="entertainment-header">
                        <span class="entertainment-title">{category_display} {title_data['title']}</span>
                        <span class="sentiment-score" style="color: {sentiment_color}">
                            {sentiment_emoji} {title_data['avg_sentiment']:+.3f}
                        </span>
                    </div>
                    <div class="entertainment-details">
                        <span class="post-count">{title_data['post_count']} posts</span>
                        <span class="sentiment-label {title_data['sentiment_label']}">{title_data['sentiment_label'].title()}</span>
                    </div>
                </div>
            """
        
        # Generate pagination dots - three items per page
        pagination_dots = ""
        total_pages = (len(title_sentiment[:15]) + 2) // 3  # Show 3 items per page
        for i in range(total_pages):
            active_class = "active" if i == 0 else ""
            pagination_dots += f'<span class="entertainment-pagination-dot {active_class}" data-page="{i}"></span>'
        
        return f"""
        <div class="entertainment-sentiment-card">
            <div class="card-header">
                <h3>🎬 Entertainment Sentiment Tracker</h3>
                <p class="card-subtitle">Balanced sentiment tracking: 5 movies 🎬, 5 TV shows 📺, 5 anime 🎌 ({time_filter})</p>
            </div>
            <div class="card-content">
                <div class="entertainment-carousel-container">
                    <button class="carousel-nav prev" onclick="moveEntertainmentCarousel(-1)" aria-label="Previous titles">‹</button>
                    <div class="entertainment-sentiment-carousel">
                        <div class="entertainment-carousel-track" id="entertainmentCarouselTrack">
                            {title_items}
                        </div>
                    </div>
                    <button class="carousel-nav next" onclick="moveEntertainmentCarousel(1)" aria-label="Next titles">›</button>
                </div>
                <div class="carousel-pagination">
                    {pagination_dots}
                </div>
            </div>
        </div>
        """
    
    def _generate_travel_cities_widget(self, time_filter='weekly'):
        """Generate travel cities to visit widget based on advice post mentions"""
        
        # Always use weekly data for better city recommendations (daily has insufficient data)
        travel_data = None
        if 'travel' in self.datasets and 'weekly' in self.datasets['travel']:
            travel_data = self.datasets['travel']['weekly']
        
        if travel_data is None or travel_data.empty:
            return f"""
            <div class="travel-sentiment-card">
                <h3 class="card-title">🏙️ Cities to Visit</h3>
                <div class="card-content">
                    <p class="no-data">No travel data available.</p>
                </div>
            </div>
            """
        
        # Get top mentioned cities from travel advice posts
        top_cities = self.travel_city_tracker.get_top_cities_display(travel_data, top_n=15)
        
        if not top_cities:
            return f"""
            <div class="travel-sentiment-card">
                <h3 class="card-title">🏙️ Cities to Visit</h3>
                <div class="card-content">
                    <p class="no-data">No cities mentioned in travel advice posts.</p>
                </div>
            </div>
            """
        
        # Generate carousel with city mentions (3 cities per page) - same format as entertainment
        cities_per_page = 3
        total_pages = (len(top_cities) + cities_per_page - 1) // cities_per_page
        
        carousel_items = ""
        for page in range(total_pages):
            start_idx = page * cities_per_page
            end_idx = min(start_idx + cities_per_page, len(top_cities))
            page_cities = top_cities[start_idx:end_idx]
            
            carousel_items += f"""
            <div class="travel-carousel-track" data-page="{page + 1}" style="{'display: flex;' if page == 0 else 'display: none;'}">
            """
            
            for city_data in page_cities:
                carousel_items += f"""
                <div class="travel-item">
                    <div class="travel-header">
                        <div class="travel-city">{city_data['emoji']} {city_data['city']}</div>
                    </div>
                    <div class="travel-details">
                        <span class="mention-count">💬 {city_data['mentions']} mentions</span>
                    </div>
                </div>
                """
            
            carousel_items += "</div>"
        
        # Generate single carousel track with all items (like stock/entertainment)
        travel_items = ""
        for city_data in top_cities:
            travel_items += f"""
                <div class="travel-item">
                    <div class="travel-header">
                        <div class="travel-city">{city_data['emoji']} {city_data['city']}</div>
                    </div>
                    <div class="travel-details">
                        <span class="mention-count">💬 {city_data['mentions']} mentions</span>
                    </div>
                </div>
                """
        
        # Generate pagination dots
        pagination_dots = ""
        for i in range(total_pages):
            active_class = "active" if i == 0 else ""
            pagination_dots += f'<div class="travel-pagination-dot {active_class}" onclick="goToTravelPage({i})"></div>'

        return f"""
        <div class="travel-sentiment-card">
            <div class="card-header">
                <h3>🏙️ Cities to Visit</h3>
                <p class="card-subtitle">Most mentioned cities in unbiased travel advice posts (weekly)</p>
            </div>
            <div class="card-content">
                <div class="travel-carousel-container">
                    <button class="carousel-nav prev" onclick="moveTravelCarousel(-1)" aria-label="Previous cities">‹</button>
                    <div class="travel-sentiment-carousel">
                        <div class="travel-carousel-track" id="travelCarouselTrack">
                            {travel_items}
                        </div>
                    </div>
                    <button class="carousel-nav next" onclick="moveTravelCarousel(1)" aria-label="Next cities">›</button>
                </div>
                <div class="carousel-pagination">
                    {pagination_dots}
                </div>
            </div>
        </div>
        """
    
    def _get_sentiment_color(self, sentiment_score):
        """Get color based on sentiment score"""
        if sentiment_score >= 0.1:
            return '#22c55e'  # Green for positive
        elif sentiment_score <= -0.1:
            return '#ef4444'  # Red for negative
        else:
            return '#6b7280'  # Gray for neutral
    
    def _get_sentiment_emoji(self, sentiment_score):
        """Get emoji based on sentiment score"""
        if sentiment_score >= 0.3:
            return '🚀'
        elif sentiment_score >= 0.1:
            return '📈'
        elif sentiment_score <= -0.3:
            return '📉'
        elif sentiment_score <= -0.1:
            return '⚠️'
        else:
            return '➖'
        
    def generate_dashboard(self, output_file='assets/reddit_dashboard.html'):
        """Generate a unified dashboard with daily/weekly toggle"""
        
        # Calculate stats for all available categories
        category_stats = {}
        for category, data in self.datasets.items():
            category_stats[category] = {
                'weekly': {
                    'total_posts': len(data['weekly']) if not data['weekly'].empty else 0,
                    'total_upvotes': data['weekly']['score'].sum() if not data['weekly'].empty else 0,
                },
                'daily': {
                    'total_posts': len(data['daily']) if not data['daily'].empty else 0,
                    'total_upvotes': data['daily']['score'].sum() if not data['daily'].empty else 0,
                }
            }
        
        # Legacy stats for backwards compatibility
        weekly_stats = category_stats.get('finance', {}).get('weekly', {'total_posts': 0, 'total_upvotes': 0})
        daily_stats = category_stats.get('finance', {}).get('daily', {'total_posts': 0, 'total_upvotes': 0})
        weekly_entertainment_stats = category_stats.get('entertainment', {}).get('weekly', {'total_posts': 0, 'total_upvotes': 0})
        daily_entertainment_stats = category_stats.get('entertainment', {}).get('daily', {'total_posts': 0, 'total_upvotes': 0})
        
        # Generate HTML
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Insights Dashboard</title>
    <style>
        * {{ 
            margin: 0; 
            padding: 0; 
            box-sizing: border-box; 
        }}
        
        html, body {{
            overflow-x: hidden;
            max-width: 100vw;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f7f9fc;
            color: #1a202c;
            line-height: 1.6;
        }}
        
        .dashboard {{
            display: flex;
            min-height: 100vh;
            max-width: 100vw;
            overflow-x: hidden;
        }}
        
        .sidebar {{
            width: 285px;
            background: #2a2a2a;
            color: white;
            padding: 0;
            position: fixed;
            height: 100vh;
            overflow-y: auto;
        }}
        
        .sidebar-header {{
            padding: 22px 24px;
            border-bottom: 1px solid #4a5568;
            position: relative;
        }}
        
        .sidebar-title {{
            font-size: 1.5rem;
            font-weight: 700;
            margin-bottom: 4px;
            margin-left: 12px;
        }}
        
        .category-dropdown {{
            background: #2a2a2a;
            color: white;
            border: 1px solid #4a5568;
            border-radius: 8px;
            padding: 8px 12px;
            font-size: 1.3rem;
            font-weight: 700;
            cursor: pointer;
            outline: none;
            width: calc(100% - 12px);
            margin-left: -15px;
            appearance: none;
            background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236B7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e");
            background-position: right 8px center;
            background-repeat: no-repeat;
            background-size: 16px;
            padding-right: 40px;
        }}
        
        .category-dropdown:hover {{
            border-color: #ff6b35;
        }}
        
        .category-dropdown:focus {{
            border-color: #ff6b35;
            box-shadow: 0 0 0 2px rgba(255, 107, 53, 0.2);
        }}
        
        .category-dropdown option {{
            background: #2a2a2a;
            color: white;
            font-size: 0.75rem;
            padding: 8px 12px;
        }}
        
        .sidebar-subtitle {{
            font-size: 0.875rem;
            color: #a0aec0;
            margin-left: 12px;
        }}
        
        .sidebar-logo {{
            width: 80px;
            height: 80px;
            object-fit: contain;
            position: absolute;
            top: 15px;
            right: 24px;
        }}
        
        .sidebar-section {{
            padding: 12px 24px;
        }}
        
        .sidebar-section-title {{
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: #a0aec0;
            margin-bottom: 12px;
        }}
        
        .main-content {{
            flex: 1;
            margin-left: 285px;
            background: #f7f9fc;
            width: calc(100vw - 285px);
            max-width: calc(100vw - 285px);
            overflow-x: hidden;
        }}
        
        .top-header {{
            background: white;
            padding: 20px 32px;
            border-bottom: 1px solid #e2e8f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            top: 0;
            z-index: 100;
        }}
        
        .header-title {{
            font-size: 1.875rem;
            font-weight: 700;
            color: #1a202c;
        }}
        
        .header-stats {{
            display: flex;
            gap: 24px;
        }}
        
        .header-stat {{
            text-align: right;
        }}
        
        .header-stat-number {{
            font-size: 1.5rem;
            font-weight: 700;
            color: #2d3748;
        }}
        
        .header-stat-label {{
            font-size: 0.875rem;
            color: #718096;
        }}
        
        .content-area {{
            padding: 32px;
            max-width: 100%;
            box-sizing: border-box;
        }}
        
        .search-card {{
            background: white;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 24px;
        }}
        
        .stock-sentiment-card, .entertainment-sentiment-card, .travel-sentiment-card {{
            background: white;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 24px;
        }}
        
        .stock-sentiment-card {{
            border-left: 4px solid #ff6b35;
        }}
        
        .entertainment-sentiment-card {{
            border-left: 4px solid #8b5cf6;
        }}
        
        .travel-sentiment-card {{
            border-left: 4px solid #10b981;
        }}
        
        .card-header {{
            margin-bottom: 20px;
        }}
        
        .card-header h3 {{
            margin: 0;
            font-size: 1.25rem;
            font-weight: 700;
            color: #1a202c;
        }}
        
        .card-subtitle {{
            margin: 4px 0 0 0;
            font-size: 0.875rem;
            color: #6b7280;
        }}
        
        .stock-sentiment-list {{
            display: grid;
            gap: 12px;
        }}
        
        .stock-item {{
            padding: 8px 10px;
            background: #f7fafc;
            border-radius: 6px;
            border: 1px solid #e2e8f0;
        }}
        
        .stock-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }}
        
        .stock-ticker {{
            font-weight: 700;
            font-size: 0.875rem;
            color: #1a202c;
        }}
        
        .sentiment-score {{
            font-weight: 600;
            font-size: 0.75rem;
        }}
        
        .stock-details {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.6875rem;
        }}
        
        .post-count {{
            color: #6b7280;
            font-size: 0.625rem;
        }}
        
        .sentiment-label {{
            padding: 1px 6px;
            border-radius: 8px;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.5625rem;
        }}
        
        .sentiment-label.positive {{
            background: #dcfce7;
            color: #166534;
        }}
        
        .sentiment-label.negative {{
            background: #fee2e2;
            color: #991b1b;
        }}
        
        .sentiment-label.neutral {{
            background: #f3f4f6;
            color: #4b5563;
        }}
        
        .no-data {{
            color: #6b7280;
            font-style: italic;
            text-align: center;
            padding: 20px;
        }}
        
        /* Stock Carousel Styles */
        .stock-carousel-container {{
            position: relative;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .stock-sentiment-carousel {{
            flex: 1;
            overflow: hidden;
            border-radius: 8px;
            min-width: 0;
        }}
        
        .stock-carousel-track {{
            display: flex;
            transition: transform 0.3s ease;
            gap: 8px;
        }}
        
        .stock-carousel-track .stock-item, .entertainment-carousel-track .entertainment-item, .travel-carousel-track .travel-item {{
            flex: 0 0 calc(33.333% - 6px);
            min-width: 0;
            width: calc(33.333% - 6px);
        }}
        
        .entertainment-item {{
            padding: 8px 10px;
            background: #f7fafc;
            border-radius: 6px;
            border: 1px solid #e2e8f0;
        }}
        
        .entertainment-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }}
        
        .entertainment-title {{
            font-weight: 700;
            font-size: 0.875rem;
            color: #1a202c;
        }}
        
        .entertainment-details {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.6875rem;
        }}
        
        /* Travel Carousel Styles */
        .travel-item {{
            padding: 8px 10px;
            background: #f7fafc;
            border-radius: 6px;
            border: 1px solid #e2e8f0;
        }}
        
        .travel-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }}
        
        .travel-city {{
            font-weight: 700;
            font-size: 0.875rem;
            color: #1a202c;
        }}
        
        .travel-details {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            font-size: 0.6875rem;
        }}
        
        .mention-count {{
            color: #10b981;
            font-weight: 600;
        }}
        
        .travel-carousel-container {{
            position: relative;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .travel-pagination-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #cbd5e0;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .travel-pagination-dot.active {{
            background: #ff6b35;
            transform: scale(1.3);
        }}
        
        .travel-pagination-dot:hover {{
            background: #a0aec0;
        }}
        
        /* Entertainment Carousel Styles */
        .entertainment-carousel-container {{
            position: relative;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .entertainment-sentiment-carousel, .travel-sentiment-carousel {{
            flex: 1;
            overflow: hidden;
            border-radius: 8px;
            min-width: 0;
        }}
        
        .entertainment-carousel-track, .travel-carousel-track {{
            display: flex;
            transition: transform 0.3s ease;
            gap: 8px;
        }}
        
        .entertainment-pagination-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #cbd5e0;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .entertainment-pagination-dot.active {{
            background: #8b5cf6;
            transform: scale(1.3);
        }}
        
        .entertainment-pagination-dot:hover {{
            background: #a0aec0;
        }}
        
        .carousel-nav {{
            background: #ff6b35;
            color: white;
            border: none;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            font-size: 1rem;
            font-weight: bold;
            transition: all 0.2s;
            z-index: 2;
            flex-shrink: 0;
        }}
        
        .carousel-nav:hover {{
            background: #e55a2b;
            transform: scale(1.1);
        }}
        
        .carousel-nav:disabled {{
            background: #cbd5e0;
            cursor: not-allowed;
            transform: none;
        }}
        
        .carousel-pagination {{
            display: flex;
            justify-content: center;
            gap: 8px;
            margin-top: 16px;
        }}
        
        .pagination-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #cbd5e0;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .pagination-dot.active {{
            background: #ff6b35;
            transform: scale(1.3);
        }}
        
        .pagination-dot:hover {{
            background: #a0aec0;
        }}
        
        @media (max-width: 768px) {{
            .carousel-nav {{
                width: 35px;
                height: 35px;
                font-size: 1rem;
            }}
        }}
        
        .search-container {{
            display: flex;
            gap: 12px;
            margin-bottom: 8px;
        }}
        
        .search-input {{
            flex: 1;
            padding: 12px 16px;
            border: 1px solid #e2e8f0;
            border-radius: 24px;
            font-size: 0.875rem;
            font-weight: 500;
            background: #f7fafc;
            transition: all 0.2s;
        }}
        
        .search-input:focus {{
            outline: none;
            border-color: #ff6b35;
            background: white;
            box-shadow: 0 0 0 3px rgba(255, 107, 53, 0.1);
        }}
        
        .search-btn, .clear-btn {{
            padding: 12px 20px;
            border: none;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.875rem;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .search-btn {{
            background: #ff6b35;
            color: white;
        }}
        
        .search-btn:hover {{
            background: #e55a2b;
            transform: translateY(-1px);
        }}
        
        .clear-btn {{
            background: #edf2f7;
            color: #4a5568;
        }}
        
        .clear-btn:hover {{
            background: #e2e8f0;
        }}
        
        .search-results {{
            font-size: 0.875rem;
            color: #718096;
            min-height: 20px;
            font-weight: 500;
        }}
        
        .controls-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 20px;
        }}
        
        .time-filter-section {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        
        .time-filter-label {{
            font-weight: 600;
            color: #4a5568;
            font-size: 0.875rem;
        }}
        
        .time-filter-dropdown {{
            padding: 8px 16px;
            border: 1px solid #e2e8f0;
            background: white;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .time-filter-dropdown:focus {{
            outline: none;
            border-color: #ff6b35;
            box-shadow: 0 0 0 3px rgba(255, 107, 53, 0.1);
        }}
        
        .sidebar .filter-tabs {{
            display: flex;
            flex-direction: column;
            gap: 2px;
        }}
        
        .sidebar .tab-btn {{
            padding: 8px 12px;
            border: none;
            background: transparent;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            font-size: 0.875rem;
            font-weight: 500;
            color: #a0aec0;
            text-align: left;
            white-space: nowrap;
        }}
        
        .sidebar .tab-btn:hover {{
            background: #4a5568;
            color: white;
        }}
        
        .sidebar .tab-btn.active {{
            background: #ff6b35;
            color: white;
        }}
        
        .sidebar .sort-controls select {{
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #4a5568;
            border-radius: 6px;
            background: #4a5568;
            color: white;
            font-size: 0.875rem;
        }}
        
        .sidebar .time-filter-dropdown {{
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #4a5568;
            border-radius: 6px;
            background: #4a5568;
            color: white;
            font-size: 0.875rem;
        }}
        
        .time-content {{
            display: none;
        }}
        
        .time-content.active {{
            display: block;
        }}
        
        .category-content {{
            display: none;
        }}
        
        .category-content.active {{
            display: block;
        }}
        
        .post-hidden {{
            display: none !important;
        }}
        
        .post-visible {{
            display: block;
        }}
        
        .post-search-match {{
            display: block !important;
        }}
        
        .category-section {{
            margin-bottom: 32px;
        }}
        
        .category-header-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            padding: 20px 24px;
            background: white;
            border-radius: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }}
        
        .category-header {{
            color: #1a202c;
            font-size: 1.25rem;
            font-weight: 700;
            margin: 0;
        }}
        
        .summarize-btn {{
            background: #4a4a4a;
            color: white;
            border: none;
            padding: 10px 16px;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        
        .summarize-btn:hover {{
            background: #5a5a5a;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(74, 74, 74, 0.4);
        }}
        
        .summarize-btn:disabled {{
            background: #a0aec0;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }}
        
        .summarize-icon {{
            font-size: 1rem;
        }}
        
        .summary-container {{
            background: #f0f8ff;
            border: 1px solid #90cdf4;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }}
        
        .summary-content {{
            line-height: 1.7;
            color: #2d3748;
            font-size: 0.9rem;
        }}
        
        .summary-loading {{
            text-align: center;
            color: #718096;
            font-style: italic;
        }}
        
        .summary-error {{
            background: #fed7d7;
            border-color: #fc8181;
            color: #c53030;
        }}
        
        .post-card {{
            background: white;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 16px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            border: 1px solid #f7fafc;
            transition: all 0.2s;
        }}
        
        .post-card:hover {{
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            transform: translateY(-2px);
        }}
        
        .post-title {{
            color: #1a202c;
            margin-bottom: 12px;
            font-size: 1.125rem;
            font-weight: 600;
            line-height: 1.4;
        }}
        
        .post-meta {{
            display: flex;
            gap: 16px;
            align-items: center;
            flex-wrap: wrap;
            margin-bottom: 16px;
        }}
        
        .subreddit-tag {{
            background: #edf2f7;
            color: #4a5568;
            padding: 4px 12px;
            border-radius: 16px;
            font-size: 0.75rem;
            font-weight: 600;
        }}
        
        .stat {{
            font-size: 0.875rem;
            color: #718096;
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 4px;
        }}
        
        .post-actions {{
            display: flex;
            gap: 12px;
        }}
        
        .view-btn {{
            padding: 8px 16px;
            background: #ff6b35;
            color: white;
            text-decoration: none;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            transition: all 0.2s;
        }}
        
        .view-btn:hover {{
            background: #e55a2b;
            transform: translateY(-1px);
        }}
        
        .expand-btn {{
            background: #f7fafc;
            color: #4a5568;
            border: 1px solid #e2e8f0;
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .expand-btn:hover {{
            background: #edf2f7;
            color: #2d3748;
        }}
        
        .post-details {{
            background: #f7fafc;
            padding: 16px;
            border-radius: 8px;
            margin-top: 16px;
            border: 1px solid #e2e8f0;
        }}
        
        .post-details p {{
            margin-bottom: 8px;
            font-size: 0.875rem;
            color: #4a5568;
            line-height: 1.5;
        }}
        
        .post-comments {{
            background: #f0f8ff;
            padding: 16px;
            border-radius: 8px;
            margin-top: 12px;
            border: 1px solid #bee3f8;
        }}
        
        .comments-list {{
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}
        
        .comment-item {{
            background: white;
            padding: 12px;
            border-radius: 6px;
            border-left: 3px solid #4299e1;
        }}
        
        .comment-meta {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
            font-size: 0.75rem;
        }}
        
        .comment-author {{
            font-weight: 600;
            color: #2d3748;
        }}
        
        .comment-score {{
            color: #4299e1;
            font-weight: 500;
        }}
        
        .comment-text {{
            font-size: 0.875rem;
            color: #4a5568;
            line-height: 1.4;
        }}
        
        .pagination-container {{
            text-align: center;
            margin: 24px 0;
            display: flex;
            gap: 12px;
            justify-content: center;
        }}
        
        .show-more-btn, .show-less-btn {{
            background: #ff6b35;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .show-less-btn {{
            background: #718096;
        }}
        
        .show-more-btn:hover {{
            background: #e55a2b;
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(255, 107, 53, 0.4);
        }}
        
        .show-less-btn:hover {{
            background: #4a5568;
            transform: translateY(-1px);
        }}
        
        .search-highlight {{
            background-color: #fef08a;
            padding: 2px 4px;
            border-radius: 4px;
            font-weight: 600;
        }}
        
    </style>
</head>
<body>
    <div class="dashboard">
        <!-- Sidebar -->
        <div class="sidebar">
            <div class="sidebar-header">
                <div class="sidebar-title">
                    <select id="categorySelect" class="category-dropdown" onchange="switchCategory(this.value)">
{self._generate_category_options()}
                    </select>
                </div>
                <img src="finance-logo.png" alt="Logo" class="sidebar-logo" id="sidebarLogo">
                <div class="sidebar-subtitle">Dashboard</div>
            </div>
            
            <div class="sidebar-section">
                <div class="sidebar-section-title">Time Range</div>
                <div class="time-filter-section">
                    <select id="timeFilterSelect" class="time-filter-dropdown" onchange="switchTimeFilter(this.value)">
                        <option value="weekly" selected>Weekly Data</option>
                        <option value="daily">Daily Data</option>
                    </select>
                </div>
            </div>
            
            <div class="sidebar-section">
                <div class="sidebar-section-title">Sort</div>
                <div class="sort-controls">
                    <select id="sortSelect" onchange="sortPosts()">
                        <option value="popularity">Popularity Score</option>
                        <option value="score">Upvotes</option>
                        <option value="comments">Comments</option>
                        <option value="recent">Most Recent</option>
                    </select>
                </div>
            </div>
            
            <div class="sidebar-section">
                <div class="sidebar-section-title">Categories</div>
                <div class="filter-tabs" id="categoryTabs">
                    <button class="tab-btn active" onclick="showCategory('all')" id="allBtn">All Posts</button>
                    {self._generate_category_tabs('weekly')}
                </div>
            </div>
        </div>
        
        <!-- Main Content -->
        <div class="main-content">
            <div class="top-header">
                <div class="header-title">Reddit Insights</div>
                <div class="header-stats">
                    <div class="header-stat">
                        <div class="header-stat-number" id="totalPosts">{weekly_stats['total_posts']:,}</div>
                        <div class="header-stat-label">Posts Analyzed</div>
                    </div>
                    <div class="header-stat">
                        <div class="header-stat-number" id="totalUpvotes">{weekly_stats['total_upvotes']:,}</div>
                        <div class="header-stat-label">Total Upvotes</div>
                    </div>
                </div>
            </div>
            
            <div class="content-area">
                <div class="search-card">
                    <div class="search-container">
                        <input type="text" id="searchInput" placeholder="Search posts..." class="search-input">
                        <button onclick="searchPosts()" class="search-btn">Search</button>
                        <button onclick="clearSearch()" class="clear-btn">Clear</button>
                    </div>
                    <div id="searchResults" class="search-results"></div>
                </div>
                
                <div id="stockSentimentWidget">
                    {self._generate_stock_sentiment_widget('weekly')}
                </div>
                
                {self._generate_all_category_content()}
            </div>
        </div>
    </div>
    
    <script>
        // Data for all categories and time filters
        const statsData = {{
            {self._generate_stats_data(category_stats)}
        }};
        
        // Map category names to stats keys for compatibility
        const categoryStatsMap = {{
            'finance': 'finance',
            'entertainment': 'movies_shows',
            'travel': 'travel'  // Unified travel category
        }};
        
        const categoryData = {{
            finance: {{
                weekly: `{self._generate_category_tabs('weekly')}`,
                daily: `{self._generate_category_tabs('daily')}`
            }},
            movies_shows: {{
                weekly: `{self._generate_entertainment_category_tabs('weekly')}`,
                daily: `{self._generate_entertainment_category_tabs('daily')}`
            }},
            travel: {{
                weekly: `{self._generate_travel_category_tabs('weekly')}`,
                daily: `{self._generate_travel_category_tabs('daily')}`
            }}
        }};
        
        let currentCategory = 'finance';
        
        function switchCategory(category) {{
            currentCategory = category;
            
            // Hide all category content
            document.querySelectorAll('.category-content').forEach(content => {{
                content.classList.remove('active');
            }});
            
            // Show selected category content - dynamic mapping
            const categoryIdMap = {{
                'finance': 'financeCategory',
                'entertainment': 'moviesShowsCategory',  // Keep legacy name
                'travel': 'travelCategory'  // Unified travel category
            }};
            
            const logoMap = {{
                'finance': {{'src': 'finance-logo.png', 'alt': 'Finance Logo'}},
                'entertainment': {{'src': 'entertainment-logo.png', 'alt': 'Entertainment Logo'}},
                'travel': {{'src': 'travel-logo.png', 'alt': 'Travel Logo'}}
            }};
            
            const categoryId = categoryIdMap[category] || category + 'Category';
            const categoryElement = document.getElementById(categoryId);
            if (categoryElement) {{
                categoryElement.classList.add('active');
            }}
            
            // Update logo
            const logoInfo = logoMap[category] || {{'src': 'finance-logo.png', 'alt': 'Logo'}};
            const logoElement = document.getElementById('sidebarLogo');
            if (logoElement) {{
                logoElement.src = logoInfo.src;
                logoElement.alt = logoInfo.alt;
            }}
            
            // Update stats based on current category and time filter
            const timeFilter = document.getElementById('timeFilterSelect').value;
            switchTimeFilter(timeFilter);
            
            // Clear search when switching categories
            clearSearch();
        }}
        
        function switchTimeFilter(timeFilter) {{
            // Update dropdown value (in case called programmatically)
            document.getElementById('timeFilterSelect').value = timeFilter;
            
            // Update stats based on current category using mapping
            const statsKey = categoryStatsMap[currentCategory] || currentCategory;
            if (statsData[statsKey] && statsData[statsKey][timeFilter]) {{
                document.getElementById('totalPosts').textContent = statsData[statsKey][timeFilter].posts.toLocaleString();
                document.getElementById('totalUpvotes').textContent = statsData[statsKey][timeFilter].upvotes.toLocaleString();
            }} else {{
                document.getElementById('totalPosts').textContent = '0';
                document.getElementById('totalUpvotes').textContent = '0';
            }}
            
            // Update content visibility within current category
            const activeCategory = document.querySelector('.category-content.active');
            if (activeCategory) {{
                activeCategory.querySelectorAll('.time-content').forEach(content => {{
                    content.classList.remove('active');
                    content.style.display = 'none';
                }});
                
                // Show appropriate time content based on category - dynamic
                const timeContentIdMap = {{
                    'finance': timeFilter + 'Content',
                    'entertainment': timeFilter + 'MoviesContent',  // Keep legacy pattern
                    'travel': timeFilter + 'TravelContent'  // Unified travel category
                }};
                
                const timeContentId = timeContentIdMap[currentCategory];
                if (timeContentId) {{
                    const timeContentElement = document.getElementById(timeContentId);
                    if (timeContentElement) {{
                        timeContentElement.classList.add('active');
                        timeContentElement.style.display = 'block';
                    }}
                }}
            }}
            
            // Update category buttons
            const categoryTabs = document.getElementById('categoryTabs');
            const allBtn = document.getElementById('allBtn');
            if (allBtn) {{
                // Remove all buttons except the All Posts button
                const buttonsToRemove = categoryTabs.querySelectorAll('.tab-btn:not(#allBtn)');
                buttonsToRemove.forEach(btn => btn.remove());
                // Add new category buttons based on current category
                const categoryDataMapping = {{
                    'finance': 'finance',
                    'entertainment': 'movies_shows', 
                    'travel': 'travel'
                }};
                const categoryDataKey = categoryDataMapping[currentCategory] || currentCategory;
                if (categoryData[categoryDataKey] && categoryData[categoryDataKey][timeFilter]) {{
                    allBtn.insertAdjacentHTML('afterend', categoryData[categoryDataKey][timeFilter]);
                }}
                
                // Reset category filter to 'all'
                document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
                allBtn.classList.add('active');
            }}
            
            // Update stock sentiment widget
            updateStockSentimentWidget(timeFilter);
            
            // Clear search
            clearSearch();
        }}
        
        function updateStockSentimentWidget(timeFilter) {{
            // This function would need to make an AJAX call to regenerate the widget
            // For now, we'll just hide it for non-weekly/daily views
            const stockWidget = document.getElementById('stockSentimentWidget');
            
            if (stockWidget) {{
                // Show widget for finance data, hide for others
                if (currentCategory === 'finance') {{
                    stockWidget.style.display = 'block';
                    // Reset carousel to first page when switching time filters
                    currentStockPage = 0;
                    if (document.getElementById('stockCarouselTrack')) {{
                        moveStockCarousel(0);
                    }}
                }} else {{
                    stockWidget.style.display = 'none';
                }}
            }}
            
            // Handle entertainment widgets (one for each time filter)
            const entertainmentWidgetWeekly = document.getElementById('entertainmentSentimentWidget-weekly');
            const entertainmentWidgetDaily = document.getElementById('entertainmentSentimentWidget-daily');
            
            // Entertainment widgets are already embedded in their respective time sections
            // They will be shown/hidden automatically when time sections are toggled
            // Just reset carousel when switching time filters for entertainment
            if (currentCategory === 'entertainment') {{
                currentEntertainmentPage = 0;
                const carouselTrack = document.getElementById('entertainmentCarouselTrack');
                if (carouselTrack) {{
                    moveEntertainmentCarousel(0);
                }}
            }}
        }}
        
        function showMorePosts(categoryId) {{
            const showMoreBtn = document.querySelector(`#pagination-${{categoryId}} .show-more-btn`);
            const showLessBtn = document.querySelector(`#pagination-${{categoryId}} .show-less-btn`);
            const shown = parseInt(showMoreBtn.dataset.shown);
            const total = parseInt(showMoreBtn.dataset.total);
            const categorySection = document.getElementById(`category-${{categoryId}}`);
            
            // Show next 10 posts
            const hiddenPosts = categorySection.querySelectorAll('.post-hidden');
            const postsToShow = Math.min(10, hiddenPosts.length);
            
            for (let i = 0; i < postsToShow; i++) {{
                hiddenPosts[i].classList.remove('post-hidden');
                hiddenPosts[i].classList.add('post-visible');
            }}
            
            // Update button states
            const newShown = shown + postsToShow;
            showMoreBtn.dataset.shown = newShown;
            
            // Show "Show Less" button
            showLessBtn.style.display = 'inline-block';
            
            // Hide "Show More" button if all posts are shown
            if (newShown >= total) {{
                showMoreBtn.style.display = 'none';
            }}
        }}
        
        function showLessPosts(categoryId) {{
            const showMoreBtn = document.querySelector(`#pagination-${{categoryId}} .show-more-btn`);
            const showLessBtn = document.querySelector(`#pagination-${{categoryId}} .show-less-btn`);
            const categorySection = document.getElementById(`category-${{categoryId}}`);
            
            // Hide all posts except first 10
            const allPosts = categorySection.querySelectorAll('.post-card');
            allPosts.forEach((post, index) => {{
                if (index < 10) {{
                    post.classList.remove('post-hidden');
                    post.classList.add('post-visible');
                }} else {{
                    post.classList.remove('post-visible');
                    post.classList.add('post-hidden');
                }}
            }});
            
            // Reset button states
            showMoreBtn.dataset.shown = '10';
            showMoreBtn.style.display = 'inline-block';
            showLessBtn.style.display = 'none';
        }}
        
        async function summarizeCategory(category, timeFilter) {{
            // Map display names to data column names for travel categories
            const categoryMapping = {{
                'Travel Advice': 'travel_advice',
                'Asian Travel': 'asian_travel',
                'European Travel': 'european_travel',
                'North America Travel': 'north_america_travel',
                'South America Travel': 'south_america_travel',
                'Oceania Africa Travel': 'oceania_africa_travel'
            }};
            
            // Use mapped category name if it exists, otherwise use original
            const apiCategory = categoryMapping[category] || category;
            
            const button = document.querySelector(`[data-category=\"${{category}}\"][data-time-filter=\"${{timeFilter}}\"]`);
            const summaryContainer = document.getElementById(`summary-${{category.replace(/ /g, '_').replace(/&/g, 'and').toLowerCase()}}-${{timeFilter}}`);
            const summaryContent = summaryContainer.querySelector('.summary-content');
            
            // Disable button and show loading
            button.disabled = true;
            button.innerHTML = 'Summarizing...';
            
            // Show container and display loading message
            summaryContainer.style.display = 'block';
            summaryContainer.classList.remove('summary-error');
            summaryContent.innerHTML = '<div class=\"summary-loading\">AI is analyzing posts in this category... This may take a few seconds.</div>';
            
            try {{
                const response = await fetch('http://localhost:5002/summarize', {{
                    method: 'POST',
                    headers: {{
                        'Content-Type': 'application/json',
                    }},
                    body: JSON.stringify({{
                        category: apiCategory,
                        time_filter: timeFilter
                    }})
                }});
                
                const result = await response.json();
                
                if (result.success) {{
                    // Display successful summary
                    summaryContent.innerHTML = `
                        <div style=\"margin-bottom: 10px;\">
                            <strong>AI Summary for ${{category}} (${{result.total_posts}} posts analyzed)</strong>
                        </div>
                        <div style=\"white-space: pre-line;\">${{result.summary}}</div>
                        <div style=\"margin-top: 15px; font-size: 0.85em; color: #6b7280;\">
                            Generated at ${{new Date(result.generated_at).toLocaleString()}}
                        </div>
                    `;
                }} else {{
                    // Display error
                    summaryContainer.classList.add('summary-error');
                    summaryContent.innerHTML = `
                        <div><strong>Summarization Failed</strong></div>
                        <div style=\"margin-top: 8px;\">${{result.error}}</div>
                        <div style=\"margin-top: 10px; font-size: 0.9em;\">
                            Make sure the AI service is running: <code>python services/ai_summarizer.py</code>
                        </div>
                    `;
                }}
                
            }} catch (error) {{
                // Handle network or other errors
                summaryContainer.classList.add('summary-error');
                summaryContent.innerHTML = `
                    <div><strong>Connection Error</strong></div>
                    <div style=\"margin-top: 8px;\">Could not connect to AI service.</div>
                    <div style=\"margin-top: 10px; font-size: 0.9em;\">
                        Please start the AI service: <code>python services/ai_summarizer.py</code>
                    </div>
                `;
            }}
            
            // Re-enable button and change to "Hide Summary"
            button.disabled = false;
            button.innerHTML = 'Hide Summary';
            button.onclick = function() {{ hideSummary(category, timeFilter); }};
        }}
        
        function hideSummary(category, timeFilter) {{
            const button = document.querySelector(`[data-category=\"${{category}}\"][data-time-filter=\"${{timeFilter}}\"]`);
            const summaryContainer = document.getElementById(`summary-${{category.replace(/ /g, '_').replace(/&/g, 'and').toLowerCase()}}-${{timeFilter}}`);
            
            // Hide the summary container
            summaryContainer.style.display = 'none';
            
            // Change button back to "Summarize"
            button.innerHTML = 'Summarize';
            button.onclick = function() {{ summarizeCategory(category, timeFilter); }};
        }}
        
        function searchPosts() {{
            const searchInput = document.getElementById('searchInput').value.trim();
            const resultsDiv = document.getElementById('searchResults');
            
            if (!searchInput) {{
                resultsDiv.textContent = 'Please enter a search term';
                return;
            }}
            
            const searchTerms = searchInput.toLowerCase().split(/\\s+/).filter(term => term.length > 0);
            // Only search within the currently active category and time filter content
            const activeCategory = document.querySelector('.category-content.active');
            const activeContent = activeCategory ? activeCategory.querySelector('.time-content.active') : null;
            const allPosts = activeContent ? activeContent.querySelectorAll('.post-card') : [];
            let matchCount = 0;
            
            // Show all category sections first
            document.querySelectorAll('.category-section').forEach(section => {{
                section.style.display = 'block';
            }});
            
            // Hide all pagination buttons during search
            document.querySelectorAll('.pagination-container').forEach(container => {{
                container.style.display = 'none';
            }});
            
            // Track which categories have matches
            const categoriesWithMatches = new Set();
            
            allPosts.forEach(post => {{
                const titleData = post.getAttribute('data-search-title') || '';
                const contentData = post.getAttribute('data-search-content') || '';
                const combinedText = titleData + ' ' + contentData;
                
                const matchesAllTerms = searchTerms.every(term => combinedText.includes(term));
                
                if (matchesAllTerms) {{
                    // Force show matching posts regardless of pagination state
                    post.classList.remove('post-hidden');
                    post.classList.add('post-visible');
                    post.classList.add('post-search-match');
                    matchCount++;
                    
                    // Track which category this post belongs to
                    const categorySection = post.closest('.category-section');
                    if (categorySection) {{
                        categoriesWithMatches.add(categorySection);
                    }}
                }} else {{
                    // Hide non-matching posts
                    post.classList.remove('post-search-match');
                    post.style.display = 'none';
                }}
            }});
            
            // Show/hide category headers based on whether they have matches
            document.querySelectorAll('.category-section').forEach(section => {{
                const header = section.querySelector('.category-header-row');
                const summarizeBtn = section.querySelector('.summarize-btn');
                
                if (categoriesWithMatches.has(section)) {{
                    // Show header for categories with matches but hide summarize button
                    header.style.display = 'flex';
                    if (summarizeBtn) {{
                        summarizeBtn.style.display = 'none';
                    }}
                }} else {{
                    // Hide header for categories without matches
                    header.style.display = 'none';
                }}
            }});
            
            if (matchCount === 0) {{
                resultsDiv.textContent = `No posts found for "${{searchInput}}"`;
            }} else {{
                const termText = searchTerms.length > 1 ? `all terms: ${{searchTerms.join(', ')}}` : `"${{searchInput}}"`;
                resultsDiv.textContent = `Found ${{matchCount}} post${{matchCount === 1 ? '' : 's'}} matching ${{termText}}`;
            }}
            
            document.querySelectorAll('.tab-btn').forEach(btn => btn.style.opacity = '0.5');
        }}
        
        function clearSearch() {{
            document.getElementById('searchInput').value = '';
            document.getElementById('searchResults').textContent = '';
            
            // Restore category headers and summarize buttons
            document.querySelectorAll('.category-header-row').forEach(header => {{
                header.style.display = 'flex';
            }});
            
            // Restore summarize buttons
            document.querySelectorAll('.summarize-btn').forEach(btn => {{
                btn.style.display = 'inline-flex';
            }});
            
            // Restore pagination buttons
            document.querySelectorAll('.pagination-container').forEach(container => {{
                container.style.display = 'flex';
            }});
            
            // Reset all posts to their original pagination state
            document.querySelectorAll('.post-card').forEach((post, index) => {{
                const categorySection = post.closest('.category-section');
                const postsInCategory = Array.from(categorySection.querySelectorAll('.post-card'));
                const postIndex = postsInCategory.indexOf(post);
                
                // Remove search match class
                post.classList.remove('post-search-match');
                
                // Show first 10 posts in each category, hide the rest
                if (postIndex < 10) {{
                    post.classList.remove('post-hidden');
                    post.classList.add('post-visible');
                    post.style.display = 'block';
                }} else {{
                    post.classList.remove('post-visible');
                    post.classList.add('post-hidden');
                    post.style.display = 'none';
                }}
            }});
            
            // Reset all Show More/Show Less buttons to original state
            document.querySelectorAll('.show-more-btn').forEach(btn => {{
                btn.dataset.shown = '10';
                btn.style.display = 'inline-block';
            }});
            document.querySelectorAll('.show-less-btn').forEach(btn => {{
                btn.style.display = 'none';
            }});
            
            document.querySelectorAll('.tab-btn').forEach(btn => btn.style.opacity = '1');
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.querySelector('.tab-btn[onclick*="all"]').classList.add('active');
        }}
        
        function showCategory(category) {{
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            // Handle category sections (headers + posts)
            document.querySelectorAll('.category-section').forEach(section => {{
                if (category === 'all') {{
                    // Show all sections when "All Posts" is selected
                    section.style.display = 'block';
                }} else {{
                    // Check if this section contains posts of the selected category
                    const categoryPosts = section.querySelectorAll(`[data-category="${{category}}"]`);
                    if (categoryPosts.length > 0) {{
                        section.style.display = 'block';
                    }} else {{
                        section.style.display = 'none';
                    }}
                }}
            }});
            
            // Handle individual post cards within visible sections
            document.querySelectorAll('.post-card').forEach(card => {{
                if (category === 'all' || card.dataset.category === category) {{
                    card.style.display = 'block';
                }} else {{
                    card.style.display = 'none';
                }}
            }});
        }}
        
        function sortPosts() {{
            const sortBy = document.getElementById('sortSelect').value;
            
            // Get the currently active category and time filter content
            const activeCategory = document.querySelector('.category-content.active');
            const activeContent = activeCategory ? activeCategory.querySelector('.time-content.active') : null;
            if (!activeContent) return;
            
            // Get all category sections within the active content
            const categorySections = Array.from(activeContent.querySelectorAll('.category-section'));
            
            categorySections.forEach(categorySection => {{
                // Get all post cards within this category section
                const posts = Array.from(categorySection.querySelectorAll('.post-card'));
                
                if (posts.length === 0) return;
                
                // Sort the posts
                posts.sort((a, b) => {{
                    let aVal, bVal;
                    
                    switch(sortBy) {{
                        case 'popularity':
                            aVal = parseFloat(a.dataset.popularity);
                            bVal = parseFloat(b.dataset.popularity);
                            break;
                        case 'score':
                            aVal = parseInt(a.dataset.score);
                            bVal = parseInt(b.dataset.score);
                            break;
                        case 'comments':
                            aVal = parseInt(a.dataset.comments);
                            bVal = parseInt(b.dataset.comments);
                            break;
                        case 'recent':
                            aVal = new Date(a.dataset.time);
                            bVal = new Date(b.dataset.time);
                            break;
                    }}
                    
                    return bVal - aVal; // Descending order
                }});
                
                // Find the summary container to insert posts after it
                const summaryContainer = categorySection.querySelector('.summary-container');
                const insertPoint = summaryContainer.nextElementSibling || summaryContainer.nextSibling;
                
                // Re-append sorted posts in the correct location
                posts.forEach((post, index) => {{
                    // Reset pagination visibility - first 10 visible, rest hidden
                    if (index < 10) {{
                        post.classList.remove('post-hidden');
                        post.classList.add('post-visible');
                    }} else {{
                        post.classList.remove('post-visible');
                        post.classList.add('post-hidden');
                    }}
                    
                    // Insert after summary container
                    if (insertPoint) {{
                        categorySection.insertBefore(post, insertPoint);
                    }} else {{
                        categorySection.appendChild(post);
                    }}
                }});
                
                // Reset pagination buttons
                const showMoreBtn = categorySection.querySelector('.show-more-btn');
                const showLessBtn = categorySection.querySelector('.show-less-btn');
                if (showMoreBtn) {{
                    showMoreBtn.dataset.shown = '10';
                    showMoreBtn.style.display = posts.length > 10 ? 'inline-block' : 'none';
                }}
                if (showLessBtn) {{
                    showLessBtn.style.display = 'none';
                }}
            }});
        }}
        
        function toggleDetails(button) {{
            const details = button.parentNode.nextElementSibling;
            if (details.style.display === 'none') {{
                details.style.display = 'block';
                button.textContent = 'Hide Details';
            }} else {{
                details.style.display = 'none';
                button.textContent = 'Show Details';
            }}
        }}
        
        
        async function toggleComments(button) {{
            const postCard = button.closest('.post-card');
            const comments = postCard.querySelector('.post-comments');
            const postId = postCard.dataset.postId;
            
            if (comments && comments.style.display === 'none') {{
                // Check if comments are already loaded
                if (comments.innerHTML.trim() === '' || comments.innerHTML.includes('Loading...')) {{
                    // Show loading state
                    comments.innerHTML = '<div class="loading-comments">Loading top comments...</div>';
                    comments.style.display = 'block';
                    button.textContent = 'Loading...';
                    button.disabled = true;
                    
                    try {{
                        // Fetch comments from live API
                        const response = await fetch(`http://127.0.0.1:5001/api/comments/${{postId}}?limit=3&min_score=2`);
                        const data = await response.json();
                        
                        if (data.success && data.comments.length > 0) {{
                            // Build comments HTML
                            let commentsHtml = '<div class="comments-list">';
                            data.comments.forEach(comment => {{
                                commentsHtml += `
                                    <div class="comment-item">
                                        <div class="comment-meta">
                                            <span class="comment-author">${{comment.author}}</span>
                                            <span class="comment-score">+${{comment.score}}</span>
                                        </div>
                                        <div class="comment-text">${{comment.text}}</div>
                                    </div>
                                `;
                            }});
                            commentsHtml += '</div>';
                            comments.innerHTML = commentsHtml;
                        }} else {{
                            comments.innerHTML = '<div class="no-comments">No comments available or API service not running.</div>';
                        }}
                    }} catch (error) {{
                        console.error('Error fetching comments:', error);
                        comments.innerHTML = '<div class="error-comments">Error loading comments. Make sure the comment API is running on port 5001.</div>';
                    }}
                    
                    button.textContent = 'Hide Comments';
                    button.disabled = false;
                }} else {{
                    // Comments already loaded, just show them
                    comments.style.display = 'block';
                    button.textContent = 'Hide Comments';
                }}
            }} else if (comments) {{
                comments.style.display = 'none';
                button.textContent = 'View Top Comments';
            }}
        }}
        
        document.addEventListener('DOMContentLoaded', function() {{
            const searchInput = document.getElementById('searchInput');
            if (searchInput) {{
                searchInput.addEventListener('keypress', function(e) {{
                    if (e.key === 'Enter') {{
                        searchPosts();
                    }}
                }});
            }}
        }});
        
        // Stock Carousel Functionality
        let currentStockPage = 0;
        const itemsPerPage = 3;
        
        // Entertainment Carousel Functionality
        let currentEntertainmentPage = 0;
        
        function moveStockCarousel(direction) {{
            const track = document.getElementById('stockCarouselTrack');
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (totalItems === 0) return;
            
            // Update current page
            currentStockPage += direction;
            
            // Wrap around if needed
            if (currentStockPage < 0) {{
                currentStockPage = totalPages - 1;
            }} else if (currentStockPage >= totalPages) {{
                currentStockPage = 0;
            }}
            
            // Calculate transform
            const translateX = -(currentStockPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            // Update pagination dots
            updatePaginationDots();
            
            // Update navigation buttons
            const prevBtn = track.closest('.stock-carousel-container').querySelector('.carousel-nav.prev');
            const nextBtn = track.closest('.stock-carousel-container').querySelector('.carousel-nav.next');
            
            if (totalPages <= 1) {{
                prevBtn.disabled = true;
                nextBtn.disabled = true;
            }} else {{
                prevBtn.disabled = false;
                nextBtn.disabled = false;
            }}
        }}
        
        function goToStockPage(pageIndex) {{
            const track = document.getElementById('stockCarouselTrack');
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (pageIndex < 0 || pageIndex >= totalPages) return;
            
            currentStockPage = pageIndex;
            const translateX = -(currentStockPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            updatePaginationDots();
        }}
        
        function updatePaginationDots() {{
            const dots = document.querySelectorAll('.pagination-dot');
            dots.forEach((dot, index) => {{
                if (index === currentStockPage) {{
                    dot.classList.add('active');
                }} else {{
                    dot.classList.remove('active');
                }}
            }});
        }}
        
        // Entertainment Carousel Functions
        function moveEntertainmentCarousel(direction) {{
            const track = document.getElementById('entertainmentCarouselTrack');
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (totalItems === 0) return;
            
            // Update current page
            currentEntertainmentPage += direction;
            
            // Wrap around if needed
            if (currentEntertainmentPage < 0) {{
                currentEntertainmentPage = totalPages - 1;
            }} else if (currentEntertainmentPage >= totalPages) {{
                currentEntertainmentPage = 0;
            }}
            
            // Calculate transform
            const translateX = -(currentEntertainmentPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            // Update pagination dots
            updateEntertainmentPaginationDots();
            
            // Update navigation buttons
            const prevBtn = track.closest('.entertainment-carousel-container').querySelector('.carousel-nav.prev');
            const nextBtn = track.closest('.entertainment-carousel-container').querySelector('.carousel-nav.next');
            
            if (totalPages <= 1) {{
                prevBtn.disabled = true;
                nextBtn.disabled = true;
            }} else {{
                prevBtn.disabled = false;
                nextBtn.disabled = false;
            }}
        }}
        
        function goToEntertainmentPage(pageIndex) {{
            const track = document.getElementById('entertainmentCarouselTrack');
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (pageIndex < 0 || pageIndex >= totalPages) return;
            
            currentEntertainmentPage = pageIndex;
            const translateX = -(currentEntertainmentPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            updateEntertainmentPaginationDots();
        }}
        
        function updateEntertainmentPaginationDots() {{
            const dots = document.querySelectorAll('.entertainment-pagination-dot');
            dots.forEach((dot, index) => {{
                if (index === currentEntertainmentPage) {{
                    dot.classList.add('active');
                }} else {{
                    dot.classList.remove('active');
                }}
            }});
        }}
        
        // Travel Carousel Functions (matching stock/entertainment pattern)
        let currentTravelPage = 0;
        
        function moveTravelCarousel(direction) {{
            const track = document.getElementById('travelCarouselTrack');
            if (!track) return;
            
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (totalItems === 0) return;
            
            // Update current page
            currentTravelPage += direction;
            
            // Wrap around if needed
            if (currentTravelPage < 0) {{
                currentTravelPage = totalPages - 1;
            }} else if (currentTravelPage >= totalPages) {{
                currentTravelPage = 0;
            }}
            
            // Calculate transform
            const translateX = -(currentTravelPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            updateTravelPaginationDots();
        }}
        
        function goToTravelPage(pageIndex) {{
            const track = document.getElementById('travelCarouselTrack');
            if (!track) return;
            
            const items = track.children;
            const totalItems = items.length;
            const totalPages = Math.ceil(totalItems / itemsPerPage);
            
            if (pageIndex < 0 || pageIndex >= totalPages) return;
            
            currentTravelPage = pageIndex;
            const translateX = -(currentTravelPage * 100);
            track.style.transform = `translateX(${{translateX}}%)`;
            
            updateTravelPaginationDots();
        }}
        
        function updateTravelPaginationDots() {{
            const dots = document.querySelectorAll('.travel-pagination-dot');
            dots.forEach((dot, index) => {{
                if (index === currentTravelPage) {{
                    dot.classList.add('active');
                }} else {{
                    dot.classList.remove('active');
                }}
            }});
        }}

        // Add click handlers to pagination dots
        document.addEventListener('DOMContentLoaded', function() {{
            document.querySelectorAll('.pagination-dot').forEach((dot, index) => {{
                dot.addEventListener('click', () => goToStockPage(index));
            }});
            
            document.querySelectorAll('.entertainment-pagination-dot').forEach((dot, index) => {{
                dot.addEventListener('click', () => goToEntertainmentPage(index));
            }});
            
            document.querySelectorAll('.travel-pagination-dot').forEach((dot, index) => {{
                dot.addEventListener('click', () => goToTravelPage(index + 1));
            }});
            
            // Initialize carousels
            if (document.getElementById('stockCarouselTrack')) {{
                moveStockCarousel(0); // Initialize
            }}
            if (document.getElementById('entertainmentCarouselTrack')) {{
                moveEntertainmentCarousel(0); // Initialize
            }}
            if (document.getElementById('travelCarouselTrack')) {{
                moveTravelCarousel(0); // Initialize travel carousel
            }}
        }});
        
    </script>
</body>
</html>"""
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"Clean dashboard generated: {output_file}")
        return output_file
    
    def _generate_category_tabs(self, time_filter='weekly'):
        """Generate category filter tabs for specified time filter in priority order"""
        df = self.weekly_df if time_filter == 'weekly' else self.daily_df
        if df.empty:
            return ""
        
        # Define category priority based on current main category
        if hasattr(self, 'current_category'):
            if self.current_category == 'travel':
                category_priority = [
                    'Travel Advice',
                    'Asian Travel', 'European Travel', 'North America Travel', 'South America Travel', 'Oceania Africa Travel'
                ]
            elif self.current_category == 'entertainment':
                category_priority = [
                    'Recommendation Requests', 'Reviews & Discussions', 'News & Announcements',
                    'Lists & Rankings', 'Identification & Help'
                ]
            else:  # finance
                category_priority = [
                    'Analysis & Education', 'Market News & Politics', 'Questions & Help',
                    'Personal Trading Stories', 'Community Discussion', 'Memes & Entertainment'
                ]
        else:
            # Fallback to value_counts order
            category_priority = list(df['category'].value_counts().index)
        
        # Get available categories with counts
        category_counts = df['category'].value_counts()
        
        tabs = ""
        # Add categories in priority order
        for category in category_priority:
            if category in category_counts:
                count = category_counts[category]
                safe_category = category.replace(' ', '_').replace('&', 'and').lower()
                tabs += f'<button class="tab-btn" onclick="showCategory(\'{safe_category}\')">{category} ({count})</button>\n'
        
        # Add any unexpected categories at the end
        for category in category_counts.index:
            if category not in category_priority:
                count = category_counts[category]
                safe_category = category.replace(' ', '_').replace('&', 'and').lower()
                tabs += f'<button class="tab-btn" onclick="showCategory(\'{safe_category}\')">{category} ({count})</button>\n'
        
        return tabs
    
    def _generate_posts_html(self, time_filter='weekly'):
        """Generate HTML for all posts with SAFE attributes"""
        df = self.weekly_df if time_filter == 'weekly' else self.daily_df
        if df.empty:
            return f"<div class='category-section'><h2>No {time_filter} data available</h2></div>"
            
        posts_html = ""
        
        # Define category priority order for finance (logical flow)
        category_priority = [
            'Analysis & Education',
            'Market News & Politics',
            'Questions & Help',
            'Personal Trading Stories',
            'Community Discussion',
            'Memes & Entertainment'
        ]
        
        # Order categories by post count (most to least) to match sidebar order
        category_counts = df['category'].value_counts()
        ordered_categories = list(category_counts.index)
        
        for category in ordered_categories:
            category_posts = df[df['category'] == category].sort_values('popularity_score', ascending=False)
            safe_category = category.replace(' ', '_').replace('&', 'and').lower()
            
            posts_html += f'<div class="category-section" id="category-{safe_category}-{time_filter}">\n'
            posts_html += f'<div class="category-header-row">\n'
            posts_html += f'<h2 class="category-header">{category}</h2>\n'
            posts_html += f'<button class="summarize-btn" onclick="summarizeCategory(\'{category}\', \'{time_filter}\')" data-category="{category}" data-time-filter="{time_filter}">\n'
            posts_html += f'Summarize\n'
            posts_html += f'</button>\n'
            posts_html += f'</div>\n'
            posts_html += f'<div class="summary-container" id="summary-{safe_category}-{time_filter}" style="display: none;">\n'
            posts_html += f'<div class="summary-content"></div>\n'
            posts_html += f'</div>\n'
            
            # Generate all posts but mark them as visible/hidden for pagination
            post_count = 0
            for _, post in category_posts.iterrows():
                post_count += 1
                # First 10 posts are visible, rest are hidden
                visibility_class = 'post-visible' if post_count <= 10 else 'post-hidden'
                posts_html += self._generate_post_card(post, safe_category, visibility_class)
            
            # Add pagination buttons if there are more than 10 posts
            if len(category_posts) > 10:
                posts_html += f'<div class="pagination-container" id="pagination-{safe_category}-{time_filter}">\n'
                posts_html += f'<button class="show-more-btn" onclick="showMorePosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" data-shown="10" data-total="{len(category_posts)}">Show More</button>\n'
                posts_html += f'<button class="show-less-btn" onclick="showLessPosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" style="display: none;">Show Less</button>\n'
                posts_html += f'</div>\n'
            
            posts_html += '</div>\n'
        
        return posts_html
    
    def _generate_entertainment_category_tabs(self, time_filter='weekly'):
        """Generate category filter tabs for entertainment data"""
        df = self.weekly_entertainment_df if time_filter == 'weekly' else self.daily_entertainment_df
        if df.empty:
            return ""
            
        tabs = ""
        for category, count in df['category'].value_counts().items():
            safe_category = category.replace(' ', '_').replace('&', 'and').lower()
            tabs += f'<button class="tab-btn" onclick="showCategory(\'{safe_category}\')">{category} ({count})</button>\n'
        return tabs
    
    def _generate_travel_category_tabs(self, time_filter='weekly'):
        """Generate simple category filter tabs for unified travel data"""
        df = self.datasets['travel'][time_filter] if 'travel' in self.datasets else pd.DataFrame()
        if df.empty:
            return ""
            
        tabs = ""
        for category, count in df['category'].value_counts().items():
            safe_category = category.replace(' ', '_').replace('&', 'and').lower()
            tabs += f'<button class="tab-btn" onclick="showCategory(\'{safe_category}\')">{category} ({count})</button>\n'
        return tabs
    
    def _generate_entertainment_posts_html(self, time_filter='weekly'):
        """Generate HTML for entertainment posts"""
        df = self.weekly_entertainment_df if time_filter == 'weekly' else self.daily_entertainment_df
        if df.empty:
            return f"<div class='category-section'><h2>No {time_filter} entertainment data available</h2><p>Please run: <code>python generate_entertainment_data.py</code></p></div>"
            
        # Add entertainment sentiment widget
        posts_html = f"""
            <div id="entertainmentSentimentWidget-{time_filter}">
                {self._generate_entertainment_sentiment_widget(time_filter)}
            </div>
        """
        
        # Entertainment category priority - logical flow starting with Recommendation Requests
        category_priority = [
            'Recommendation Requests',
            'Reviews & Discussions',
            'News & Announcements',
            'Lists & Rankings',
            'Identification & Help'
        ]
        
        # Order categories by post count (most to least) to match sidebar order
        category_counts = df['category'].value_counts()
        ordered_categories = list(category_counts.index)
        
        for category in ordered_categories:
            category_posts = df[df['category'] == category].sort_values('popularity_score', ascending=False)
            safe_category = category.replace(' ', '_').replace('&', 'and').lower()
            
            posts_html += f'<div class="category-section" id="category-{safe_category}-{time_filter}">\n'
            posts_html += f'<div class="category-header-row">\n'
            posts_html += f'<h2 class="category-header">{category}</h2>\n'
            posts_html += f'<button class="summarize-btn" onclick="summarizeCategory(\'{category}\', \'{time_filter}\')" data-category="{category}" data-time-filter="{time_filter}">\n'
            posts_html += f'Summarize\n'
            posts_html += f'</button>\n'
            posts_html += f'</div>\n'
            posts_html += f'<div class="summary-container" id="summary-{safe_category}-{time_filter}" style="display: none;">\n'
            posts_html += f'<div class="summary-content"></div>\n'
            posts_html += f'</div>\n'
            
            # Generate all posts but mark them as visible/hidden for pagination
            post_count = 0
            for _, post in category_posts.iterrows():
                post_count += 1
                # First 10 posts are visible, rest are hidden
                visibility_class = 'post-visible' if post_count <= 10 else 'post-hidden'
                posts_html += self._generate_post_card(post, safe_category, visibility_class)
            
            # Add pagination buttons if there are more than 10 posts
            if len(category_posts) > 10:
                posts_html += f'<div class="pagination-container" id="pagination-{safe_category}-{time_filter}">\n'
                posts_html += f'<button class="show-more-btn" onclick="showMorePosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" data-shown="10" data-total="{len(category_posts)}">Show More</button>\n'
                posts_html += f'<button class="show-less-btn" onclick="showLessPosts(\'{safe_category}-{time_filter}\')" data-category="{safe_category}-{time_filter}" style="display: none;">Show Less</button>\n'
                posts_html += f'</div>\n'
            
            posts_html += '</div>\n'
        
        return posts_html
    
    def _generate_post_card(self, post, category, visibility_class='post-visible'):
        """Generate HTML for individual post card with SAFE escaping"""
        # Safely escape all text for HTML attributes
        full_title = str(post['title'])
        title_display = html.escape(full_title[:80] + ('...' if len(full_title) > 80 else ''))
        search_title = html.escape(full_title.lower())
        
        # Handle selftext safely
        selftext = post.get('selftext', '') or ''
        if pd.isna(selftext):
            selftext = ''
        search_content = html.escape(str(selftext).lower()[:500])  # Limit content length
        
        # Process top comments
        top_comments = post.get('top_comments', '[]')
        has_comments = False
        comments_html = ""
        
        if top_comments and top_comments != '[]':
            try:
                import json
                comments_data = json.loads(top_comments)
                if comments_data:
                    has_comments = True
                    comments_html = "<div class='comments-list'>"
                    for comment in comments_data[:3]:  # Display top 3 comments
                        comment_text = html.escape(comment.get('text', ''))
                        comment_score = comment.get('score', 0)
                        comment_author = html.escape(comment.get('author', '[deleted]'))
                        comments_html += f"""
                        <div class='comment-item'>
                            <div class='comment-meta'>
                                <span class='comment-author'>{comment_author}</span>
                                <span class='comment-score'>👍 {comment_score}</span>
                            </div>
                            <div class='comment-text'>{comment_text}</div>
                        </div>
                        """
                    comments_html += "</div>"
            except (json.JSONDecodeError, TypeError):
                has_comments = False
                comments_html = "<p>Comments could not be loaded.</p>"
        
        # Ticker extraction removed
        
        time_ago = self._time_ago(post['created_utc'])
        
        # Build comment button and section (always show button for live fetching)
        comment_button = f"<button class='expand-btn' onclick='toggleComments(this)'>View Top Comments</button>"
        comment_section = f"<div class='post-comments' style='display: none;'></div>"
        
        return f"""
        <div class="post-card {visibility_class}" data-category="{category}" 
             data-popularity="{post['popularity_score']}" data-score="{post['score']}" 
             data-comments="{post['num_comments']}" data-time="{post['created_utc']}"
             data-post-id="{post['post_id']}" data-search-title="{search_title}"
             data-search-content="{search_content}">
            <div class="post-header">
                <h3 class="post-title">{title_display}</h3>
                <div class="post-meta">
                    <span class="subreddit-tag">r/{post['subreddit']}</span>
                    <span class="stat">👍 {post['score']:,}</span>
                    <span class="stat">💬 {post['num_comments']:,}</span>
                    <span class="stat">🕒 {time_ago}</span>
                </div>
            </div>
            <div class="post-actions">
                <a href="{post['url']}" target="_blank" class="view-btn">View Post</a>
                <button class="expand-btn" onclick="toggleDetails(this)">Show Details</button>
                {comment_button}
            </div>
            <div class="post-details" style="display: none;">
                <p><strong>Author:</strong> {post['author']}</p>
                <p><strong>Content Preview:</strong> {str(selftext)[:300]}{'...' if len(str(selftext)) > 300 else ''}</p>
            </div>
            {comment_section}
        </div>
        """
    
    def _time_ago(self, timestamp):
        """Calculate time ago string"""
        now = datetime.now()
        diff = now - timestamp.to_pydatetime().replace(tzinfo=None)
        
        if diff.days > 0:
            return f"{diff.days}d ago"
        elif diff.seconds > 3600:
            return f"{diff.seconds // 3600}h ago"
        else:
            return f"{diff.seconds // 60}m ago"

if __name__ == "__main__":
    dashboard = CleanRedditDashboard('assets')
    dashboard.generate_dashboard()