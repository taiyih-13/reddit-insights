import pandas as pd
from datetime import datetime
import json
import html
import os

class CleanRedditDashboard:
    def __init__(self, assets_directory='assets'):
        self.assets_directory = assets_directory
        self.datasets = {}
        
        # Auto-discover all available CSV files
        self._discover_datasets()
        
        # Set default dataset for backwards compatibility
        if 'finance' in self.datasets and not self.datasets['finance']['weekly'].empty:
            self.df = self.datasets['finance']['weekly']
        elif 'finance' in self.datasets and not self.datasets['finance']['daily'].empty:
            self.df = self.datasets['finance']['daily']
        else:
            self.df = pd.DataFrame()
    
    def _discover_datasets(self):
        """Automatically discover and load all available datasets"""
        print("🔍 Discovering available datasets...")
        
        # Define expected categories - travel will be combined
        categories = ['finance', 'entertainment', 'travel_tips', 'regional_travel']
        
        for category in categories:
            self.datasets[category] = {'weekly': pd.DataFrame(), 'daily': pd.DataFrame()}
            
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
        self._combine_travel_categories()
        
        # Summary
        available_categories = [cat for cat in categories if not self.datasets[cat]['weekly'].empty or not self.datasets[cat]['daily'].empty]
        # Replace individual travel categories with combined "travel" in the summary
        if any(cat in available_categories for cat in ['travel_tips', 'regional_travel']):
            available_categories = [cat for cat in available_categories if cat not in ['travel_tips', 'regional_travel']]
            available_categories.append('travel')
        print(f"📊 Available categories: {', '.join(available_categories)}")
        
        # Legacy attribute mappings for backwards compatibility
        if 'finance' in self.datasets:
            self.weekly_df = self.datasets['finance']['weekly']
            self.daily_df = self.datasets['finance']['daily']
        if 'entertainment' in self.datasets:
            self.weekly_entertainment_df = self.datasets['entertainment']['weekly']
            self.daily_entertainment_df = self.datasets['entertainment']['daily']
    
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
                        'solo_travel': 'Solo Travel',
                        'budget_travel': 'Budget Travel',
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
                        'american_travel': 'American Travel',
                        'adventure_travel': 'Adventure Travel'
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
        
        posts_html = ""
        
        # Travel category priority - logical flow starting with Travel Advice
        category_priority = [
            # Travel Tips (logical flow)
            'Travel Advice', 'Solo Travel', 'Budget Travel',
            # Regional Travel (logical order)
            'Asian Travel', 'European Travel', 'American Travel', 'Adventure Travel'
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
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f7f9fc;
            color: #1a202c;
            line-height: 1.6;
        }}
        
        .dashboard {{
            display: flex;
            min-height: 100vh;
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
        }}
        
        .search-card {{
            background: white;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 24px;
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
                const categoryDataKey = currentCategory === 'entertainment' ? 'movies_shows' : currentCategory;
                if (categoryData[categoryDataKey] && categoryData[categoryDataKey][timeFilter]) {{
                    allBtn.insertAdjacentHTML('afterend', categoryData[categoryDataKey][timeFilter]);
                }}
                
                // Reset category filter to 'all'
                document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
                allBtn.classList.add('active');
            }}
            
            // Clear search
            clearSearch();
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
                        category: category,
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
                    'Travel Advice', 'Solo Travel', 'Budget Travel',
                    'Asian Travel', 'European Travel', 'American Travel', 'Adventure Travel'
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
            
        posts_html = ""
        
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