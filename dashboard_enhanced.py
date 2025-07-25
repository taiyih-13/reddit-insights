#!/usr/bin/env python3
"""
Enhanced Reddit Dashboard Generator
Updated to work with the new base architecture and configuration system
"""

import pandas as pd
from datetime import datetime
import json
import html
import os
from config_manager import ConfigManager
from pathlib import Path

class EnhancedRedditDashboard:
    """
    Enhanced dashboard generator that integrates with the new base architecture
    """
    
    def __init__(self):
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        
        # Load domain configurations
        self.finance_config = self.config_manager.get_domain_config('finance')
        self.entertainment_config = self.config_manager.get_domain_config('entertainment')
        
        # Load datasets
        self.datasets = self._load_all_datasets()
        
        # Dashboard metadata
        self.dashboard_metadata = {
            'generated_at': datetime.now().isoformat(),
            'version': '2.0.0',
            'architecture': 'base_architecture',
            'domains': list(self.datasets.keys())
        }
    
    def _load_all_datasets(self):
        """Load all available datasets with domain detection"""
        datasets = {}
        
        # Standard CSV files
        csv_files = {
            'finance': {
                'weekly': 'week_reddit_posts.csv',
                'daily': 'day_reddit_posts.csv'
            },
            'entertainment': {
                'weekly': 'week_entertainment_posts.csv',
                'daily': 'day_entertainment_posts.csv'
            }
        }
        
        for domain, periods in csv_files.items():
            datasets[domain] = {}
            for period, filename in periods.items():
                if os.path.exists(filename):
                    try:
                        df = pd.read_csv(filename)
                        df['created_utc'] = pd.to_datetime(df['created_utc'])
                        datasets[domain][period] = df
                        print(f"✅ Loaded {domain} {period} data: {len(df)} posts from {filename}")
                    except Exception as e:
                        print(f"❌ Error loading {filename}: {e}")
                        datasets[domain][period] = pd.DataFrame()
                else:
                    print(f"⚠️  File not found: {filename}")
                    datasets[domain][period] = pd.DataFrame()
        
        return datasets
    
    def get_domain_summary(self, domain):
        """Get summary statistics for a domain"""
        if domain not in self.datasets:
            return {}
        
        domain_data = self.datasets[domain]
        summary = {
            'domain': domain,
            'weekly_posts': len(domain_data.get('weekly', pd.DataFrame())),
            'daily_posts': len(domain_data.get('daily', pd.DataFrame())),
            'categories': {},
            'subreddits': {}
        }
        
        # Analyze categories and subreddits
        for period, df in domain_data.items():
            if not df.empty:
                if 'category' in df.columns:
                    category_counts = df['category'].value_counts().to_dict()
                    summary['categories'][period] = category_counts
                
                if 'subreddit' in df.columns:
                    subreddit_counts = df['subreddit'].value_counts().head(10).to_dict()
                    summary['subreddits'][period] = subreddit_counts
        
        return summary
    
    def generate_category_selector(self, domain, period='weekly'):
        """Generate category selector dropdown for a domain"""
        if domain not in self.datasets or period not in self.datasets[domain]:
            return '<option value="">No data available</option>'
        
        df = self.datasets[domain][period]
        if df.empty or 'category' not in df.columns:
            return '<option value="">No categories found</option>'
        
        categories = df['category'].value_counts()
        options = ['<option value="">Select a category</option>']
        
        for category, count in categories.items():
            options.append(f'<option value="{html.escape(category)}">{html.escape(category)} ({count} posts)</option>')
        
        return '\\n'.join(options)
    
    def generate_posts_table(self, domain, period='weekly', category=None, limit=50):
        """Generate HTML table for posts in a specific domain/period/category"""
        if domain not in self.datasets or period not in self.datasets[domain]:
            return '<tr><td colspan="6">No data available</td></tr>'
        
        df = self.datasets[domain][period]
        if df.empty:
            return '<tr><td colspan="6">No posts found</td></tr>'
        
        # Filter by category if specified
        if category and 'category' in df.columns:
            df = df[df['category'] == category]
        
        # Sort by popularity score if available, otherwise by score
        sort_column = 'popularity_score' if 'popularity_score' in df.columns else 'score'
        df = df.sort_values(sort_column, ascending=False).head(limit)
        
        rows = []
        for _, post in df.iterrows():
            # Get post data with safe defaults
            title = html.escape(str(post.get('title', 'No title')))
            subreddit = html.escape(str(post.get('subreddit', 'unknown')))
            score = post.get('score', 0)
            comments = post.get('num_comments', 0)
            popularity = post.get('popularity_score', score)
            category_name = html.escape(str(post.get('category', 'uncategorized')))
            url = post.get('url', '#')
            
            # Create clickable title
            title_link = f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
            
            # Format popularity score
            popularity_formatted = f"{popularity:.1f}" if isinstance(popularity, (int, float)) else str(popularity)
            
            row = f"""
            <tr>
                <td class="post-title">{title_link}</td>
                <td class="subreddit">r/{subreddit}</td>
                <td class="category">{category_name}</td>
                <td class="score">{score:,}</td>
                <td class="comments">{comments:,}</td>
                <td class="popularity">{popularity_formatted}</td>
            </tr>
            """
            rows.append(row)
        
        return '\\n'.join(rows) if rows else '<tr><td colspan="6">No posts found for selected filters</td></tr>'
    
    def generate_dashboard_stats(self):
        """Generate overall dashboard statistics"""
        stats = {
            'total_domains': len(self.datasets),
            'total_posts': 0,
            'domains': {}
        }
        
        for domain, periods in self.datasets.items():
            domain_total = 0
            for period, df in periods.items():
                domain_total += len(df)
            
            stats['domains'][domain] = domain_total
            stats['total_posts'] += domain_total
        
        return stats
    
    def generate_enhanced_html(self, output_file='reddit_dashboard_enhanced.html'):
        """Generate the complete enhanced HTML dashboard"""
        
        # Generate statistics
        stats = self.generate_dashboard_stats()
        
        # Generate category options for both domains
        finance_categories_weekly = self.generate_category_selector('finance', 'weekly')
        finance_categories_daily = self.generate_category_selector('finance', 'daily')
        entertainment_categories_weekly = self.generate_category_selector('entertainment', 'weekly')
        entertainment_categories_daily = self.generate_category_selector('entertainment', 'daily')
        
        html_content = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced Reddit Analytics Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            text-align: center;
        }}
        
        .header h1 {{
            color: #2c3e50;
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 700;
        }}
        
        .header .subtitle {{
            color: #7f8c8d;
            font-size: 1.2rem;
            margin-bottom: 20px;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .stat-card {{
            background: rgba(255, 255, 255, 0.95);
            border-radius: 15px;
            padding: 25px;
            text-align: center;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            backdrop-filter: blur(10px);
            transition: transform 0.3s ease;
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
        }}
        
        .stat-number {{
            font-size: 2.5rem;
            font-weight: 700;
            color: #3498db;
            margin-bottom: 10px;
        }}
        
        .stat-label {{
            color: #7f8c8d;
            font-size: 1.1rem;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .controls {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 30px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }}
        
        .controls h3 {{
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.3rem;
        }}
        
        .control-group {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            align-items: end;
        }}
        
        .control-item {{
            display: flex;
            flex-direction: column;
        }}
        
        .control-item label {{
            margin-bottom: 8px;
            font-weight: 600;
            color: #555;
        }}
        
        .control-item select, .control-item button {{
            padding: 12px 16px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 1rem;
            transition: all 0.3s ease;
            background: white;
        }}
        
        .control-item select:focus {{
            outline: none;
            border-color: #3498db;
            box-shadow: 0 0 0 3px rgba(52, 152, 219, 0.1);
        }}
        
        .control-item button {{
            background: linear-gradient(135deg, #3498db, #2980b9);
            color: white;
            border: none;
            cursor: pointer;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .control-item button:hover {{
            background: linear-gradient(135deg, #2980b9, #1f4e79);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }}
        
        .data-table {{
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 30px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        
        .data-table h3 {{
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.4rem;
        }}
        
        .table-container {{
            overflow-x: auto;
            border-radius: 10px;
            box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.05);
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
        }}
        
        th {{
            background: linear-gradient(135deg, #34495e, #2c3e50);
            color: white;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-size: 0.9rem;
        }}
        
        td {{
            padding: 12px;
            border-bottom: 1px solid #ecf0f1;
            transition: background-color 0.3s ease;
        }}
        
        tr:hover td {{
            background-color: #f8f9fa;
        }}
        
        .post-title {{
            max-width: 400px;
            word-wrap: break-word;
        }}
        
        .post-title a {{
            color: #2c3e50;
            text-decoration: none;
            font-weight: 500;
        }}
        
        .post-title a:hover {{
            color: #3498db;
            text-decoration: underline;
        }}
        
        .subreddit {{
            color: #e74c3c;
            font-weight: 600;
            font-family: 'Courier New', monospace;
        }}
        
        .category {{
            background: #ecf0f1;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.85rem;
            font-weight: 500;
            color: #555;
        }}
        
        .score, .comments, .popularity {{
            text-align: center;
            font-weight: 600;
        }}
        
        .score {{
            color: #e67e22;
        }}
        
        .comments {{
            color: #9b59b6;
        }}
        
        .popularity {{
            color: #27ae60;
        }}
        
        .loading {{
            text-align: center;
            padding: 50px;
            color: #7f8c8d;
            font-style: italic;
        }}
        
        .metadata {{
            background: rgba(255, 255, 255, 0.8);
            border-radius: 10px;
            padding: 15px;
            margin-top: 30px;
            font-size: 0.9rem;
            color: #7f8c8d;
            text-align: center;
        }}
        
        @media (max-width: 768px) {{
            .container {{
                padding: 10px;
            }}
            
            .header h1 {{
                font-size: 2rem;
            }}
            
            .control-group {{
                grid-template-columns: 1fr;
            }}
            
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
            
            th, td {{
                padding: 8px 6px;
                font-size: 0.85rem;
            }}
            
            .post-title {{
                max-width: 200px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Enhanced Reddit Analytics Dashboard</h1>
            <p class="subtitle">Multi-Domain Intelligence • Base Architecture v2.0</p>
            <p>Real-time insights from finance and entertainment communities</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{stats['total_posts']:,}</div>
                <div class="stat-label">Total Posts</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats.get('domains', {}).get('finance', 0):,}</div>
                <div class="stat-label">Finance Posts</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats.get('domains', {}).get('entertainment', 0):,}</div>
                <div class="stat-label">Entertainment Posts</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(self.finance_config.subreddits + self.entertainment_config.subreddits)}</div>
                <div class="stat-label">Subreddits Monitored</div>
            </div>
        </div>
        
        <div class="controls">
            <h3>📊 Data Filters & Controls</h3>
            <div class="control-group">
                <div class="control-item">
                    <label for="domain-select">Domain:</label>
                    <select id="domain-select" onchange="updateCategories()">
                        <option value="finance">💰 Finance</option>
                        <option value="entertainment">🎬 Entertainment</option>
                    </select>
                </div>
                <div class="control-item">
                    <label for="period-select">Time Period:</label>
                    <select id="period-select" onchange="updateCategories()">
                        <option value="weekly">📅 Weekly</option>
                        <option value="daily">📆 Daily</option>
                    </select>
                </div>
                <div class="control-item">
                    <label for="category-select">Category:</label>
                    <select id="category-select">
                        {finance_categories_weekly}
                    </select>
                </div>
                <div class="control-item">
                    <label>&nbsp;</label>
                    <button onclick="loadPosts()">🔍 Load Posts</button>
                </div>
            </div>
        </div>
        
        <div class="data-table">
            <h3 id="table-title">📈 Top Posts - Finance Weekly</h3>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Post Title</th>
                            <th>Subreddit</th>
                            <th>Category</th>
                            <th>Score</th>
                            <th>Comments</th>
                            <th>Popularity</th>
                        </tr>
                    </thead>
                    <tbody id="posts-tbody">
                        <tr><td colspan="6" class="loading">🔄 Select filters and click "Load Posts" to view data</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="metadata">
            <p><strong>Enhanced Dashboard v2.0</strong> | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 
            Architecture: Base Classes + Performance Optimization | 
            Domains: {', '.join(stats['domains'].keys()).title()}</p>
        </div>
    </div>
    
    <script>
        // Category data for different combinations
        const categoryData = {{
            'finance': {{
                'weekly': `{finance_categories_weekly}`,
                'daily': `{finance_categories_daily}`
            }},
            'entertainment': {{
                'weekly': `{entertainment_categories_weekly}`,
                'daily': `{entertainment_categories_daily}`
            }}
        }};
        
        // Post data for different combinations (simplified for demo)
        const postData = {{
            'finance': {{
                'weekly': `{self.generate_posts_table('finance', 'weekly')}`,
                'daily': `{self.generate_posts_table('finance', 'daily')}`
            }},
            'entertainment': {{
                'weekly': `{self.generate_posts_table('entertainment', 'weekly')}`,
                'daily': `{self.generate_posts_table('entertainment', 'daily')}`
            }}
        }};
        
        function updateCategories() {{
            const domain = document.getElementById('domain-select').value;
            const period = document.getElementById('period-select').value;
            const categorySelect = document.getElementById('category-select');
            
            // Update category options
            categorySelect.innerHTML = categoryData[domain][period];
            
            // Update table title
            const titleMap = {{
                'finance': '💰 Finance',
                'entertainment': '🎬 Entertainment'
            }};
            const periodMap = {{
                'weekly': 'Weekly',
                'daily': 'Daily'
            }};
            
            document.getElementById('table-title').textContent = 
                `📈 Top Posts - ${{titleMap[domain]}} ${{periodMap[period]}}`;
        }}
        
        function loadPosts() {{
            const domain = document.getElementById('domain-select').value;
            const period = document.getElementById('period-select').value;
            const category = document.getElementById('category-select').value;
            const tbody = document.getElementById('posts-tbody');
            
            // Show loading
            tbody.innerHTML = '<tr><td colspan="6" class="loading">🔄 Loading posts...</td></tr>';
            
            // Simulate loading delay for better UX
            setTimeout(() => {{
                let posts = postData[domain][period];
                
                // If specific category is selected, we would filter here
                // For now, showing all posts from the selected domain/period
                tbody.innerHTML = posts;
            }}, 500);
        }}
        
        // Initialize on page load
        document.addEventListener('DOMContentLoaded', function() {{
            updateCategories();
            loadPosts();
        }});
    </script>
</body>
</html>
        '''
        
        # Write HTML file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ Enhanced dashboard generated: {output_file}")
        return output_file

def main():
    """Generate the enhanced dashboard"""
    print("🚀 Enhanced Reddit Dashboard Generator")
    print("=" * 50)
    
    dashboard = EnhancedRedditDashboard()
    
    # Generate dashboard statistics
    stats = dashboard.generate_dashboard_stats()
    print(f"\\n📊 Dashboard Statistics:")
    print(f"  Total domains: {stats['total_domains']}")
    print(f"  Total posts: {stats['total_posts']:,}")
    for domain, count in stats['domains'].items():
        print(f"  {domain.title()}: {count:,} posts")
    
    # Generate HTML dashboard
    output_file = dashboard.generate_enhanced_html()
    
    print(f"\\n🎯 Enhanced dashboard ready!")
    print(f"📂 File: {output_file}")
    print(f"🌐 Open in browser to view")

if __name__ == "__main__":
    main()