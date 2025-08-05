#!/usr/bin/env python3
"""
Original Style Database Dashboard Generator
Generates dashboard matching the exact original design using Supabase data
"""

import pandas as pd
from datetime import datetime
import json
import html
import os
import sys

# Add the parent directory to the Python path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.enhanced_database_service import get_enhanced_db_service

class OriginalStyleDatabaseDashboard:
    """
    Dashboard generator that matches the original design exactly
    Uses pure database data but maintains original UI/UX
    """
    
    def __init__(self, assets_directory='assets'):
        self.assets_directory = assets_directory
        self.datasets = {}
        self.db_service = get_enhanced_db_service()
        
        print("✅ Using database for original-style dashboard generation")
    
    def generate_dashboard(self):
        """Generate dashboard matching original design from database data"""
        
        print("🚀 Generating original-style dashboard from database...")
        
        # Load data from database
        self._load_database_datasets()
        
        if not self.datasets:
            print("❌ No data found in database!")
            return False
        
        # Generate dashboard HTML matching original structure
        dashboard_html = self._generate_original_dashboard_html()
        
        # Save dashboard
        dashboard_path = os.path.join(self.assets_directory, 'reddit_dashboard.html')
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(dashboard_html)
        
        print(f"✅ Original-style database dashboard generated: {dashboard_path}")
        return True
    
    def _load_database_datasets(self):
        """Load all datasets from database"""
        
        print("📊 Loading data from Supabase database...")
        
        domains = ['finance', 'entertainment', 'travel']
        time_filters = ['week', 'day']
        
        for domain in domains:
            for time_filter in time_filters:
                try:
                    # Get posts with computed fields
                    posts_df = self.db_service.get_posts_with_computed_fields(domain, time_filter)
                    
                    if not posts_df.empty:
                        dataset_key = f"{time_filter}_{domain}"
                        
                        # Add legacy attributes for UI compatibility
                        posts_df = self._add_legacy_attributes(posts_df, domain)
                        
                        self.datasets[dataset_key] = posts_df
                        print(f"   ✅ {dataset_key}: {len(posts_df)} posts")
                    else:
                        print(f"   ⚠️  {time_filter}_{domain}: No posts found")
                        
                except Exception as e:
                    print(f"   ❌ Error loading {time_filter}_{domain}: {e}")
        
        total_posts = sum(len(df) for df in self.datasets.values())
        print(f"\n📊 Total posts loaded: {total_posts}")
    
    def _add_legacy_attributes(self, posts_df: pd.DataFrame, domain: str) -> pd.DataFrame:
        """Add legacy attributes for UI compatibility"""
        
        # Ensure required columns exist with defaults
        required_columns = {
            'sentiment_score': 0.0,
            'sentiment_label': 'neutral',
            'hours_old': 0.0,
            'base_score': 0.0,
            'category': 'General',
            'top_comments': '[]'
        }
        
        for col, default_value in required_columns.items():
            if col not in posts_df.columns:
                posts_df[col] = default_value
        
        # Domain-specific attributes
        if domain == 'finance':
            if 'stock_tickers' not in posts_df.columns:
                posts_df['stock_tickers'] = ''
        elif domain == 'entertainment':
            if 'entertainment_titles' not in posts_df.columns:
                posts_df['entertainment_titles'] = ''
        elif domain == 'travel':
            if 'travel_subcategory' not in posts_df.columns:
                posts_df['travel_subcategory'] = ''
        
        # Ensure proper data types
        posts_df['sentiment_score'] = pd.to_numeric(posts_df['sentiment_score'], errors='coerce').fillna(0.0)
        posts_df['hours_old'] = pd.to_numeric(posts_df['hours_old'], errors='coerce').fillna(0.0)
        posts_df['popularity_score'] = pd.to_numeric(posts_df['popularity_score'], errors='coerce').fillna(0.0)
        
        return posts_df
    
    def _generate_original_dashboard_html(self) -> str:
        """Generate the complete dashboard HTML matching original design"""
        
        print("🎨 Generating original-style dashboard HTML...")
        
        # Read the original HTML as template and modify it to use database data
        original_path = os.path.join(self.assets_directory, 'reddit_dashboard_original.html')
        
        if not os.path.exists(original_path):
            print("❌ Original dashboard template not found!")
            return self._generate_fallback_html()
        
        # Read original HTML
        with open(original_path, 'r', encoding='utf-8') as f:
            original_html = f.read()
        
        # Replace data sections with database data
        modified_html = self._replace_data_sections(original_html)
        
        # Add Update Feed button next to title
        modified_html = self._add_update_feed_button(modified_html)
        
        # Update timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d at %H:%M:%S')
        modified_html = modified_html.replace(
            'Generated on ', 
            f'Generated on {timestamp} using Supabase database - '
        )
        
        return modified_html
    
    def _replace_data_sections(self, html_content: str) -> str:
        """Replace data sections in original HTML with database data"""
        
        # This is a complex operation - for now, let's generate sections that match the structure
        # We'll replace the JavaScript data variables with our database data
        
        # Generate JavaScript data objects from database
        js_data = self._generate_javascript_data()
        
        # Find and replace the data section in the original HTML
        # Look for the script section with data definitions
        start_marker = "// === DATA SECTION ==="
        end_marker = "// === END DATA SECTION ==="
        
        if start_marker in html_content and end_marker in html_content:
            start_pos = html_content.find(start_marker)
            end_pos = html_content.find(end_marker) + len(end_marker)
            
            # Replace the data section
            new_html = (
                html_content[:start_pos] + 
                js_data + 
                html_content[end_pos:]
            )
            return new_html
        else:
            # If markers not found, append our data before </script>
            script_end = html_content.rfind("</script>")
            if script_end != -1:
                new_html = (
                    html_content[:script_end] + 
                    "\n" + js_data + "\n" + 
                    html_content[script_end:]
                )
                return new_html
        
        return html_content
    
    def _generate_javascript_data(self) -> str:
        """Generate JavaScript data objects from database data"""
        
        js_data = "// === DATABASE DATA SECTION ===\n"
        
        # Generate data for each domain and time filter
        for dataset_key, df in self.datasets.items():
            if df.empty:
                continue
                
            # Convert DataFrame to JavaScript-friendly format
            posts_data = []
            
            for _, post in df.iterrows():
                post_data = {
                    'id': str(post.get('id', '')),
                    'title': str(post.get('title', '')),
                    'author': str(post.get('author', '')),
                    'subreddit': str(post.get('subreddit', '')),
                    'score': int(post.get('score', 0)),
                    'num_comments': int(post.get('num_comments', 0)),
                    'url': str(post.get('url', '')),
                    'created_utc': str(post.get('created_utc', '')),
                    'popularity_score': float(post.get('popularity_score', 0)),
                    'sentiment_score': float(post.get('sentiment_score', 0)),
                    'sentiment_label': str(post.get('sentiment_label', 'neutral')),
                    'category': str(post.get('category', 'General')),
                    'hours_old': float(post.get('hours_old', 0))
                }
                
                # Add domain-specific fields
                if 'stock_tickers' in post:
                    post_data['stock_tickers'] = str(post.get('stock_tickers', ''))
                if 'entertainment_titles' in post:
                    post_data['entertainment_titles'] = str(post.get('entertainment_titles', ''))
                if 'travel_subcategory' in post:
                    post_data['travel_subcategory'] = str(post.get('travel_subcategory', ''))
                
                posts_data.append(post_data)
            
            # Generate JavaScript variable
            js_data += f"const {dataset_key}_posts = {json.dumps(posts_data, indent=2)};\n\n"
        
        js_data += "// === END DATABASE DATA SECTION ===\n"
        
        return js_data
    
    def _add_update_feed_button(self, html_content: str) -> str:
        """Add Update Feed button next to Reddit Insights title"""
        
        # CSS for the Update Feed button
        button_css = """
        .update-feed-button {
            background: #ff6b35;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            margin-left: 16px;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }
        
        .update-feed-button:hover {
            background: #e55a2b;
            transform: translateY(-1px);
        }
        
        .update-feed-button:disabled {
            background: #cbd5e0;
            cursor: not-allowed;
            transform: none;
        }
        
        .update-feed-button .spinner {
            width: 12px;
            height: 12px;
            border: 2px solid #ffffff40;
            border-top: 2px solid #ffffff;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .update-status {
            margin-left: 12px;
            font-size: 0.875rem;
            color: #4a5568;
            font-weight: 500;
        }
        
        .update-status.success {
            color: #38a169;
        }
        
        .update-status.error {
            color: #e53e3e;
        }
        """
        
        # Add CSS to head section
        head_end = html_content.find('</head>')
        if head_end != -1:
            html_content = html_content[:head_end] + f"<style>{button_css}</style>\n" + html_content[head_end:]
        
        # Add button next to "Reddit Insights" title
        title_replacement = '''<div class="header-title">Reddit Insights
            <button id="updateFeedBtn" class="update-feed-button" onclick="updateFeed()">
                <span id="updateBtnText">Update Feed</span>
            </button>
            <span id="updateStatus" class="update-status"></span>
        </div>'''
        
        html_content = html_content.replace(
            '<div class="header-title">Reddit Insights</div>',
            title_replacement
        )
        
        # Add JavaScript for Update Feed functionality
        update_js = """
        function updateFeed() {
            const btn = document.getElementById('updateFeedBtn');
            const btnText = document.getElementById('updateBtnText');
            const status = document.getElementById('updateStatus');
            
            // Disable button and show loading
            btn.disabled = true;
            btnText.innerHTML = '<div class="spinner"></div> Updating...';
            status.textContent = 'Starting update...';
            status.className = 'update-status';
            
            // Get current time filter and map to API format
            const dashboardTimeFilter = document.getElementById('timeFilterSelect')?.value || 'weekly';
            const timeFilter = dashboardTimeFilter === 'weekly' ? 'week' : 'day';
            
            // Call incremental update API
            fetch('http://localhost:5003/update-feed', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ time_filter: timeFilter })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    status.textContent = data.message;
                    status.className = 'update-status success';
                    
                    // Auto-refresh dashboard after 2 seconds
                    setTimeout(() => {
                        location.reload();
                    }, 2000);
                } else {
                    status.textContent = `Error: ${data.error}`;
                    status.className = 'update-status error';
                }
            })
            .catch(error => {
                console.error('Update failed:', error);
                status.textContent = 'Update failed - check if update service is running';
                status.className = 'update-status error';
            })
            .finally(() => {
                // Re-enable button
                setTimeout(() => {
                    btn.disabled = false;
                    btnText.textContent = 'Update Feed';
                    if (!status.textContent.includes('Error') && !status.textContent.includes('failed')) {
                        status.textContent = '';
                    }
                }, status.textContent.includes('Error') || status.textContent.includes('failed') ? 3000 : 5000);
            });
        }
        """
        
        # Add JavaScript before closing </body> tag
        body_end = html_content.rfind('</body>')
        if body_end != -1:
            html_content = html_content[:body_end] + f"<script>{update_js}</script>\n" + html_content[body_end:]
        
        return html_content
    
    def _generate_fallback_html(self) -> str:
        """Generate fallback HTML if original template not found"""
        
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Insights Dashboard</title>
</head>
<body>
    <h1>Reddit Insights Dashboard</h1>
    <p>Error: Original dashboard template not found at assets/reddit_dashboard_original.html</p>
    <p>Please ensure the original template exists to generate the proper dashboard.</p>
    <p>Generated on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')} using Supabase database</p>
</body>
</html>"""

def main():
    """Main execution function"""
    
    print("🚀 Original Style Database Dashboard Generator")
    print("=" * 50)
    
    generator = OriginalStyleDatabaseDashboard()
    success = generator.generate_dashboard()
    
    if success:
        print("\n✅ Dashboard generation completed successfully!")
        print("📂 View your dashboard: assets/reddit_dashboard.html")
        print("🎨 Uses original design with database data")
    else:
        print("\n❌ Dashboard generation failed!")
    
    return success

if __name__ == "__main__":
    main()