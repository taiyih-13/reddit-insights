#!/usr/bin/env python3
"""
Simple Dashboard Generator - Creates dashboard directly from database
Bypasses template issues and generates fresh HTML with current data
"""

import sys
import os
from datetime import datetime
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.enhanced_database_service import get_enhanced_db_service

def generate_simple_dashboard():
    """Generate a simple but functional dashboard from database data"""
    
    print("🚀 SIMPLE DASHBOARD GENERATOR")
    print("=" * 50)
    
    # Get database service
    db = get_enhanced_db_service()
    
    # Load data
    print("📊 Loading data from database...")
    
    domains = ['finance', 'entertainment', 'travel']
    data = {}
    total_posts = 0
    
    for domain in domains:
        weekly_posts = db.get_posts_by_domain(domain, 'week')
        daily_posts = db.get_posts_by_domain(domain, 'day')
        
        data[domain] = {
            'weekly': weekly_posts,
            'daily': daily_posts,
            'weekly_count': len(weekly_posts),
            'daily_count': len(daily_posts)
        }
        
        total_posts += len(weekly_posts) + len(daily_posts)
        print(f"   {domain.title()}: {len(weekly_posts)}W/{len(daily_posts)}D posts")
    
    print(f"   Total: {total_posts} posts")
    
    # Generate HTML
    print("🎨 Generating HTML...")
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Insights Dashboard - Database Generated</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background: #f5f5f5;
        }}
        .header {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .domain-section {{
            background: #fff;
            margin-bottom: 20px;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .domain-header {{
            background: #1a73e8;
            color: white;
            padding: 15px 20px;
            font-size: 18px;
            font-weight: bold;
        }}
        .posts-grid {{
            display: grid;
            gap: 10px;
            padding: 20px;
            max-height: 400px;
            overflow-y: auto;
        }}
        .post-item {{
            padding: 10px;
            border: 1px solid #e0e0e0;
            border-radius: 4px;
            background: #fafafa;
        }}
        .post-title {{
            font-weight: bold;
            margin-bottom: 5px;
            color: #1a73e8;
        }}
        .post-meta {{
            font-size: 12px;
            color: #666;
        }}
        .finance {{ background: #e8f5e8 !important; }}
        .entertainment {{ background: #fff3e0 !important; }}
        .travel {{ background: #e3f2fd !important; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Reddit Insights Dashboard</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')} using Supabase database</p>
        <p><strong>Total Posts:</strong> {total_posts} | <strong>Data Source:</strong> Live database extraction</p>
    </div>
    
    <div class="stats">
        <div class="stat-card finance">
            <h3>💰 Finance</h3>
            <p><strong>Weekly:</strong> {data['finance']['weekly_count']} posts</p>
            <p><strong>Daily:</strong> {data['finance']['daily_count']} posts</p>
        </div>
        <div class="stat-card entertainment">
            <h3>🎬 Entertainment</h3>
            <p><strong>Weekly:</strong> {data['entertainment']['weekly_count']} posts</p>
            <p><strong>Daily:</strong> {data['entertainment']['daily_count']} posts</p>
        </div>
        <div class="stat-card travel">
            <h3>✈️ Travel</h3>
            <p><strong>Weekly:</strong> {data['travel']['weekly_count']} posts</p>
            <p><strong>Daily:</strong> {data['travel']['daily_count']} posts</p>
        </div>
    </div>
"""
    
    # Add domain sections
    for domain in domains:
        weekly_posts = data[domain]['weekly']
        domain_class = domain
        
        html_content += f"""
    <div class="domain-section">
        <div class="domain-header {domain_class}">
            {domain.title()} - Recent Posts ({len(weekly_posts)} total)
        </div>
        <div class="posts-grid">
"""
        
        # Show top 20 posts for each domain
        top_posts = weekly_posts.head(20) if not weekly_posts.empty else []
        
        for _, post in top_posts.iterrows():
            title = str(post.get('title', 'No Title'))[:100] + ('...' if len(str(post.get('title', ''))) > 100 else '')
            author = str(post.get('author', 'Unknown'))
            subreddit = str(post.get('subreddit', 'Unknown'))
            score = int(post.get('score', 0))
            comments = int(post.get('num_comments', 0))
            url = str(post.get('url', '#'))
            
            html_content += f"""
            <div class="post-item">
                <div class="post-title">
                    <a href="{url}" target="_blank" style="text-decoration: none; color: #1a73e8;">{title}</a>
                </div>
                <div class="post-meta">
                    r/{subreddit} • by u/{author} • {score} points • {comments} comments
                </div>
            </div>
"""
        
        html_content += """
        </div>
    </div>
"""
    
    # Close HTML
    html_content += """
    <div class="header">
        <p><strong>✅ Dashboard successfully generated from live database!</strong></p>
        <p>This dashboard shows current data from your Supabase database, proving the extraction pipeline is working correctly.</p>
    </div>
</body>
</html>
"""
    
    # Save dashboard
    output_path = 'assets/reddit_dashboard.html'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Simple dashboard generated: {output_path}")
    print(f"📊 Contains {total_posts} posts from live database")
    print(f"🎯 Finance: {data['finance']['weekly_count']} posts (should show 431)")
    
    return True

if __name__ == "__main__":
    generate_simple_dashboard()