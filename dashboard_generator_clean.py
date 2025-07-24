import pandas as pd
from datetime import datetime
import json
import html

class CleanRedditDashboard:
    def __init__(self, weekly_csv='week_reddit_posts.csv', daily_csv='day_reddit_posts.csv'):
        # Load both datasets
        try:
            self.weekly_df = pd.read_csv(weekly_csv)
            self.weekly_df['created_utc'] = pd.to_datetime(self.weekly_df['created_utc'])
        except FileNotFoundError:
            print(f"Warning: {weekly_csv} not found, creating empty weekly dataset")
            self.weekly_df = pd.DataFrame()
            
        try:
            self.daily_df = pd.read_csv(daily_csv)
            self.daily_df['created_utc'] = pd.to_datetime(self.daily_df['created_utc'])
        except FileNotFoundError:
            print(f"Warning: {daily_csv} not found, creating empty daily dataset")
            self.daily_df = pd.DataFrame()
        
        # Default to weekly for backwards compatibility
        self.df = self.weekly_df if not self.weekly_df.empty else self.daily_df
        
    def generate_dashboard(self, output_file='reddit_dashboard.html'):
        """Generate a unified dashboard with daily/weekly toggle"""
        
        # Calculate stats for both datasets
        weekly_stats = {
            'total_posts': len(self.weekly_df) if not self.weekly_df.empty else 0,
            'total_upvotes': self.weekly_df['score'].sum() if not self.weekly_df.empty else 0,
        }
        
        daily_stats = {
            'total_posts': len(self.daily_df) if not self.daily_df.empty else 0,
            'total_upvotes': self.daily_df['score'].sum() if not self.daily_df.empty else 0,
        }
        
        # Generate HTML
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reddit Finance Dashboard</title>
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
            width: 265px;
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
            margin-left: 265px;
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
                    Finance
                </div>
                <img src="finance-logo.png" alt="Finance Logo" class="sidebar-logo">
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
                
                <div id="weeklyContent" class="time-content active">
                    {self._generate_posts_html('weekly')}
                </div>
                <div id="dailyContent" class="time-content">
                    {self._generate_posts_html('daily')}
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Data for both time filters
        const statsData = {{
            weekly: {{
                posts: {weekly_stats['total_posts']},
                upvotes: {weekly_stats['total_upvotes']}
            }},
            daily: {{
                posts: {daily_stats['total_posts']},
                upvotes: {daily_stats['total_upvotes']}
            }}
        }};
        
        const categoryData = {{
            weekly: `{self._generate_category_tabs('weekly')}`,
            daily: `{self._generate_category_tabs('daily')}`
        }};
        
        function switchTimeFilter(timeFilter) {{
            // Update dropdown value (in case called programmatically)
            document.getElementById('timeFilterSelect').value = timeFilter;
            
            // Update stats
            document.getElementById('totalPosts').textContent = statsData[timeFilter].posts.toLocaleString();
            document.getElementById('totalUpvotes').textContent = statsData[timeFilter].upvotes.toLocaleString();
            
            // Update content visibility
            document.querySelectorAll('.time-content').forEach(content => {{
                content.classList.remove('active');
                content.style.display = 'none';
            }});
            document.getElementById(timeFilter + 'Content').classList.add('active');
            document.getElementById(timeFilter + 'Content').style.display = 'block';
            
            // Update category buttons - replace the content after All Posts button
            const categoryTabs = document.getElementById('categoryTabs');
            const allBtn = document.getElementById('allBtn');
            // Remove all buttons except the All Posts button
            const buttonsToRemove = categoryTabs.querySelectorAll('.tab-btn:not(#allBtn)');
            buttonsToRemove.forEach(btn => btn.remove());
            // Add new category buttons
            allBtn.insertAdjacentHTML('afterend', categoryData[timeFilter]);
            
            // Reset category filter to 'all'
            document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
            document.getElementById('allBtn').classList.add('active');
            
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
                const response = await fetch('http://localhost:5001/summarize', {{
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
                            Make sure the AI service is running: <code>python ai_summarizer.py</code>
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
                        Please start the AI service: <code>python ai_summarizer.py</code>
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
            // Only search within the currently active time filter content
            const activeContent = document.querySelector('.time-content.active');
            const allPosts = activeContent ? activeContent.querySelectorAll('.post-card') : document.querySelectorAll('.post-card');
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
            
            // Get the currently active time filter content
            const activeContent = document.querySelector('.time-content.active');
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
        """Generate category filter tabs for specified time filter"""
        df = self.weekly_df if time_filter == 'weekly' else self.daily_df
        if df.empty:
            return ""
            
        tabs = ""
        for category, count in df['category'].value_counts().items():
            safe_category = category.replace(' ', '_').replace('&', 'and').lower()
            tabs += f'<button class="tab-btn" onclick="showCategory(\'{safe_category}\')">{category} ({count})</button>\n'
        return tabs
    
    def _generate_posts_html(self, time_filter='weekly'):
        """Generate HTML for all posts with SAFE attributes"""
        df = self.weekly_df if time_filter == 'weekly' else self.daily_df
        if df.empty:
            return f"<div class='category-section'><h2>No {time_filter} data available</h2></div>"
            
        posts_html = ""
        
        # Define category priority order (most important first)
        category_priority = [
            'Analysis & Education',
            'Market News & Politics', 
            'Personal Trading Stories',
            'Questions & Help',
            'Community Discussion',
            'Memes & Entertainment'
        ]
        
        # Get categories in priority order, only including ones that exist in data
        available_categories = df['category'].unique()
        ordered_categories = [cat for cat in category_priority if cat in available_categories]
        # Add any unexpected categories at the end
        ordered_categories.extend([cat for cat in available_categories if cat not in category_priority])
        
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
        
        # Ticker extraction removed
        
        time_ago = self._time_ago(post['created_utc'])
        
        return f"""
        <div class="post-card {visibility_class}" data-category="{category}" 
             data-popularity="{post['popularity_score']}" data-score="{post['score']}" 
             data-comments="{post['num_comments']}" data-time="{post['created_utc']}"
             data-search-title="{search_title}"
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
            </div>
            <div class="post-details" style="display: none;">
                <p><strong>Author:</strong> {post['author']}</p>
                <p><strong>Content Preview:</strong> {str(selftext)[:300]}{'...' if len(str(selftext)) > 300 else ''}</p>
            </div>
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
    dashboard = CleanRedditDashboard('week_reddit_posts.csv', 'day_reddit_posts.csv')
    dashboard.generate_dashboard()