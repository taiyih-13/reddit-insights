#!/usr/bin/env python3
"""
AI Summarization Service using Groq API
Provides category-level summaries for Reddit finance posts
"""

import os
import json
import pandas as pd
import re
import sys
from groq import Groq
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Add the parent directory to the Python path so we can import from prompts
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prompts.finance_prompt import create_comprehensive_finance_prompt
from prompts.entertainment_prompt import create_entertainment_prompt
from prompts.travel_prompt import create_travel_prompt
from services.enhanced_database_service import get_enhanced_db_service

# Load environment variables from .env file
load_dotenv()

class RedditSummarizer:
    def __init__(self, api_key=None):
        """Initialize the summarizer with Groq API key"""
        self.api_key = api_key or os.getenv('GROQ_API_KEY')
        if not self.api_key:
            raise ValueError("Groq API key required. Set GROQ_API_KEY environment variable or pass api_key parameter.")
        
        self.client = Groq(api_key=self.api_key)
        self.model = "llama-3.1-8b-instant"  # 131k context, 6k TPM on free tier
        self.db_service = get_enhanced_db_service()
    
    def _detect_domain_from_category(self, category):
        """Detect domain based on category name or explicit category"""
        # Check for direct domain matches first
        if category == 'finance':
            return 'finance'
        if category == 'entertainment':
            return 'entertainment'
        if category == 'travel':
            return 'travel'
            
        # Entertainment subcategories
        entertainment_categories = [
            'Recommendation Requests',
            'Reviews & Discussions', 
            'News & Announcements',
            'Lists & Rankings',
            'Identification & Help'
        ]
        
        # Finance subcategories
        finance_categories = [
            'Personal Trading Stories',
            'Analysis & Education',
            'Market News & Politics',
            'Questions & Help',
            'Community Discussion',
            'Memes & Entertainment'
        ]
        
        # Travel subcategories (actual values from database)
        travel_categories = [
            'Europe', 'Asia', 'North America', 'South America', 'Oceania & Africa',
            'General Travel Advice', 'Solo Travel', 'Budget Travel', 'Backpacking'
        ]
        
        if category in entertainment_categories:
            return 'entertainment'
        elif category in finance_categories:
            return 'finance'
        elif category in travel_categories:
            return 'travel'
        else:
            return 'finance'  # Default fallback
    
    def load_reddit_data(self, time_filter='weekly', category=None):
        """Load Reddit data from Supabase database with domain detection"""
        try:
            # Detect domain from category if provided
            domain = self._detect_domain_from_category(category) if category else 'finance'
            
            # Convert time_filter format
            db_time_filter = 'week' if time_filter == 'weekly' else 'day'
            
            # Load data from database
            df = self.db_service.get_posts_with_computed_fields(domain, db_time_filter)
            
            if df.empty:
                raise ValueError(f"No data found for domain: {domain}, time_filter: {db_time_filter}")
            
            # Generate categories from subreddits since computed fields aren't available yet
            df['category'] = df['subreddit'].apply(self._derive_category_from_subreddit)
            
            return df, domain
            
        except Exception as e:
            raise Exception(f"Failed to load data from database: {str(e)}")
    
    def _derive_category_from_subreddit(self, subreddit):
        """Derive category from subreddit name for AI summarization"""
        subreddit = subreddit.lower()
        
        # Travel regional categories
        europe_subs = ['travel_europe', 'uktravel', 'spain', 'france', 'germany', 'greece', 'portugal', 'visitingiceland', 'italytravel']
        asia_subs = ['japantravel', 'thailandtourism', 'indiatravel', 'southeastasia', 'koreatravel', 'chinatravel', 'vietnamtravel', 'nepal', 'indonesia']
        americas_subs = ['mexicotravel', 'usatravel', 'canadatravel', 'caribbeantravel', 'guatemala', 'costarica', 'braziltravel', 'argentina', 'chile', 'peru', 'colombia', 'ecuador']
        oceania_africa_subs = ['australia', 'newzealand', 'southafrica', 'morocco', 'kenya', 'ethiopia']
        travel_advice_subs = ['travel', 'travelnopics', 'travelhacks', 'onebag', 'shoestring']
        solo_travel_subs = ['solotravel']
        budget_travel_subs = ['shoestring', 'onebag']
        backpacking_subs = ['backpacking']
        
        # Entertainment categories
        recommendation_subs = ['moviesuggestions', 'ifyoulikeblank', 'suggestmeabook', 'booksuggestions', 'tipofmytongue', 'animesuggest']
        review_subs = ['movies', 'television', 'truefilm', 'criterion', 'flicks', 'letterboxd', 'horror', 'horrormovies', 'documentaries']
        music_subs = ['music', 'listentothis', 'wearethemusicmakers', 'edmproduction', 'makinghiphop']
        gaming_subs = ['gaming', 'games']
        streaming_subs = ['netflix', 'televisionsuggestions', 'disneyplus', 'hbomax', 'netflixbestof', 'anime']
        
        # Finance categories
        trading_subs = ['wallstreetbets', 'daytrading', 'options', 'thetagang', 'pennystocks']
        investing_subs = ['investing', 'stocks', 'securityanalysis', 'valueinvesting', 'stockmarket']
        crypto_subs = ['bitcoin', 'cryptocurrency', 'ethtrader', 'forex']
        personal_finance_subs = ['personalfinance', 'financialindependence']
        
        # Travel mapping
        if subreddit in europe_subs:
            return 'Europe'
        elif subreddit in asia_subs:
            return 'Asia'  
        elif subreddit in americas_subs:
            return 'North America'  # Simplified for now
        elif subreddit in oceania_africa_subs:
            return 'Oceania & Africa'
        elif subreddit in solo_travel_subs:
            return 'Solo Travel'
        elif subreddit in budget_travel_subs:
            return 'Budget Travel'
        elif subreddit in backpacking_subs:
            return 'Backpacking'
        elif subreddit in travel_advice_subs:
            return 'General Travel Advice'
            
        # Entertainment mapping
        elif subreddit in recommendation_subs:
            return 'Recommendation Requests'
        elif subreddit in review_subs:
            return 'Reviews & Discussions'
        elif subreddit in music_subs:
            return 'News & Announcements'  # Simplified
        elif subreddit in gaming_subs:
            return 'Reviews & Discussions'
        elif subreddit in streaming_subs:
            return 'Lists & Rankings'  # Simplified
            
        # Finance mapping
        elif subreddit in trading_subs:
            return 'Personal Trading Stories'
        elif subreddit in investing_subs:
            return 'Analysis & Education'
        elif subreddit in crypto_subs:
            return 'Market News & Politics'
        elif subreddit in personal_finance_subs:
            return 'Questions & Help'
        
        # Default fallback
        else:
            return 'General'
    
    def estimate_tokens(self, text):
        """Rough token estimation (1 token ≈ 4 chars)"""
        return len(text) // 4
    
    def prepare_posts_for_summary(self, df, category, domain='finance'):
        """Intelligently prepare posts within TPM constraints while maximizing analysis"""
        category_posts = df[df['category'] == category].copy()
        
        if len(category_posts) == 0:
            return None, 0
        
        # Sort by popularity for intelligent selection and limit to top 50 posts
        category_posts = category_posts.sort_values('popularity_score', ascending=False).head(50)
        
        # Smart token management: use ~4500 tokens for content (leaving 1500 for prompt/response)
        target_content_tokens = 4500
        current_tokens = 0
        posts_text = []
        posts_analyzed = 0
        
        for _, post in category_posts.iterrows():
            title = post['title']
            content = str(post.get('selftext', ''))
            
            # Create post summary
            post_summary = f"Title: {title}"
            if content and content != 'nan' and len(content) > 10:
                # Truncate very long content to manage tokens efficiently
                if len(content) > 2000:  # ~500 tokens
                    content = content[:2000] + "... [truncated]"
                post_summary += f"\nContent: {content}"
            
            # Add top comments if available
            top_comments = post.get('top_comments', '[]')
            if top_comments and top_comments != '[]':
                try:
                    import json
                    comments_data = json.loads(top_comments)
                    if comments_data:
                        post_summary += f"\nTop Comments:"
                        for i, comment in enumerate(comments_data[:3], 1):  # Use top 3 for AI processing
                            comment_text = comment.get('text', '')[:150]  # Limit comment length
                            comment_score = comment.get('score', 0)
                            post_summary += f"\n  Comment {i} ({comment_score} upvotes): {comment_text}"
                except (json.JSONDecodeError, TypeError):
                    # Skip comments if JSON parsing fails
                    pass
            
            # Estimate tokens for this post
            post_tokens = self.estimate_tokens(post_summary)
            
            # Check if adding this post would exceed our target
            if current_tokens + post_tokens > target_content_tokens and posts_analyzed > 0:
                break
                
            posts_text.append(post_summary)
            current_tokens += post_tokens
            posts_analyzed += 1
        
        combined_text = "\n\n---\n\n".join(posts_text)
        
        # Log the improvement over old system
        improvement_factor = posts_analyzed / 20 if posts_analyzed > 20 else 1
        print(f"📊 Analyzing {posts_analyzed} posts (vs old limit of 20) - {improvement_factor:.1f}x improvement")
        print(f"🎯 Using ~{current_tokens} tokens of 6000 TPM limit ({current_tokens/6000*100:.1f}% utilization)")
        
        return combined_text, len(category_posts)
    

    def create_summary_prompt(self, category, posts_text, total_posts, time_filter, domain='finance'):
        """Create domain-appropriate prompt for AI summarization"""
        if domain == 'entertainment':
            return create_entertainment_prompt(posts_text, category, total_posts, time_filter)
        elif domain == 'travel':
            return create_travel_prompt(posts_text, category, total_posts, time_filter)
        else:  # finance
            return create_comprehensive_finance_prompt(posts_text, category, total_posts, time_filter)
    
    def remove_incomplete_sentences(self, text, category=None):
        """Remove incomplete content based on category format"""
        
        # Route to category-specific filtering
        if category == 'Recommendation Requests':
            return self._filter_recommendation_format(text)
        else:
            return self._filter_general_format(text)
    
    def _filter_recommendation_format(self, text):
        """Filter content for Recommendation Requests (pipe-separated format)"""
        lines = text.split('\n')
        complete_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                complete_lines.append(line)
                continue
                
            # Check if it's a title format line that might be incomplete
            if line.startswith('**') and '|' in line:
                # Count pipe separators - complete format should have 3 pipes
                pipe_count = line.count('|')
                if pipe_count >= 3:
                    # Check if the line ends properly (with platform info)
                    if 'Available on' in line or line.endswith('**'):
                        complete_lines.append(line)
                    # If it looks incomplete (ends mid-rating like "8."), skip it
                    elif re.search(r'\b\d+\.\s*$', line):
                        break  # Stop here to avoid incomplete ratings
                else:
                    # Incomplete format - stop processing
                    break
            else:
                # Regular text lines - keep if they end properly
                if line.endswith('.') or line.endswith(':') or line.startswith('**') or line.startswith('#'):
                    complete_lines.append(line)
                else:
                    # Incomplete sentence - stop here
                    break
        
        return '\n'.join(complete_lines).strip()
    
    def _filter_general_format(self, text):
        """Filter content for general categories (Reviews, News, Lists, etc.)"""
        lines = text.split('\n')
        complete_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                complete_lines.append(line)
                continue
            
            # Keep section headers and properly formatted content
            if (line.startswith('**') or  # Section headers
                line.startswith('#') or   # Alternative headers
                line.endswith('.') or     # Complete sentences
                line.endswith('!') or     # Exclamations
                line.endswith('?') or     # Questions
                line.endswith(':') or     # Colons (for lists, etc.)
                line.endswith('"') or     # Quoted content
                line.endswith("'") or     # Quoted content
                '•' in line or           # Bullet points
                '-' in line[:3]):        # Dash lists
                complete_lines.append(line)
            else:
                # Only stop if line looks genuinely incomplete (very short or cut off)
                if len(line) < 10 or line.endswith(',') or re.search(r'\b\w+\s*$', line):
                    # Looks incomplete - stop here
                    break
                else:
                    # Probably complete content, keep it
                    complete_lines.append(line)
        
        return '\n'.join(complete_lines).strip()
    
    def generate_summary(self, category, time_filter='weekly'):
        """Generate AI summary for a specific category"""
        try:
            # Load data with domain detection
            df, domain = self.load_reddit_data(time_filter, category)
            
            # Prepare posts
            posts_text, total_posts = self.prepare_posts_for_summary(df, category, domain)
            
            if posts_text is None:
                return {
                    'success': False,
                    'error': f'No posts found in category: {category}'
                }
            
            # Create domain-appropriate prompt
            prompt = self.create_summary_prompt(category, posts_text, total_posts, time_filter, domain)
            
            # Call Groq API with expanded capacity
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,  # Lower temperature for analytical consistency
                max_tokens=750,  # Increased to allow sentence completion (~600 words)
            )
            
            summary = response.choices[0].message.content
            
            # Remove incomplete sentences with category-specific filtering
            clean_summary = self.remove_incomplete_sentences(summary, category)
            
            return {
                'success': True,
                'category': category,
                'time_filter': time_filter,
                'total_posts': total_posts,
                'summary': clean_summary,
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'category': category,
                'time_filter': time_filter
            }

# Flask API for dashboard integration
app = Flask(__name__)
CORS(app)  # Allow requests from dashboard

@app.route('/summarize', methods=['POST'])
def summarize_category():
    """API endpoint for category summarization"""
    try:
        data = request.get_json()
        category = data.get('category')
        time_filter = data.get('time_filter', 'weekly')
        
        if not category:
            return jsonify({'success': False, 'error': 'Category is required'}), 400
        
        # Initialize summarizer (you'll need to set GROQ_API_KEY environment variable)
        summarizer = RedditSummarizer()
        result = summarizer.generate_summary(category, time_filter)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'reddit-summarizer'})

if __name__ == '__main__':
    # Example usage
    print("Reddit AI Summarizer")
    print("====================")
    
    # Check if API key is set
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        print("❌ Please set GROQ_API_KEY environment variable")
        print("   Get your free API key at: https://console.groq.com/keys")
        print("   Then run: export GROQ_API_KEY='your-api-key-here'")
        exit(1)
    
    print("✅ Starting Flask API server...")
    print("📊 Dashboard can now make requests to: http://localhost:5002/summarize")
    
    # Start Flask server on port 5002 to avoid conflicts with comment fetcher
    app.run(debug=True, port=5002, host='127.0.0.1')