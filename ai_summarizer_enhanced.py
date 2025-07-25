#!/usr/bin/env python3
"""
Enhanced AI Summarization Service
Updated to work with new base architecture and support multiple domains
"""

import os
import json
import pandas as pd
import re
from groq import Groq
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from comprehensive_finance_prompt import create_comprehensive_finance_prompt
from config_manager import ConfigManager

load_dotenv()

class EnhancedRedditSummarizer:
    """
    Enhanced Reddit summarizer that works with the new base architecture
    Supports both finance and entertainment domains
    """
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('GROQ_API_KEY')
        if not self.api_key:
            raise ValueError("Groq API key required. Set GROQ_API_KEY environment variable.")
        
        self.client = Groq(api_key=self.api_key)
        self.model = "llama-3.1-8b-instant"
        
        # Load configuration for domain support
        self.config_manager = ConfigManager()
        
        # Domain-specific prompt templates
        self.prompt_templates = {
            'finance': self._create_finance_prompt,
            'entertainment': self._create_entertainment_prompt
        }
    
    def load_reddit_data(self, time_filter='weekly', domain=None):
        """
        Load Reddit data from CSV files with domain detection
        
        Args:
            time_filter: 'weekly' or 'daily'
            domain: 'finance', 'entertainment', or None for auto-detect
        """
        try:
            # Standard filename mapping
            filename_map = {
                'weekly': 'week_reddit_posts.csv',
                'daily': 'day_reddit_posts.csv'
            }
            
            if time_filter not in filename_map:
                raise ValueError("time_filter must be 'weekly' or 'daily'")
            
            filename = filename_map[time_filter]
            df = pd.read_csv(filename)
            
            # Auto-detect domain if not specified
            if domain is None:
                domain = self._detect_domain(df)
                print(f"🔍 Auto-detected domain: {domain}")
            
            return df, domain
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Reddit data file not found: {filename}")
    
    def _detect_domain(self, df):
        """Detect domain based on subreddits in the data"""
        if df.empty or 'subreddit' not in df.columns:
            return 'finance'  # Default fallback
        
        # Get unique subreddits
        subreddits = set(df['subreddit'].unique())
        
        # Load domain configurations
        try:
            finance_config = self.config_manager.get_domain_config('finance')
            entertainment_config = self.config_manager.get_domain_config('entertainment')
            
            finance_subreddits = set(finance_config.subreddits)
            entertainment_subreddits = set(entertainment_config.subreddits)
            
            # Calculate overlap
            finance_overlap = len(subreddits & finance_subreddits)
            entertainment_overlap = len(subreddits & entertainment_subreddits)
            
            # Return domain with higher overlap
            if entertainment_overlap > finance_overlap:
                return 'entertainment'
            else:
                return 'finance'
                
        except Exception:
            return 'finance'  # Fallback to finance
    
    def estimate_tokens(self, text):
        """Rough token estimation (1 token ≈ 4 chars)"""
        return len(text) // 4
    
    def prepare_posts_for_summary(self, df, category, domain='finance'):
        """
        Prepare posts for summarization with domain awareness
        
        Args:
            df: DataFrame with posts
            category: Category to summarize
            domain: Domain type for context
        """
        if df.empty or 'category' not in df.columns:
            return None, 0
        
        category_posts = df[df['category'] == category].copy()
        
        if len(category_posts) == 0:
            return None, 0
        
        # Domain-specific post limits and processing
        if domain == 'entertainment':
            max_posts = 40  # Entertainment posts may be longer
            target_tokens = 4000
        else:  # finance
            max_posts = 50  # Finance posts are typically shorter
            target_tokens = 4500
        
        # Sort by popularity and limit posts
        category_posts = category_posts.sort_values('popularity_score', ascending=False).head(max_posts)
        
        # Smart token management
        current_tokens = 0
        posts_text = []
        posts_analyzed = 0
        
        for _, post in category_posts.iterrows():
            title = post['title']
            content = str(post.get('selftext', ''))
            
            # Create post summary
            post_summary = f"Title: {title}"
            if content and content != 'nan' and len(content) > 10:
                # Truncate content based on domain
                max_content_length = 1500 if domain == 'entertainment' else 2000
                if len(content) > max_content_length:
                    content = content[:max_content_length] + "... [truncated]"
                post_summary += f"\\nContent: {content}"
            
            # Add subreddit context for entertainment
            if domain == 'entertainment':
                post_summary += f"\\nSource: r/{post.get('subreddit', 'unknown')}"
            
            # Estimate tokens
            post_tokens = self.estimate_tokens(post_summary)
            
            # Check token limit
            if current_tokens + post_tokens > target_tokens and posts_analyzed > 0:
                break
            
            posts_text.append(post_summary)
            current_tokens += post_tokens
            posts_analyzed += 1
        
        combined_text = "\\n\\n---\\n\\n".join(posts_text)
        
        print(f"📊 Analyzing {posts_analyzed} {domain} posts for {category}")
        print(f"🎯 Using ~{current_tokens} tokens ({current_tokens/6000*100:.1f}% of limit)")
        
        return combined_text, len(category_posts)
    
    def _create_finance_prompt(self, posts_text, category, total_posts, time_filter):
        """Create finance-specific prompt"""
        return create_comprehensive_finance_prompt(posts_text, category, total_posts, time_filter)
    
    def _create_entertainment_prompt(self, posts_text, category, total_posts, time_filter):
        """Create entertainment-specific prompt"""
        return f'''Analyze these {total_posts} entertainment posts from the {category} category over the past {time_filter}:

{posts_text}

Create a comprehensive summary focusing on:

**Entertainment Trends & Highlights:**
• Popular shows, movies, or content mentioned
• Emerging trends in entertainment consumption
• Platform-specific discussions (Netflix, Disney+, etc.)
• Genre preferences and recommendations

**Community Insights:**
• Most discussed topics and titles
• User recommendations and reviews
• Seasonal or cultural entertainment patterns
• Platform wars and streaming service comparisons

**Key Takeaways:**
• What entertainment content is capturing attention
• How viewing habits are evolving
• Notable releases or announcements
• Community sentiment about various shows/movies

Keep the summary engaging, informative, and focused on entertainment value. Highlight specific titles, creators, or platforms when relevant.'''
    
    def create_summary_prompt(self, category, posts_text, total_posts, time_filter, domain='finance'):
        """Create domain-appropriate summary prompt"""
        if domain in self.prompt_templates:
            return self.prompt_templates[domain](posts_text, category, total_posts, time_filter)
        else:
            # Fallback to finance prompt
            return self._create_finance_prompt(posts_text, category, total_posts, time_filter)
    
    def remove_incomplete_sentences(self, text):
        """Remove incomplete sentences that end mid-sentence"""
        sentences = re.split(r'([.!?])', text)
        
        complete_text = ""
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                sentence = sentences[i] + sentences[i + 1]
                complete_text += sentence
        
        if sentences and len(sentences) % 2 == 1:
            last_part = sentences[-1].strip()
            if last_part and last_part[-1] in '.!?':
                complete_text += last_part
        
        return complete_text.strip()
    
    def generate_summary(self, category, time_filter='weekly', domain=None):
        """
        Generate AI summary for a specific category with domain support
        
        Args:
            category: Category to summarize
            time_filter: 'weekly' or 'daily'
            domain: 'finance', 'entertainment', or None for auto-detect
        """
        try:
            # Load data with domain detection
            df, detected_domain = self.load_reddit_data(time_filter, domain)
            domain = domain or detected_domain
            
            # Prepare posts
            posts_text, total_posts = self.prepare_posts_for_summary(df, category, domain)
            
            if posts_text is None:
                return {
                    'success': False,
                    'error': f'No posts found in category: {category}',
                    'domain': domain
                }
            
            # Create domain-appropriate prompt
            prompt = self.create_summary_prompt(category, posts_text, total_posts, time_filter, domain)
            
            # Generate summary
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=450,
            )
            
            summary = response.choices[0].message.content
            clean_summary = self.remove_incomplete_sentences(summary)
            
            return {
                'success': True,
                'category': category,
                'domain': domain,
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
                'time_filter': time_filter,
                'domain': domain or 'unknown'
            }
    
    def get_available_categories(self, time_filter='weekly', domain=None):
        """Get list of available categories for summarization"""
        try:
            df, detected_domain = self.load_reddit_data(time_filter, domain)
            domain = domain or detected_domain
            
            if 'category' in df.columns:
                categories = df['category'].value_counts().to_dict()
                return {
                    'success': True,
                    'domain': domain,
                    'categories': categories,
                    'total_posts': len(df)
                }
            else:
                return {
                    'success': False,
                    'error': 'No category column found in data',
                    'domain': domain
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'domain': domain or 'unknown'
            }

# Enhanced Flask API
app = Flask(__name__)
CORS(app)

@app.route('/summarize', methods=['POST'])
def summarize_category():
    """Enhanced API endpoint for category summarization"""
    try:
        data = request.get_json()
        category = data.get('category')
        time_filter = data.get('time_filter', 'weekly')
        domain = data.get('domain')  # Optional domain specification
        
        if not category:
            return jsonify({'success': False, 'error': 'Category is required'}), 400
        
        summarizer = EnhancedRedditSummarizer()
        result = summarizer.generate_summary(category, time_filter, domain)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/categories', methods=['GET'])
def get_categories():
    """Get available categories for a time period"""
    try:
        time_filter = request.args.get('time_filter', 'weekly')
        domain = request.args.get('domain')
        
        summarizer = EnhancedRedditSummarizer()
        result = summarizer.get_available_categories(time_filter, domain)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy', 
        'service': 'enhanced-reddit-summarizer',
        'supports_domains': ['finance', 'entertainment']
    })

if __name__ == '__main__':
    print("🚀 Enhanced Reddit AI Summarizer")
    print("=" * 40)
    
    # Check API key
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        print("❌ Please set GROQ_API_KEY environment variable")
        exit(1)
    
    print("✅ Multi-domain summarizer ready")
    print("🎯 Supports: Finance & Entertainment")
    print("📊 Auto-detects domain from data")
    print("🌐 API server starting on http://localhost:5001")
    
    app.run(debug=True, port=5001, host='127.0.0.1')