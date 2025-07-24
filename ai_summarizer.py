#!/usr/bin/env python3
"""
AI Summarization Service using Groq API
Provides category-level summaries for Reddit finance posts
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
    
    def load_reddit_data(self, time_filter='weekly'):
        """Load Reddit data from CSV files"""
        try:
            if time_filter == 'weekly':
                df = pd.read_csv('week_reddit_posts.csv')
            elif time_filter == 'daily':
                df = pd.read_csv('day_reddit_posts.csv')
            else:
                raise ValueError("time_filter must be 'weekly' or 'daily'")
            
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"Reddit data file not found for {time_filter} posts")
    
    def estimate_tokens(self, text):
        """Rough token estimation (1 token ≈ 4 chars)"""
        return len(text) // 4
    
    def prepare_posts_for_summary(self, df, category):
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
    
    def create_summary_prompt(self, category, posts_text, total_posts, time_filter):
        """Create comprehensive finance-focused prompt for AI summarization"""
        return create_comprehensive_finance_prompt(posts_text, category, total_posts, time_filter)
    
    def remove_incomplete_sentences(self, text):
        """Remove incomplete sentences that end mid-sentence"""
        # Split into sentences using common sentence endings
        sentences = re.split(r'([.!?])', text)
        
        # Reconstruct complete sentences
        complete_text = ""
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                sentence = sentences[i] + sentences[i + 1]
                complete_text += sentence
        
        # If the last part doesn't end with punctuation, it's incomplete - remove it
        if sentences and len(sentences) % 2 == 1:
            last_part = sentences[-1].strip()
            # Only add if it's not just whitespace and ends with proper punctuation
            if last_part and last_part[-1] in '.!?':
                complete_text += last_part
        
        return complete_text.strip()
    
    def generate_summary(self, category, time_filter='weekly'):
        """Generate AI summary for a specific category"""
        try:
            # Load data
            df = self.load_reddit_data(time_filter)
            
            # Prepare posts
            posts_text, total_posts = self.prepare_posts_for_summary(df, category)
            
            if posts_text is None:
                return {
                    'success': False,
                    'error': f'No posts found in category: {category}'
                }
            
            # Create comprehensive prompt
            prompt = self.create_summary_prompt(category, posts_text, total_posts, time_filter)
            
            # Call Groq API with expanded capacity
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,  # Lower temperature for analytical consistency
                max_tokens=450,  # Increased to allow sentence completion (~325 words)
            )
            
            summary = response.choices[0].message.content
            
            # Remove incomplete sentences
            clean_summary = self.remove_incomplete_sentences(summary)
            
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
    print("📊 Dashboard can now make requests to: http://localhost:5001/summarize")
    
    # Start Flask server on port 5001 to avoid conflicts
    app.run(debug=True, port=5001, host='127.0.0.1')