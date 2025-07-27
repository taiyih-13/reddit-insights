#!/usr/bin/env python3
"""
Live Comment Fetcher API
Fetches top comments on-demand for dashboard display
"""

import json
import praw
import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend calls

# Initialize Reddit client
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT')
)

def fetch_live_comments(post_id, limit=3, min_score=2):
    """
    Fetch top comments for a specific post ID
    
    Args:
        post_id: Reddit post ID
        limit: Maximum number of comments to fetch (default: 3)
        min_score: Minimum comment score threshold (default: 2)
    
    Returns:
        List of comment objects or empty list if error
    """
    try:
        # Get the post by ID
        submission = reddit.submission(id=post_id)
        
        # Get comments (this will make the API call)
        submission.comments.replace_more(limit=0)  # Don't load "more comments"
        comments = submission.comments.list()[:20]  # Get first 20 comments
        
        # Filter and sort comments
        valid_comments = [c for c in comments if hasattr(c, 'body') and hasattr(c, 'score')]
        top_comments = sorted(valid_comments, key=lambda x: x.score, reverse=True)
        
        comment_data = []
        for comment in top_comments:
            # Skip deleted/removed comments and comments below threshold  
            if (comment.body not in ['[deleted]', '[removed]'] and
                comment.score >= min_score):
                
                # Limit comment text length
                comment_text = comment.body[:200]
                if len(comment.body) > 200:
                    comment_text += "..."
                
                comment_data.append({
                    "text": comment_text,
                    "score": comment.score,
                    "author": str(comment.author) if comment.author else "[deleted]"
                })
                
                if len(comment_data) >= limit:
                    break
        
        return comment_data
        
    except Exception as e:
        print(f"Error fetching comments for post {post_id}: {e}")
        return []

@app.route('/api/comments/<post_id>')
def get_comments(post_id):
    """API endpoint to fetch comments for a post"""
    try:
        limit = int(request.args.get('limit', 3))
        min_score = int(request.args.get('min_score', 2))
        
        comments = fetch_live_comments(post_id, limit, min_score)
        
        return jsonify({
            'success': True,
            'post_id': post_id,
            'comments': comments,
            'count': len(comments)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'post_id': post_id,
            'comments': []
        }), 500

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'live-comment-fetcher'})

if __name__ == '__main__':
    print("🚀 Starting Live Comment Fetcher API...")
    print("📡 Endpoints:")
    print("   GET /api/comments/<post_id>?limit=3&min_score=2")
    print("   GET /api/health")
    print("🌐 CORS enabled for frontend access")
    
    # Run in development mode
    app.run(host='127.0.0.1', port=5001, debug=True)