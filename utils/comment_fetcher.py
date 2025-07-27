#!/usr/bin/env python3
"""
Comment Fetcher Utility
Fetches top comments from Reddit posts for enhanced content analysis
"""

import json
import praw


def fetch_top_comments(post, limit=5, min_score=2):
    """
    Fetch top comments from a Reddit post
    
    Args:
        post: PRAW submission object
        limit: Maximum number of comments to fetch (default: 5)
        min_score: Minimum comment score threshold (default: 2)
    
    Returns:
        JSON string containing array of comment objects:
        [{"text": "comment content", "score": 15, "author": "username"}]
        Returns empty array JSON string if no qualifying comments or error
    """
    try:
        # Ensure we can access the post
        if not hasattr(post, 'comments'):
            return '[]'
        
        # Get initial comments without additional API calls
        # Reddit provides comment data with the initial post fetch
        available_comments = [c for c in post.comments.list()[:20] if hasattr(c, 'body') and hasattr(c, 'score')]
        
        # Get top-level comments sorted by score
        top_comments = sorted(available_comments, key=lambda x: x.score, reverse=True)
        
        comment_data = []
        
        for comment in top_comments:
            # Skip deleted/removed comments and comments below threshold  
            if (comment.body not in ['[deleted]', '[removed]'] and
                comment.score >= min_score):
                
                # Limit comment text length for token management
                comment_text = comment.body[:200]
                if len(comment.body) > 200:
                    comment_text += "..."
                
                comment_data.append({
                    "text": comment_text,
                    "score": comment.score,
                    "author": str(comment.author) if comment.author else "[deleted]"
                })
                
                # Stop when we have enough comments
                if len(comment_data) >= limit:
                    break
        
        return json.dumps(comment_data)
        
    except Exception as e:
        # Return empty array on any error (network issues, deleted posts, etc.)
        print(f"Warning: Could not fetch comments for post {getattr(post, 'id', 'unknown')}: {e}")
        return '[]'


def test_comment_fetcher():
    """Test the comment fetcher with a sample post"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )
    
    # Test with a popular post
    try:
        subreddit = reddit.subreddit('investing')
        posts = list(subreddit.hot(limit=5))
        
        for post in posts:
            print(f"\nPost: {post.title[:50]}...")
            comments = fetch_top_comments(post, limit=3)
            print(f"Comments: {comments}")
            
    except Exception as e:
        print(f"Test failed: {e}")


if __name__ == "__main__":
    test_comment_fetcher()