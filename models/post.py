#!/usr/bin/env python3
"""
Post model and query builder for Reddit posts
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd

from .base import BaseModel, BaseQuery

@dataclass
class Post(BaseModel):
    """Reddit post model matching database schema"""
    
    id: str  # Reddit post_id
    subreddit: str
    title: str
    author: Optional[str] = None
    score: int = 0
    upvote_ratio: Optional[float] = None
    num_comments: int = 0
    created_utc: Optional[datetime] = None
    url: Optional[str] = None
    selftext: Optional[str] = None
    link_flair_text: Optional[str] = None
    
    # Classification results
    category_id: Optional[int] = None
    classification_confidence: Optional[str] = None
    
    # Computed metrics
    popularity_score: Optional[float] = None
    engagement_ratio: Optional[float] = None
    time_bonus: Optional[float] = None
    
    # Processing metadata
    time_filter: str = 'week'
    extracted_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_reddit_submission(cls, submission, category_id: int = None, 
                             time_filter: str = 'week', **kwargs) -> 'Post':
        """Create Post from PRAW submission object"""
        return cls(
            id=submission.id,
            subreddit=submission.subreddit.display_name,
            title=submission.title,
            author=submission.author.name if submission.author else None,
            score=submission.score,
            upvote_ratio=submission.upvote_ratio,
            num_comments=submission.num_comments,
            created_utc=datetime.fromtimestamp(submission.created_utc),
            url=submission.url,
            selftext=submission.selftext,
            link_flair_text=submission.link_flair_text,
            category_id=category_id,
            time_filter=time_filter,
            extracted_at=datetime.now(),
            updated_at=datetime.now(),
            **kwargs
        )
    
    def calculate_popularity_score(self, comment_multiplier: float = 1.0) -> float:
        """Calculate popularity score using the existing algorithm"""
        if not self.created_utc:
            return 0.0
        
        # Time bonus calculation (similar to existing logic)
        hours_since_creation = (datetime.now() - self.created_utc).total_seconds() / 3600
        if hours_since_creation <= 24:
            time_bonus = 1.5  # Recent posts get bonus
        elif hours_since_creation <= 168:  # 1 week
            time_bonus = 1.0
        else:
            time_bonus = 0.5
        
        # Engagement ratio
        engagement_ratio = self.num_comments / max(self.score, 1) if self.score > 0 else 0
        
        # Final score
        popularity_score = (
            self.score * 
            (1 + engagement_ratio * comment_multiplier) * 
            time_bonus
        )
        
        # Update model attributes
        self.popularity_score = popularity_score
        self.engagement_ratio = engagement_ratio
        self.time_bonus = time_bonus
        
        return popularity_score

class PostQuery(BaseQuery):
    """Query builder for Reddit posts"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._domain = None
        self._time_filter = None
        self._subreddit = None
        self._category_id = None
        self._limit = None
        self._order_by = 'popularity_score'
        self._order_desc = True
        self._time_range = None
    
    def by_domain(self, domain: str) -> 'PostQuery':
        """Filter by domain (finance, entertainment, travel)"""
        self._domain = domain
        return self
    
    def by_time_filter(self, time_filter: str) -> 'PostQuery':
        """Filter by time filter (day, week)"""
        self._time_filter = time_filter
        return self
    
    def by_subreddit(self, subreddit: str) -> 'PostQuery':
        """Filter by specific subreddit"""
        self._subreddit = subreddit
        return self
    
    def by_category(self, category_id: int) -> 'PostQuery':
        """Filter by category ID"""
        self._category_id = category_id
        return self
    
    def limit(self, count: int) -> 'PostQuery':
        """Limit number of results"""
        self._limit = count
        return self
    
    def order_by(self, field: str, desc: bool = True) -> 'PostQuery':
        """Order results by field"""
        self._order_by = field
        self._order_desc = desc
        return self
    
    def recent(self, hours: int = None, days: int = None) -> 'PostQuery':
        """Filter to recent posts"""
        if hours:
            self._time_range = datetime.now() - timedelta(hours=hours)
        elif days:
            self._time_range = datetime.now() - timedelta(days=days)
        return self
    
    def execute(self) -> pd.DataFrame:
        """Execute the query and return DataFrame"""
        # Build the query using the posts_with_details view
        query = self.db.read_client.table('posts_with_details').select('*')
        
        # Apply filters
        if self._domain:
            query = query.eq('domain_name', self._domain)
        
        if self._time_filter:
            query = query.eq('time_filter', self._time_filter)
        
        if self._subreddit:
            query = query.eq('subreddit', self._subreddit)
        
        if self._category_id:
            query = query.eq('category_id', self._category_id)
        
        if self._time_range:
            query = query.gte('created_utc', self._time_range.isoformat())
        
        # Apply ordering
        query = query.order(self._order_by, desc=self._order_desc)
        
        # Apply limit
        if self._limit:
            query = query.limit(self._limit)
        
        # Execute query
        result = query.execute()
        
        if result.data:
            return self.to_dataframe(result.data)
        else:
            return pd.DataFrame()
    
    def count(self) -> int:
        """Get count of posts matching query"""
        query = self.db.read_client.table('posts').select('id', count='exact')
        
        # Apply same filters as execute() but without ordering/limiting
        if self._domain:
            # Need to join with categories and domains for domain filter
            query = query.eq('categories.domains.name', self._domain)
        
        if self._time_filter:
            query = query.eq('time_filter', self._time_filter)
        
        if self._subreddit:
            query = query.eq('subreddit', self._subreddit)
        
        if self._category_id:
            query = query.eq('category_id', self._category_id)
        
        if self._time_range:
            query = query.gte('created_utc', self._time_range.isoformat())
        
        result = query.execute()
        return result.count if result.count is not None else 0
    
    def top_by_score(self, limit: int = 10) -> pd.DataFrame:
        """Get top posts by score"""
        return self.order_by('score', desc=True).limit(limit).execute()
    
    def top_by_popularity(self, limit: int = 10) -> pd.DataFrame:
        """Get top posts by popularity score"""
        return self.order_by('popularity_score', desc=True).limit(limit).execute()
    
    def top_by_engagement(self, limit: int = 10) -> pd.DataFrame:
        """Get top posts by engagement ratio"""
        return self.order_by('engagement_ratio', desc=True).limit(limit).execute()
    
    def by_category_breakdown(self) -> Dict[str, int]:
        """Get post count breakdown by category"""
        df = self.execute()
        if df.empty:
            return {}
        
        if 'category_name' in df.columns:
            return df['category_name'].value_counts().to_dict()
        else:
            return {}
    
    def by_subreddit_breakdown(self) -> Dict[str, int]:
        """Get post count breakdown by subreddit"""
        df = self.execute()
        if df.empty:
            return {}
        
        return df['subreddit'].value_counts().to_dict()

# Convenience functions for common queries
def get_posts_by_domain(db_service, domain: str, time_filter: str = 'week', limit: int = None) -> pd.DataFrame:
    """Get posts for a domain"""
    query = PostQuery(db_service).by_domain(domain).by_time_filter(time_filter)
    if limit:
        query = query.limit(limit)
    return query.execute()

def get_top_posts(db_service, domain: str = None, limit: int = 10) -> pd.DataFrame:
    """Get top posts by popularity"""
    query = PostQuery(db_service).top_by_popularity(limit)
    if domain:
        query = query.by_domain(domain)
    return query.execute()

def get_recent_posts(db_service, hours: int = 24, domain: str = None) -> pd.DataFrame:
    """Get recent posts"""
    query = PostQuery(db_service).recent(hours=hours)
    if domain:
        query = query.by_domain(domain)
    return query.execute()