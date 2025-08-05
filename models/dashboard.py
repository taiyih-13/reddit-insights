#!/usr/bin/env python3
"""
Dashboard and logging models
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd
import json

from .base import BaseModel, BaseQuery

@dataclass
class DashboardStats(BaseModel):
    """Dashboard statistics model"""
    
    domain: str = None
    time_filter: str = 'week'
    
    # Core metrics
    total_posts: int = 0
    total_upvotes: int = 0
    avg_score: float = 0.0
    avg_popularity: float = 0.0
    
    # Category breakdown
    top_categories: Optional[Dict[str, int]] = None
    top_subreddits: Optional[Dict[str, int]] = None
    
    # Sentiment summary
    sentiment_summary: Optional[Dict[str, Any]] = None
    
    # Cache metadata
    generated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize after creation"""
        if self.generated_at is None:
            self.generated_at = datetime.now()
        
        if self.top_categories is None:
            self.top_categories = {}
        
        if self.top_subreddits is None:
            self.top_subreddits = {}
        
        if self.sentiment_summary is None:
            self.sentiment_summary = {}
    
    @classmethod
    def from_posts_dataframe(cls, posts_df: pd.DataFrame, domain: str, 
                           time_filter: str = 'week') -> 'DashboardStats':
        """Create DashboardStats from posts DataFrame"""
        
        if posts_df.empty:
            return cls(domain=domain, time_filter=time_filter)
        
        # Calculate core metrics
        total_posts = len(posts_df)
        total_upvotes = posts_df['score'].sum() if 'score' in posts_df.columns else 0
        avg_score = posts_df['score'].mean() if 'score' in posts_df.columns else 0.0
        avg_popularity = posts_df['popularity_score'].mean() if 'popularity_score' in posts_df.columns else 0.0
        
        # Category breakdown
        top_categories = {}
        if 'category_name' in posts_df.columns:
            top_categories = posts_df['category_name'].value_counts().head(10).to_dict()
        
        # Subreddit breakdown
        top_subreddits = {}
        if 'subreddit' in posts_df.columns:
            top_subreddits = posts_df['subreddit'].value_counts().head(10).to_dict()
        
        return cls(
            domain=domain,
            time_filter=time_filter,
            total_posts=total_posts,
            total_upvotes=int(total_upvotes),
            avg_score=float(avg_score),
            avg_popularity=float(avg_popularity),
            top_categories=top_categories,
            top_subreddits=top_subreddits
        )
    
    def to_cache_record(self, ttl_minutes: int = 30) -> Dict[str, Any]:
        """Convert to cache record for database storage"""
        expires_at = datetime.now() + timedelta(minutes=ttl_minutes)
        
        return {
            'time_filter': self.time_filter,
            'domain_name': self.domain,
            'total_posts': self.total_posts,
            'total_upvotes': self.total_upvotes,
            'avg_score': self.avg_score,
            'top_categories': json.dumps(self.top_categories),
            'top_subreddits': json.dumps(self.top_subreddits),
            'sentiment_summary': json.dumps(self.sentiment_summary),
            'expires_at': expires_at.isoformat()
        }

@dataclass
class RefreshLog(BaseModel):
    """Refresh operation log model"""
    
    id: Optional[int] = None
    time_filter: str = None
    domain_name: Optional[str] = None
    
    # Operation metrics
    posts_added: int = 0
    posts_updated: int = 0
    posts_removed: int = 0
    total_posts_after: int = 0
    
    # Performance metrics
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[int] = None
    
    # Status tracking
    success: bool = False
    error_message: Optional[str] = None
    
    # Timestamps
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize after creation"""
        if self.started_at is None:
            self.started_at = datetime.now()
    
    def mark_completed(self, success: bool = True, error_message: str = None):
        """Mark the refresh operation as completed"""
        self.completed_at = datetime.now()
        self.success = success
        self.error_message = error_message
        
        if self.started_at and self.completed_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

class DashboardQuery(BaseQuery):
    """Query builder for dashboard operations"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._domain = None
        self._time_filter = None
    
    def for_domain(self, domain: str) -> 'DashboardQuery':
        """Set domain for dashboard query"""
        self._domain = domain
        return self
    
    def for_time_filter(self, time_filter: str) -> 'DashboardQuery':
        """Set time filter for dashboard query"""
        self._time_filter = time_filter
        return self
    
    def get_cached_stats(self) -> Optional[DashboardStats]:
        """Get cached dashboard stats if available"""
        if not self._domain or not self._time_filter:
            return None
        
        result = self.db.read_client.table('dashboard_cache').select('*').eq('domain_name', self._domain).eq('time_filter', self._time_filter).gt('expires_at', datetime.now().isoformat()).execute()
        
        if result.data and len(result.data) > 0:
            cache_data = result.data[0]
            
            # Parse JSON fields
            top_categories = json.loads(cache_data.get('top_categories', '{}'))
            top_subreddits = json.loads(cache_data.get('top_subreddits', '{}'))
            sentiment_summary = json.loads(cache_data.get('sentiment_summary', '{}'))
            
            return DashboardStats(
                domain=self._domain,
                time_filter=self._time_filter,
                total_posts=cache_data.get('total_posts', 0),
                total_upvotes=cache_data.get('total_upvotes', 0),
                avg_score=cache_data.get('avg_score', 0.0),
                top_categories=top_categories,
                top_subreddits=top_subreddits,
                sentiment_summary=sentiment_summary,
                generated_at=datetime.fromisoformat(cache_data.get('cached_at', datetime.now().isoformat()))
            )
        
        return None
    
    def generate_fresh_stats(self) -> DashboardStats:
        """Generate fresh dashboard stats from current data"""
        if not self._domain or not self._time_filter:
            return DashboardStats()
        
        # Get posts for the domain and time filter
        from .post import PostQuery
        posts_df = PostQuery(self.db).by_domain(self._domain).by_time_filter(self._time_filter).execute()
        
        # Generate stats from posts
        stats = DashboardStats.from_posts_dataframe(posts_df, self._domain, self._time_filter)
        
        # Add sentiment summary if available
        from .sentiment import SentimentQuery
        sentiment_summary = SentimentQuery(self.db).by_domain(self._domain).sentiment_summary()
        stats.sentiment_summary = sentiment_summary
        
        return stats
    
    def get_or_generate_stats(self, use_cache: bool = True, cache_ttl: int = 30) -> DashboardStats:
        """Get cached stats or generate fresh ones"""
        if use_cache:
            cached_stats = self.get_cached_stats()
            if cached_stats:
                return cached_stats
        
        # Generate fresh stats
        fresh_stats = self.generate_fresh_stats()
        
        # Cache the fresh stats
        try:
            cache_record = fresh_stats.to_cache_record(cache_ttl)
            self.db.write_client.table('dashboard_cache').upsert(
                cache_record,
                on_conflict='time_filter,domain_name'
            ).execute()
        except Exception as e:
            # Log error but don't fail the request
            print(f"Failed to cache dashboard stats: {e}")
        
        return fresh_stats
    
    def execute(self) -> DashboardStats:
        """Execute the dashboard query"""
        return self.get_or_generate_stats()

class RefreshLogQuery(BaseQuery):
    """Query builder for refresh logs"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._domain = None
        self._time_filter = None
        self._success_only = False
        self._limit = None
        self._recent_hours = None
    
    def by_domain(self, domain: str) -> 'RefreshLogQuery':
        """Filter by domain"""
        self._domain = domain
        return self
    
    def by_time_filter(self, time_filter: str) -> 'RefreshLogQuery':
        """Filter by time filter"""
        self._time_filter = time_filter
        return self
    
    def success_only(self) -> 'RefreshLogQuery':
        """Filter to successful operations only"""
        self._success_only = True
        return self
    
    def recent(self, hours: int = 24) -> 'RefreshLogQuery':
        """Filter to recent operations"""
        self._recent_hours = hours
        return self
    
    def limit(self, count: int) -> 'RefreshLogQuery':
        """Limit number of results"""
        self._limit = count
        return self
    
    def execute(self) -> pd.DataFrame:
        """Execute the query and return DataFrame"""
        query = self.db.read_client.table('refresh_logs').select('*')
        
        # Apply filters
        if self._domain:
            query = query.eq('domain_name', self._domain)
        
        if self._time_filter:
            query = query.eq('time_filter', self._time_filter)
        
        if self._success_only:
            query = query.eq('success', True)
        
        if self._recent_hours:
            cutoff = datetime.now() - timedelta(hours=self._recent_hours)
            query = query.gte('started_at', cutoff.isoformat())
        
        # Order by most recent
        query = query.order('started_at', desc=True)
        
        # Apply limit
        if self._limit:
            query = query.limit(self._limit)
        
        # Execute query
        result = query.execute()
        
        if result.data:
            return self.to_dataframe(result.data)
        else:
            return pd.DataFrame()

# Convenience functions for dashboard operations
def get_dashboard_stats(db_service, domain: str, time_filter: str = 'week', 
                       use_cache: bool = True) -> DashboardStats:
    """Get dashboard statistics for a domain"""
    return DashboardQuery(db_service).for_domain(domain).for_time_filter(time_filter).get_or_generate_stats(use_cache)

def get_recent_refresh_logs(db_service, domain: str = None, hours: int = 24, 
                          limit: int = 10) -> pd.DataFrame:
    """Get recent refresh operation logs"""
    query = RefreshLogQuery(db_service).recent(hours).limit(limit)
    if domain:
        query = query.by_domain(domain)
    return query.execute()

def create_refresh_log(db_service, time_filter: str, domain: str = None) -> int:
    """Create a new refresh log entry"""
    log = RefreshLog(time_filter=time_filter, domain_name=domain)
    
    result = db_service.write_client.table('refresh_logs').insert(log.to_dict()).execute()
    
    if result.data and len(result.data) > 0:
        return result.data[0]['id']
    return None