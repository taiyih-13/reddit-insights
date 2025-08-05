#!/usr/bin/env python3
"""
Sentiment analysis model and query builder
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List
import pandas as pd
import json

from .base import BaseModel, BaseQuery

@dataclass
class SentimentAnalysis(BaseModel):
    """Sentiment analysis result model"""
    
    id: Optional[int] = None
    post_id: str = None
    sentiment_type: str = None  # 'stock', 'entertainment', 'travel'
    
    # Core sentiment metrics
    sentiment_score: Optional[float] = None
    sentiment_label: str = 'neutral'  # 'positive', 'negative', 'neutral'
    positive_score: Optional[float] = None
    neutral_score: Optional[float] = None
    negative_score: Optional[float] = None
    
    # Domain-specific metadata
    metadata: Optional[Dict[str, Any]] = None
    
    # Processing metadata
    computed_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize after creation"""
        if self.computed_at is None:
            self.computed_at = datetime.now()
        
        if self.metadata is None:
            self.metadata = {}
    
    @classmethod
    def from_analysis_result(cls, post_id: str, sentiment_type: str, 
                           analysis_result: Dict[str, Any]) -> 'SentimentAnalysis':
        """Create SentimentAnalysis from analysis result dictionary"""
        
        # Extract sentiment scores
        sentiment_score = analysis_result.get('sentiment_score', 0.0)
        positive_score = analysis_result.get('positive_score', 0.0)
        neutral_score = analysis_result.get('neutral_score', 0.0)
        negative_score = analysis_result.get('negative_score', 0.0)
        
        # Determine sentiment label
        sentiment_label = 'neutral'
        if sentiment_score > 0.1:
            sentiment_label = 'positive'
        elif sentiment_score < -0.1:
            sentiment_label = 'negative'
        
        # Extract metadata based on sentiment type
        metadata = {}
        if sentiment_type == 'stock':
            metadata['stock_tickers'] = analysis_result.get('stock_tickers', [])
            metadata['financial_entities'] = analysis_result.get('financial_entities', [])
        elif sentiment_type == 'travel':
            metadata['destinations'] = analysis_result.get('destinations', [])
            metadata['travel_aspects'] = analysis_result.get('travel_aspects', [])
        elif sentiment_type == 'entertainment':
            metadata['entities'] = analysis_result.get('entities', [])
            metadata['genres'] = analysis_result.get('genres', [])
        
        return cls(
            post_id=post_id,
            sentiment_type=sentiment_type,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            positive_score=positive_score,
            neutral_score=neutral_score,
            negative_score=negative_score,
            metadata=metadata
        )
    
    def to_json(self) -> Dict[str, Any]:
        """Convert to JSON with proper metadata handling"""
        data = super().to_json()
        
        # Ensure metadata is JSON serializable
        if self.metadata and isinstance(self.metadata, dict):
            data['metadata'] = json.dumps(self.metadata)
        
        return data

class SentimentQuery(BaseQuery):
    """Query builder for sentiment analysis"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._post_id = None
        self._sentiment_type = None
        self._sentiment_label = None
        self._domain = None
        self._limit = None
        self._min_score = None
        self._max_score = None
    
    def by_post(self, post_id: str) -> 'SentimentQuery':
        """Filter by specific post"""
        self._post_id = post_id
        return self
    
    def by_type(self, sentiment_type: str) -> 'SentimentQuery':
        """Filter by sentiment type"""
        self._sentiment_type = sentiment_type
        return self
    
    def by_label(self, sentiment_label: str) -> 'SentimentQuery':
        """Filter by sentiment label (positive, negative, neutral)"""
        self._sentiment_label = sentiment_label
        return self
    
    def by_domain(self, domain: str) -> 'SentimentQuery':
        """Filter by domain through post relationship"""
        self._domain = domain
        return self
    
    def score_range(self, min_score: float = None, max_score: float = None) -> 'SentimentQuery':
        """Filter by sentiment score range"""
        self._min_score = min_score
        self._max_score = max_score
        return self
    
    def limit(self, count: int) -> 'SentimentQuery':
        """Limit number of results"""
        self._limit = count
        return self
    
    def execute(self) -> pd.DataFrame:
        """Execute the query and return DataFrame"""
        # Start with base sentiment query
        if self._domain:
            # Need to join with posts and categories to filter by domain
            query = self.db.read_client.table('sentiment_analysis').select(
                '*, posts!inner(subreddit, categories!inner(domains!inner(name)))'
            )
            query = query.eq('posts.categories.domains.name', self._domain)
        else:
            query = self.db.read_client.table('sentiment_analysis').select('*')
        
        # Apply filters
        if self._post_id:
            query = query.eq('post_id', self._post_id)
        
        if self._sentiment_type:
            query = query.eq('sentiment_type', self._sentiment_type)
        
        if self._sentiment_label:
            query = query.eq('sentiment_label', self._sentiment_label)
        
        if self._min_score is not None:
            query = query.gte('sentiment_score', self._min_score)
        
        if self._max_score is not None:
            query = query.lte('sentiment_score', self._max_score)
        
        # Apply limit
        if self._limit:
            query = query.limit(self._limit)
        
        # Execute query
        result = query.execute()
        
        if result.data:
            df = self.to_dataframe(result.data)
            
            # Parse metadata JSON if present
            if 'metadata' in df.columns:
                df['metadata'] = df['metadata'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else x
                )
            
            return df
        else:
            return pd.DataFrame()
    
    def sentiment_summary(self) -> Dict[str, Any]:
        """Get sentiment summary statistics"""
        df = self.execute()
        
        if df.empty:
            return {
                'total_posts': 0,
                'positive_count': 0,
                'negative_count': 0,
                'neutral_count': 0,
                'avg_sentiment_score': 0.0
            }
        
        summary = {
            'total_posts': len(df),
            'positive_count': len(df[df['sentiment_label'] == 'positive']),
            'negative_count': len(df[df['sentiment_label'] == 'negative']),
            'neutral_count': len(df[df['sentiment_label'] == 'neutral']),
            'avg_sentiment_score': df['sentiment_score'].mean() if 'sentiment_score' in df.columns else 0.0
        }
        
        # Add percentages
        total = summary['total_posts']
        if total > 0:
            summary['positive_percentage'] = (summary['positive_count'] / total) * 100
            summary['negative_percentage'] = (summary['negative_count'] / total) * 100
            summary['neutral_percentage'] = (summary['neutral_count'] / total) * 100
        
        return summary
    
    def top_entities(self, entity_type: str = 'stock_tickers', limit: int = 10) -> List[Dict[str, Any]]:
        """Get top entities from metadata"""
        df = self.execute()
        
        if df.empty or 'metadata' not in df.columns:
            return []
        
        entity_counts = {}
        
        for _, row in df.iterrows():
            metadata = row['metadata']
            if isinstance(metadata, dict) and entity_type in metadata:
                entities = metadata[entity_type]
                if isinstance(entities, list):
                    for entity in entities:
                        if entity not in entity_counts:
                            entity_counts[entity] = {'count': 0, 'avg_sentiment': 0.0, 'scores': []}
                        entity_counts[entity]['count'] += 1
                        entity_counts[entity]['scores'].append(row.get('sentiment_score', 0.0))
        
        # Calculate average sentiment for each entity
        for entity, data in entity_counts.items():
            if data['scores']:
                data['avg_sentiment'] = sum(data['scores']) / len(data['scores'])
        
        # Sort by count and return top entities
        sorted_entities = sorted(
            [{'entity': entity, **data} for entity, data in entity_counts.items()],
            key=lambda x: x['count'],
            reverse=True
        )
        
        return sorted_entities[:limit]

# Convenience functions for common sentiment queries
def get_sentiment_for_post(db_service, post_id: str) -> pd.DataFrame:
    """Get all sentiment analysis for a specific post"""
    return SentimentQuery(db_service).by_post(post_id).execute()

def get_domain_sentiment_summary(db_service, domain: str, sentiment_type: str = None) -> Dict[str, Any]:
    """Get sentiment summary for a domain"""
    query = SentimentQuery(db_service).by_domain(domain)
    if sentiment_type:
        query = query.by_type(sentiment_type)
    return query.sentiment_summary()

def get_positive_sentiment_posts(db_service, domain: str = None, limit: int = 10) -> pd.DataFrame:
    """Get posts with positive sentiment"""
    query = SentimentQuery(db_service).by_label('positive').score_range(min_score=0.1)
    if domain:
        query = query.by_domain(domain)
    if limit:
        query = query.limit(limit)
    return query.execute()

def get_stock_sentiment(db_service, limit: int = 20) -> List[Dict[str, Any]]:
    """Get top stock tickers by sentiment mention count"""
    return SentimentQuery(db_service).by_type('stock').top_entities('stock_tickers', limit)