#!/usr/bin/env python3
"""
Category, Domain, and Subreddit models
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, List
import pandas as pd

from .base import BaseModel, BaseQuery

@dataclass
class Domain(BaseModel):
    """Domain model (Finance, Entertainment, Travel)"""
    
    id: Optional[int] = None
    name: str = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None

@dataclass
class Category(BaseModel):
    """Category model (subcategories within domains)"""
    
    id: Optional[int] = None
    domain_id: int = None
    name: str = None
    description: Optional[str] = None
    confidence_threshold: float = 0.5
    created_at: Optional[datetime] = None
    
    # Related data (populated by queries)
    domain_name: Optional[str] = None

@dataclass
class Subreddit(BaseModel):
    """Subreddit model"""
    
    name: str = None  # Primary key
    domain_id: int = None
    display_name: Optional[str] = None
    description: Optional[str] = None
    subscriber_count: Optional[int] = None
    is_active: bool = True
    comment_multiplier: float = 1.0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    # Related data
    domain_name: Optional[str] = None

class CategoryQuery(BaseQuery):
    """Query builder for categories"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._domain = None
        self._domain_id = None
        self._include_domain = False
    
    def by_domain(self, domain: str) -> 'CategoryQuery':
        """Filter by domain name"""
        self._domain = domain
        return self
    
    def by_domain_id(self, domain_id: int) -> 'CategoryQuery':
        """Filter by domain ID"""
        self._domain_id = domain_id
        return self
    
    def include_domain_info(self) -> 'CategoryQuery':
        """Include domain information in results"""
        self._include_domain = True
        return self
    
    def execute(self) -> pd.DataFrame:
        """Execute the query and return DataFrame"""
        if self._include_domain:
            query = self.db.read_client.table('categories').select('*, domains(name)')
        else:
            query = self.db.read_client.table('categories').select('*')
        
        # Apply filters
        if self._domain:
            query = query.eq('domains.name', self._domain)
        
        if self._domain_id:
            query = query.eq('domain_id', self._domain_id)
        
        # Execute query
        result = query.execute()
        
        if result.data:
            df = self.to_dataframe(result.data)
            
            # Flatten domain information if included
            if self._include_domain and 'domains' in df.columns:
                df['domain_name'] = df['domains'].apply(
                    lambda x: x.get('name') if isinstance(x, dict) else None
                )
                df = df.drop('domains', axis=1)
            
            return df
        else:
            return pd.DataFrame()

class SubredditQuery(BaseQuery):
    """Query builder for subreddits"""
    
    def __init__(self, db_service):
        super().__init__(db_service)
        self._domain = None
        self._domain_id = None
        self._active_only = False
        self._include_domain = False
    
    def by_domain(self, domain: str) -> 'SubredditQuery':
        """Filter by domain name"""
        self._domain = domain
        return self
    
    def by_domain_id(self, domain_id: int) -> 'SubredditQuery':
        """Filter by domain ID"""
        self._domain_id = domain_id
        return self
    
    def active_only(self) -> 'SubredditQuery':
        """Filter to active subreddits only"""
        self._active_only = True
        return self
    
    def include_domain_info(self) -> 'SubredditQuery':
        """Include domain information in results"""
        self._include_domain = True
        return self
    
    def execute(self) -> pd.DataFrame:
        """Execute the query and return DataFrame"""
        if self._include_domain:
            query = self.db.read_client.table('subreddits').select('*, domains(name)')
        else:
            query = self.db.read_client.table('subreddits').select('*')
        
        # Apply filters
        if self._domain:
            query = query.eq('domains.name', self._domain)
        
        if self._domain_id:
            query = query.eq('domain_id', self._domain_id)
        
        if self._active_only:
            query = query.eq('is_active', True)
        
        # Execute query
        result = query.execute()
        
        if result.data:
            df = self.to_dataframe(result.data)
            
            # Flatten domain information if included
            if self._include_domain and 'domains' in df.columns:
                df['domain_name'] = df['domains'].apply(
                    lambda x: x.get('name') if isinstance(x, dict) else None
                )
                df = df.drop('domains', axis=1)
            
            return df
        else:
            return pd.DataFrame()

class DomainQuery(BaseQuery):
    """Query builder for domains"""
    
    def execute(self) -> pd.DataFrame:
        """Get all domains"""
        result = self.db.read_client.table('domains').select('*').execute()
        
        if result.data:
            return self.to_dataframe(result.data)
        else:
            return pd.DataFrame()

# Convenience functions for common queries
def get_all_domains(db_service) -> List[Dict[str, Any]]:
    """Get all available domains"""
    result = db_service.read_client.table('domains').select('*').execute()
    return result.data if result.data else []

def get_categories_for_domain(db_service, domain: str) -> pd.DataFrame:
    """Get categories for a specific domain"""
    return CategoryQuery(db_service).by_domain(domain).include_domain_info().execute()

def get_subreddits_for_domain(db_service, domain: str, active_only: bool = True) -> pd.DataFrame:
    """Get subreddits for a specific domain"""
    query = SubredditQuery(db_service).by_domain(domain).include_domain_info()
    if active_only:
        query = query.active_only()
    return query.execute()

def get_domain_by_name(db_service, domain_name: str) -> Optional[Dict[str, Any]]:
    """Get domain information by name"""
    result = db_service.read_client.table('domains').select('*').eq('name', domain_name).execute()
    return result.data[0] if result.data and len(result.data) > 0 else None

def get_category_by_id(db_service, category_id: int) -> Optional[Dict[str, Any]]:
    """Get category information by ID"""
    result = db_service.read_client.table('categories').select('*, domains(name)').eq('id', category_id).execute()
    
    if result.data and len(result.data) > 0:
        category = result.data[0]
        # Flatten domain info
        if 'domains' in category and isinstance(category['domains'], dict):
            category['domain_name'] = category['domains']['name']
            del category['domains']
        return category
    return None

def get_subreddit_info(db_service, subreddit_name: str) -> Optional[Dict[str, Any]]:
    """Get subreddit information by name"""
    result = db_service.read_client.table('subreddits').select('*, domains(name)').eq('name', subreddit_name).execute()
    
    if result.data and len(result.data) > 0:
        subreddit = result.data[0]
        # Flatten domain info
        if 'domains' in subreddit and isinstance(subreddit['domains'], dict):
            subreddit['domain_name'] = subreddit['domains']['name']
            del subreddit['domains']
        return subreddit
    return None