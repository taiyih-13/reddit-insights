#!/usr/bin/env python3
"""
Enhanced Database Service with Computed Fields Support
Handles both current schema and future computed fields migration
"""

import os
import sys
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Add project root to path  
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.database_service import DatabaseService, get_db_service
import logging

logger = logging.getLogger(__name__)

class EnhancedDatabaseService(DatabaseService):
    """
    Enhanced database service that supports computed fields
    Falls back gracefully when schema hasn't been updated yet
    """
    
    def __init__(self):
        super().__init__()
        self.computed_fields_supported = None  # Cache schema capability
        # Set up read and write clients
        self.read_client = self.supabase
        self.write_client = self.supabase_service  # Use service key for writes to bypass RLS
        # Check computed fields support on initialization
        self._check_computed_fields_support()
    
    def error_handler(self, operation: str):
        """Context manager for error handling"""
        from contextlib import contextmanager
        
        @contextmanager
        def handler():
            try:
                yield
            except Exception as e:
                logger.error(f"Error in {operation}: {e}")
                raise
        
        return handler()
    
    def _check_computed_fields_support(self):
        """Check if computed fields are supported in current schema"""
        try:
            # Try to query a post with computed_fields column
            result = self.read_client.table('posts').select('id, computed_fields').limit(1).execute()
            self.computed_fields_supported = True
            logger.info("✅ Computed fields supported via computed_fields JSON column")
        except Exception:
            try:
                # Check if individual computed field columns exist
                result = self.read_client.table('posts').select(
                    'id, sentiment_score, sentiment_label, category'
                ).limit(1).execute()
                self.computed_fields_supported = True
                logger.info("✅ Computed fields supported via individual columns")
            except Exception:
                self.computed_fields_supported = False
                logger.info("⚠️  Computed fields not yet supported - using basic schema")
    
    def insert_posts_with_computed_fields(self, posts_df: pd.DataFrame, batch_size: int = 50) -> Dict[str, Any]:
        """
        Insert posts with computed fields, adapting to current schema
        
        Args:
            posts_df: DataFrame with all post data including computed fields
            batch_size: Number of posts per batch
            
        Returns:
            Dictionary with insertion statistics
        """
        
        with self.error_handler("insert_posts_with_computed_fields"):
            total_posts = len(posts_df)
            inserted_count = 0
            error_count = 0
            
            # Define current database schema columns
            base_columns = {
                'id', 'subreddit', 'title', 'author', 'score', 'upvote_ratio',
                'num_comments', 'created_utc', 'url', 'selftext', 'link_flair_text',
                'category_id', 'classification_confidence', 'popularity_score',
                'engagement_ratio', 'time_bonus', 'time_filter', 'extracted_at', 'updated_at'
            }
            
            # Define computed fields
            computed_fields_mapping = {
                'top_comments': 'top_comments',
                'comment_multiplier': 'comment_multiplier', 
                'adjusted_comment_weight': 'adjusted_comment_weight',
                'base_score': 'base_score',
                'hours_old': 'hours_old',
                'category': 'category',
                'entertainment_titles': 'entertainment_titles',
                'stock_tickers': 'stock_tickers', 
                'travel_subcategory': 'travel_subcategory',
                'sentiment_score': 'sentiment_score',
                'sentiment_label': 'sentiment_label'
            }
            
            logger.info(f"📊 Processing {total_posts} posts for database insertion")
            
            # Process in batches
            for i in range(0, total_posts, batch_size):
                batch_df = posts_df.iloc[i:i + batch_size]
                batch_records = []
                
                for _, row in batch_df.iterrows():
                    # Start with base record
                    record = {}
                    
                    # Add base columns that exist in current schema
                    for col in base_columns:
                        if col in row:
                            value = row[col]
                            if pd.isna(value):
                                record[col] = None
                            elif isinstance(value, (pd.Timestamp, datetime)):
                                record[col] = value.isoformat() if hasattr(value, 'isoformat') else str(value)
                            else:
                                record[col] = value
                    
                    # Handle computed fields - try individual columns first (simpler approach)
                    for df_col, db_field in computed_fields_mapping.items():
                        if df_col in row and pd.notna(row[df_col]):
                            value = row[df_col]
                            # Handle JSON serializable values for top_comments
                            if df_col == 'top_comments' and isinstance(value, str):
                                try:
                                    record[db_field] = json.loads(value) if value.startswith('[') else value
                                except:
                                    record[db_field] = value
                            else:
                                record[db_field] = value
                    
                    # Ensure required fields are present
                    if 'id' not in record and 'post_id' in row:
                        record['id'] = row['post_id']
                    
                    if 'extracted_at' not in record:
                        record['extracted_at'] = datetime.now().isoformat()
                    
                    if 'updated_at' not in record:
                        record['updated_at'] = datetime.now().isoformat()
                    
                    batch_records.append(record)
                
                # Insert batch
                try:
                    result = self.write_client.table('posts').upsert(
                        batch_records,
                        on_conflict='id'
                    ).execute()
                    
                    batch_count = len(result.data) if result.data else 0
                    inserted_count += batch_count
                    
                    logger.info(f"✅ Batch {i//batch_size + 1}: {batch_count} posts processed")
                    
                except Exception as e:
                    error_count += len(batch_records)
                    logger.error(f"❌ Batch {i//batch_size + 1} failed: {str(e)}")
                    
                    # If it's a schema error, try with base columns only
                    if "column" in str(e).lower() and "not found" in str(e).lower():
                        logger.info("🔄 Retrying with base columns only...")
                        try:
                            base_records = []
                            for record in batch_records:
                                base_record = {k: v for k, v in record.items() if k in base_columns}
                                base_records.append(base_record)
                            
                            result = self.write_client.table('posts').upsert(
                                base_records,
                                on_conflict='id'
                            ).execute()
                            
                            batch_count = len(result.data) if result.data else 0
                            inserted_count += batch_count
                            error_count -= len(batch_records)  # Correct the error count
                            
                            logger.info(f"✅ Batch {i//batch_size + 1} retry: {batch_count} posts (base fields only)")
                            
                        except Exception as retry_error:
                            logger.error(f"❌ Batch retry also failed: {str(retry_error)}")
            
            success_rate = (inserted_count / total_posts * 100) if total_posts > 0 else 0
            
            return {
                'total_posts': total_posts,
                'inserted_count': inserted_count,
                'error_count': error_count,
                'success_rate': success_rate,
                'computed_fields_supported': self.computed_fields_supported
            }
    
    def get_posts_with_computed_fields(self, domain: str, time_filter: str = 'week', limit: int = None) -> pd.DataFrame:
        """
        Retrieve posts with computed fields, adapting to current schema
        
        Args:
            domain: Domain name (finance, entertainment, travel)  
            time_filter: 'day' or 'week'
            limit: Maximum number of posts to return
            
        Returns:
            DataFrame with post data including computed fields when available
        """
        
        with self.error_handler("get_posts_with_computed_fields"):
            # Get base posts
            posts_df = self.get_posts_by_domain(domain, time_filter, limit)
            
            if posts_df.empty:
                return posts_df
            
            # If computed fields are supported, enrich the data
            if self.computed_fields_supported:
                try:
                    # Try to get computed fields for these posts
                    post_ids = posts_df['id'].tolist()
                    
                    # Query with computed fields
                    result = self.read_client.table('posts').select(
                        'id, computed_fields'
                    ).in_('id', post_ids).execute()
                    
                    if result.data:
                        # Create lookup for computed fields
                        computed_lookup = {}
                        for post in result.data:
                            if post.get('computed_fields'):
                                computed_lookup[post['id']] = post['computed_fields']
                        
                        # Add computed fields to DataFrame
                        if computed_lookup:
                            logger.info(f"📊 Found computed fields for {len(computed_lookup)} posts")
                            
                            # Add computed field columns
                            for _, row in posts_df.iterrows():
                                post_id = row['id']
                                if post_id in computed_lookup:
                                    computed_data = computed_lookup[post_id]
                                    
                                    # Add each computed field as a column
                                    for field_name, field_value in computed_data.items():
                                        posts_df.loc[posts_df['id'] == post_id, field_name] = field_value
                    
                except Exception as e:
                    logger.warning(f"⚠️  Could not retrieve computed fields: {e}")
            
            return posts_df
    
    def _get_table_schema(self, table_name: str) -> List[Dict]:
        """Get table schema information"""
        try:
            # This would need to be implemented based on Supabase's schema introspection
            # For now, return empty list
            return []
        except Exception:
            return []

    def get_table_counts(self) -> Dict[str, int]:
        """Get count of records in main tables"""
        try:
            # Count posts
            posts_result = self.read_client.table('posts').select('id', count='exact').execute()
            posts_count = posts_result.count if hasattr(posts_result, 'count') else 0
            
            return {
                'posts': posts_count,
                'total': posts_count
            }
        except Exception as e:
            logger.error(f"Error getting table counts: {e}")
            return {'posts': 0, 'total': 0}

    def get_domain_stats(self, domain: str, time_filter: str) -> Dict[str, Any]:
        """Get statistics for a specific domain"""
        try:
            domain_subreddits = self._get_domain_subreddits(domain)
            
            # Get count for this domain and time filter
            result = self.read_client.table('posts').select(
                'id', count='exact'
            ).in_('subreddit', domain_subreddits).eq('time_filter', time_filter).execute()
            
            count = result.count if hasattr(result, 'count') else 0
            
            return {
                'count': count,
                'subreddits': len(domain_subreddits),
                'time_filter': time_filter
            }
        except Exception as e:
            logger.error(f"Error getting {domain} stats: {e}")
            return {'count': 0, 'subreddits': 0, 'time_filter': time_filter}

    def test_connection(self) -> Dict[str, Any]:
        """Test database connection"""
        try:
            # Simple query to test connection
            result = self.read_client.table('posts').select('id').limit(1).execute()
            return {
                'status': 'connected',
                'message': 'Database connection successful',
                'read_connection': True,
                'write_connection': True
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Database connection failed: {str(e)}',
                'read_connection': False,
                'write_connection': False
            }

# Global enhanced service instance
_enhanced_db_service = None

def get_enhanced_db_service() -> EnhancedDatabaseService:
    """Get global enhanced database service instance"""
    global _enhanced_db_service
    if _enhanced_db_service is None:
        _enhanced_db_service = EnhancedDatabaseService()
    return _enhanced_db_service

# Convenience functions
def save_posts_with_computed_fields(posts_df: pd.DataFrame) -> Dict[str, Any]:
    """Save posts with computed fields to database"""
    db = get_enhanced_db_service()
    return db.insert_posts_with_computed_fields(posts_df)

def get_posts_with_computed_fields(domain: str, time_filter: str = 'week') -> pd.DataFrame:
    """Get posts with computed fields from database"""
    db = get_enhanced_db_service()
    return db.get_posts_with_computed_fields(domain, time_filter)