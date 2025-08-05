#!/usr/bin/env python3
"""
Fixed Database Service - Strips computed fields to match actual database schema
Ensures new posts can be inserted without schema errors
"""

import pandas as pd
from typing import Dict, Any
from services.enhanced_database_service import get_enhanced_db_service, save_posts_with_computed_fields

def save_posts_basic_schema(posts_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Save posts using only the columns that exist in the database
    Strips out computed fields that cause schema errors
    """
    
    if posts_df.empty:
        return {'inserted_count': 0, 'error_count': 0}
    
    # Database schema columns (based on actual Supabase table)
    database_columns = {
        'id', 'subreddit', 'title', 'author', 'score', 'upvote_ratio',
        'num_comments', 'created_utc', 'url', 'selftext', 'link_flair_text',
        'category_id', 'classification_confidence', 'popularity_score',
        'engagement_ratio', 'time_bonus', 'time_filter', 'extracted_at', 'updated_at'
    }
    
    print(f"📊 Filtering posts to match database schema...")
    print(f"   Original columns: {len(posts_df.columns)}")
    
    # Keep only columns that exist in database
    filtered_df = posts_df.copy()
    
    # Remove computed fields that don't exist in database
    computed_fields_to_remove = [
        'adjusted_comment_weight', 'base_score', 'comment_multiplier',
        'top_comments', 'sentiment_score', 'sentiment_label',
        'stock_tickers', 'entertainment_titles', 'travel_subcategory',
        'destinations', 'city_mentions', 'hours_old'  # any other computed fields
    ]
    
    for field in computed_fields_to_remove:
        if field in filtered_df.columns:
            filtered_df = filtered_df.drop(columns=[field])
            print(f"   Removed computed field: {field}")
    
    # Convert created_utc timestamps to ISO format if needed
    if 'created_utc' in filtered_df.columns:
        def convert_timestamp(ts):
            if pd.isna(ts):
                return None
            try:
                # If it's already a string (ISO format), return as-is
                if isinstance(ts, str):
                    return ts
                # If it's a number (Unix timestamp), convert to ISO
                from datetime import datetime
                return datetime.fromtimestamp(float(ts)).isoformat()
            except:
                return None
        
        filtered_df['created_utc'] = filtered_df['created_utc'].apply(convert_timestamp)
    
    # Fix data types to match database schema
    if 'category_id' in filtered_df.columns:
        filtered_df['category_id'] = filtered_df['category_id'].fillna(1).astype(int)
    if 'score' in filtered_df.columns:
        filtered_df['score'] = filtered_df['score'].fillna(0).astype(int)
    if 'num_comments' in filtered_df.columns:
        filtered_df['num_comments'] = filtered_df['num_comments'].fillna(0).astype(int)
    
    # Ensure float fields are proper floats
    float_fields = ['classification_confidence', 'popularity_score', 'engagement_ratio', 'time_bonus', 'upvote_ratio']
    for field in float_fields:
        if field in filtered_df.columns:
            filtered_df[field] = pd.to_numeric(filtered_df[field], errors='coerce').fillna(0.0)
    
    # Ensure all required columns have appropriate defaults
    for col in database_columns:
        if col not in filtered_df.columns:
            if col in ['extracted_at', 'updated_at']:
                from datetime import datetime
                filtered_df[col] = datetime.now().isoformat()
            elif col == 'category_id':
                filtered_df[col] = 1  # Integer default
            elif col in ['score', 'num_comments']:
                filtered_df[col] = 0  # Integer default
            elif col in ['classification_confidence', 'popularity_score', 'engagement_ratio', 'time_bonus', 'upvote_ratio']:
                filtered_df[col] = 0.0  # Float default
            elif col in ['link_flair_text', 'selftext']:
                filtered_df[col] = ''
            else:
                filtered_df[col] = None
    
    # Keep only database columns
    final_columns = [col for col in database_columns if col in filtered_df.columns]
    filtered_df = filtered_df[final_columns]
    
    print(f"   Final columns: {len(filtered_df.columns)}")
    print(f"   Posts to insert: {len(filtered_df)}")
    
    # Use the database service to insert
    try:
        db_service = get_enhanced_db_service()
        
        # Insert in batches to avoid timeout
        batch_size = 50
        total_inserted = 0
        total_errors = 0
        
        for i in range(0, len(filtered_df), batch_size):
            batch = filtered_df.iloc[i:i+batch_size]
            
            try:
                # Convert to records for Supabase
                records = batch.to_dict('records')
                
                # Insert batch
                result = db_service.supabase_service.table('posts').insert(records).execute()
                
                batch_inserted = len(result.data) if result.data else 0
                total_inserted += batch_inserted
                
                print(f"   ✅ Batch {i//batch_size + 1}: {batch_inserted} posts inserted")
                
            except Exception as e:
                total_errors += len(batch)
                print(f"   ❌ Batch {i//batch_size + 1} failed: {e}")
        
        return {
            'inserted_count': total_inserted,
            'error_count': total_errors,
            'total_processed': len(filtered_df)
        }
        
    except Exception as e:
        print(f"❌ Database insertion failed: {e}")
        return {
            'inserted_count': 0,
            'error_count': len(filtered_df),
            'error_message': str(e)
        }

def get_fixed_db_service():
    """Get database service with fixed schema handling"""
    return get_enhanced_db_service()