#!/usr/bin/env python3
"""
Basic Database Service for Supabase Operations
Minimal service to support enhanced database service
"""

import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

class DatabaseService:
    """Basic database service for Supabase operations"""
    
    def __init__(self):
        # Initialize Supabase client
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_anon_key = os.getenv('SUPABASE_ANON_KEY')
        supabase_service_key = os.getenv('SUPABASE_SERVICE_KEY')
        
        if not supabase_url or not supabase_anon_key:
            raise ValueError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment")
        
        # Use anon key for reads
        self.supabase: Client = create_client(supabase_url, supabase_anon_key)
        
        # Use service key for writes if available (bypasses RLS)
        if supabase_service_key:
            self.supabase_service: Client = create_client(supabase_url, supabase_service_key)
            print("INFO:services.database_service:Database service initialized with Supabase (service key available for writes)")
        else:
            self.supabase_service = self.supabase
            print("INFO:services.database_service:Database service initialized with Supabase (anon key only)")
    
    def get_posts_by_domain(self, domain: str, time_filter: str, limit: int = None) -> pd.DataFrame:
        """Get posts for a specific domain and time filter"""
        try:
            # Get domain-specific subreddits based on enhanced_database_service mapping
            domain_subreddits = self._get_domain_subreddits(domain)
            
            # Query posts
            query = self.supabase.table('posts').select('*').in_('subreddit', domain_subreddits).eq('time_filter', time_filter).order('popularity_score', desc=True)
            
            if limit:
                query = query.limit(limit)
                
            result = query.execute()
            
            if result.data:
                return pd.DataFrame(result.data)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error getting {domain} posts: {e}")
            return pd.DataFrame()
    
    def _get_domain_subreddits(self, domain: str) -> list:
        """Get subreddit list for domain"""
        
        # Finance subreddits
        finance_subreddits = [
            'investing', 'stocks', 'SecurityAnalysis', 'ValueInvesting',
            'wallstreetbets', 'personalfinance', 'financialindependence',
            'pennystocks', 'StockMarket', 'options', 'thetagang',
            'Bitcoin', 'cryptocurrency', 'ethtrader', 'CryptoCurrency',
            'daytrading', 'forex'
        ]
        
        # Entertainment subreddits  
        entertainment_subreddits = [
            'movies', 'television', 'gaming', 'Games', 'tipofmytongue',
            'MovieSuggestions', 'ifyoulikeblank', 'suggestmeabook', 'booksuggestions',
            'Music', 'listentothis', 'WeAreTheMusicMakers', 'edmproduction',
            'makinghiphop', 'netflix', 'televisionsuggestions', 'truefilm',
            'animesuggest', 'horror', 'criterion', 'flicks', 'DisneyPlus',
            'HBOMax', 'NetflixBestOf', 'anime', 'documentaries', 'horrormovies', 'letterboxd'
        ]
        
        # Travel subreddits
        travel_subreddits = [
            'travel', 'solotravel', 'backpacking', 'JapanTravel', 'ItalyTravel',
            'travel_Europe', 'ThailandTourism', 'IndiaTravel', 'SouthEastAsia',
            'koreatravel', 'chinatravel', 'VietnamTravel', 'Nepal', 'indonesia',
            'VisitingIceland', 'uktravel', 'Spain', 'france', 'germany', 'greece',
            'portugal', 'MexicoTravel', 'usatravel', 'CanadaTravel', 'caribbeantravel',
            'Guatemala', 'CostaRica', 'braziltravel', 'argentina', 'chile', 'peru', 
            'colombia', 'ecuador', 'australia', 'newzealand', 'southafrica',
            'Morocco', 'Kenya', 'Ethiopia', 'TravelNoPics', 'travelhacks', 'onebag', 'Shoestring'
        ]
        
        if domain == 'finance':
            return finance_subreddits
        elif domain == 'entertainment':
            return entertainment_subreddits
        elif domain == 'travel':
            return travel_subreddits
        else:
            return []

def get_db_service():
    """Get database service instance"""
    return DatabaseService()