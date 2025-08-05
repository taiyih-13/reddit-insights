import re
import pandas as pd
from collections import defaultdict, Counter
import ast

class TravelCityTracker:
    def __init__(self):
        # Popular cities worldwide - comprehensive list for detection
        self.world_cities = {
            # Europe
            'london', 'paris', 'rome', 'barcelona', 'amsterdam', 'berlin', 'prague', 
            'vienna', 'budapest', 'istanbul', 'athens', 'lisbon', 'madrid', 'florence', 
            'venice', 'milan', 'munich', 'zurich', 'geneva', 'stockholm', 'copenhagen', 
            'oslo', 'helsinki', 'reykjavik', 'dublin', 'edinburgh', 'brussels', 'antwerp',
            
            # Asia
            'tokyo', 'kyoto', 'osaka', 'bangkok', 'singapore', 'hong kong', 'seoul', 
            'busan', 'beijing', 'shanghai', 'mumbai', 'delhi', 'bangalore', 'chennai', 
            'kolkata', 'hanoi', 'ho chi minh city', 'kuala lumpur', 'jakarta', 'manila', 
            'bali', 'kathmandu', 'colombo', 'male',
            
            # Americas
            'new york', 'los angeles', 'san francisco', 'chicago', 'boston', 'washington dc',
            'miami', 'las vegas', 'seattle', 'portland', 'denver', 'austin', 'nashville',
            'new orleans', 'vancouver', 'toronto', 'montreal', 'quebec city', 'mexico city',
            'cancun', 'guadalajara', 'rio de janeiro', 'sao paulo', 'buenos aires', 'lima',
            'cusco', 'santiago', 'montevideo', 'bogota', 'cartagena', 'quito',
            
            # Oceania & Africa
            'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'auckland', 'wellington',
            'christchurch', 'queenstown', 'cape town', 'johannesburg', 'durban', 'cairo',
            'marrakech', 'casablanca', 'tunis', 'nairobi', 'dar es salaam', 'addis ababa',
            
            # Middle East
            'dubai', 'abu dhabi', 'doha', 'kuwait city', 'riyadh', 'jeddah', 'muscat',
            'beirut', 'amman', 'tel aviv', 'jerusalem'
        }
        
        # Travel advice context patterns - only look in advice-focused content
        self.advice_patterns = [
            r'\b(recommend|suggestion|visit|go to|check out|must see|worth visiting)\b',
            r'\b(advice|tips|guide|itinerary|planning)\b',
            r'\b(best.*city|favorite.*place|amazing.*destination)\b',
            r'\b(should.*visit|worth.*trip|highly.*recommend)\b'
        ]
    
    def _has_advice_context(self, text):
        """Check if text contains travel advice context"""
        text_lower = text.lower()
        return any(re.search(pattern, text_lower, re.IGNORECASE) 
                  for pattern in self.advice_patterns)
    
    def extract_mentioned_cities(self, title, content):
        """Extract cities mentioned in travel posts"""
        # Combine title and content - weight title more heavily
        title_text = str(title) if not pd.isna(title) else ""
        content_text = str(content) if not pd.isna(content) else ""
        
        # Search in both title and content separately for better detection
        mentioned_cities = set()
        
        # Search in title (higher weight)
        title_lower = title_text.lower()
        for city in self.world_cities:
            city_pattern = r'\b' + re.escape(city) + r'\b'
            if re.search(city_pattern, title_lower, re.IGNORECASE):
                mentioned_cities.add(city.title())
        
        # Search in content (more comprehensive)
        content_lower = content_text.lower()
        for city in self.world_cities:
            city_pattern = r'\b' + re.escape(city) + r'\b'
            if re.search(city_pattern, content_lower, re.IGNORECASE):
                mentioned_cities.add(city.title())
        
        return list(mentioned_cities)
    
    def get_most_mentioned_cities(self, travel_df):
        """Get most mentioned cities from travel advice posts"""
        city_mentions = Counter()
        
        # Only use truly unbiased general travel advice subreddits
        allowed_travel_advice_subreddits = {
            'travel', 'travelnopics', 'travelhacks', 'onebag', 'solotravel', 
            'shoestring', 'backpacking', 'budgettravel', 'longtermtravel', 'digitalnomad'
        }
        
        for _, row in travel_df.iterrows():
            # Only include general travel advice subreddits (no regional bias)
            subreddit = str(row.get('subreddit', '')).lower()
            if subreddit not in allowed_travel_advice_subreddits:
                continue
            
            # Extract cities from this post
            cities = self.extract_mentioned_cities(row.get('title', ''), row.get('selftext', ''))
            
            # Count each city mention
            for city in cities:
                city_mentions[city] += 1
        
        return city_mentions
    
    def get_top_cities_display(self, travel_df, top_n=15):
        """Get top N most mentioned cities for display"""
        city_mentions = self.get_most_mentioned_cities(travel_df)
        
        if not city_mentions:
            return []
        
        # Get top cities
        top_cities = city_mentions.most_common(top_n)
        
        # Format for display
        result = []
        for city, mention_count in top_cities:
            result.append({
                'city': city,
                'mentions': mention_count,
                'emoji': self._get_city_emoji(city)
            })
        
        return result
    
    def _get_city_emoji(self, city):
        """Get appropriate emoji for city based on region"""
        city_lower = city.lower()
        
        # European cities
        european_cities = {
            'london', 'paris', 'rome', 'barcelona', 'amsterdam', 'berlin', 'prague',
            'vienna', 'budapest', 'athens', 'lisbon', 'madrid', 'florence', 'venice',
            'milan', 'munich', 'zurich', 'stockholm', 'copenhagen', 'oslo', 'helsinki',
            'reykjavik', 'dublin', 'edinburgh', 'brussels'
        }
        
        # Asian cities
        asian_cities = {
            'tokyo', 'kyoto', 'osaka', 'bangkok', 'singapore', 'hong kong', 'seoul',
            'beijing', 'shanghai', 'mumbai', 'delhi', 'hanoi', 'kuala lumpur', 'jakarta',
            'manila', 'kathmandu', 'colombo'
        }
        
        # American cities
        american_cities = {
            'new york', 'los angeles', 'san francisco', 'chicago', 'boston', 'miami',
            'las vegas', 'seattle', 'vancouver', 'toronto', 'montreal', 'mexico city',
            'cancun', 'rio de janeiro', 'buenos aires', 'lima', 'santiago'
        }
        
        # Middle Eastern cities
        middle_eastern_cities = {
            'dubai', 'abu dhabi', 'doha', 'istanbul', 'beirut', 'amman', 'tel aviv'
        }
        
        if city_lower in european_cities:
            return '🇪🇺'
        elif city_lower in asian_cities:
            return '🌏'
        elif city_lower in american_cities:
            return '🌎'
        elif city_lower in middle_eastern_cities:
            return '🏛️'
        elif city_lower in {'sydney', 'melbourne', 'auckland', 'wellington'}:
            return '🇦🇺'
        elif city_lower in {'cape town', 'johannesburg', 'cairo', 'marrakech', 'nairobi'}:
            return '🌍'
        else:
            return '🏙️'