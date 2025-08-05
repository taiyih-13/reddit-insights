import pandas as pd
import re
import os
import sys
from collections import defaultdict

# Add utils to path for sentiment analyzer
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from travel_sentiment_analyzer import TravelSentimentAnalyzer

class TravelClassifier:
    def __init__(self):
        # Initialize travel sentiment analyzer
        self.sentiment_analyzer = TravelSentimentAnalyzer()
        
        # Define unified travel subcategories
        self.categories = {
            # Regional categories
            'Europe': 'europe',
            'Asia': 'asia', 
            'Americas': 'americas',
            'Oceania & Africa': 'oceania_africa',
            # Travel tips categories
            'General Travel Advice': 'general',
            'Solo Travel': 'solo', 
            'Budget Travel': 'budget'
        }
        
        # Level 1: Strong flair indicators
        self.flair_mapping = {
            # Regional flair mapping
            'europe': 'europe',
            'asia': 'asia',
            'america': 'americas',
            'oceania': 'oceania_africa',
            'africa': 'oceania_africa',
            'italy': 'europe',
            'germany': 'europe',
            'japan': 'asia',
            'thailand': 'asia',
            'india': 'asia',
            'china': 'asia',
            'mexico': 'americas',
            'canada': 'americas',
            'australia': 'oceania_africa',
            'new zealand': 'oceania_africa',
            # Travel tips flair mapping
            'solo': 'solo',
            'budget': 'budget',
            'cheap': 'budget',
            'backpack': 'budget',
            'advice': 'general',
            'tips': 'general',
            'help': 'general',
            'question': 'general',
            'planning': 'general',
            'itinerary': 'general',
            'guide': 'general'
        }
        
        # Level 2: Keyword patterns
        self.keyword_patterns = {
            # Regional keyword patterns
            'europe': [
                # Western Europe
                r'italy', r'france', r'spain', r'germany', r'uk', r'britain', r'england',
                r'scotland', r'ireland', r'netherlands', r'belgium', r'switzerland',
                r'austria', r'portugal', r'norway', r'sweden', r'denmark', r'finland',
                # Eastern Europe
                r'poland', r'czech', r'hungary', r'romania', r'bulgaria', r'croatia',
                r'serbia', r'slovakia', r'slovenia', r'estonia', r'latvia', r'lithuania',
                # General European terms
                r'europe', r'european', r'eu ', r'schengen', r'eurail', r'interrail'
            ],
            'asia': [
                # East Asia
                r'japan', r'china', r'korea', r'south korea', r'north korea', r'taiwan',
                r'hong kong', r'macau', r'mongolia',
                # Southeast Asia
                r'thailand', r'vietnam', r'cambodia', r'laos', r'myanmar', r'singapore',
                r'malaysia', r'indonesia', r'philippines', r'brunei',
                # South Asia
                r'india', r'pakistan', r'bangladesh', r'sri lanka', r'nepal', r'bhutan',
                r'maldives', r'afghanistan',
                # Central/West Asia
                r'kazakhstan', r'uzbekistan', r'turkmenistan', r'kyrgyzstan', r'tajikistan',
                r'iran', r'iraq', r'turkey', r'georgia', r'armenia', r'azerbaijan',
                # General Asian terms
                r'asia', r'asian', r'southeast asia', r'south asia', r'east asia'
            ],
            'americas': [
                # North America
                r'usa', r'united states', r'america', r'canada', r'mexico',
                # Central America
                r'guatemala', r'belize', r'honduras', r'el salvador', r'nicaragua',
                r'costa rica', r'panama',
                # South America
                r'brazil', r'argentina', r'chile', r'colombia', r'venezuela', r'peru',
                r'ecuador', r'bolivia', r'paraguay', r'uruguay', r'guyana', r'suriname',
                # Caribbean
                r'cuba', r'jamaica', r'haiti', r'dominican republic', r'puerto rico',
                r'bahamas', r'barbados', r'trinidad',
                # General terms
                r'latin america', r'south america', r'north america', r'central america'
            ],
            'oceania_africa': [
                # Oceania
                r'australia', r'new zealand', r'fiji', r'papua new guinea', r'samoa',
                r'tonga', r'vanuatu', r'solomon islands', r'micronesia', r'palau',
                # Africa
                r'south africa', r'egypt', r'morocco', r'tunisia', r'kenya', r'tanzania',
                r'uganda', r'rwanda', r'ethiopia', r'ghana', r'nigeria', r'senegal',
                r'mali', r'burkina faso', r'ivory coast', r'cameroon', r'gabon',
                r'congo', r'zambia', r'zimbabwe', r'botswana', r'namibia', r'madagascar',
                # General terms
                r'africa', r'african', r'oceania', r'pacific islands'
            ],
            # Travel tips keyword patterns
            'solo': [
                # Solo-specific terms
                r'solo travel', r'traveling alone', r'alone', r'by myself', r'first time solo',
                r'solo female', r'solo male', r'safety.*solo', r'solo.*safe',
                # Solo concerns
                r'lonely', r'meeting people', r'making friends', r'social', r'single traveler',
                r'on my own', r'by yourself', r'solo.*experience', r'solo.*trip'
            ],
            'budget': [
                # Money/cost terms
                r'\$[\d,]+', r'budget', r'cheap', r'affordable', r'cost', r'expensive',
                r'broke', r'money', r'save.*money', r'shoestring', r'hostels',
                # Budget strategies
                r'free', r'discount', r'deal', r'coupon', r'sale', r'bargain',
                r'backpack', r'hitchhike', r'couch.*surf', r'work.*exchange',
                r'volunteer', r'wwoof', r'budget.*airline', r'low.*cost'
            ],
            'general': [
                # Planning and advice
                r'itinerary', r'planning', r'advice', r'tips', r'guide', r'help',
                r'recommend', r'suggest', r'best.*way', r'how.*to', r'what.*do',
                # General travel terms
                r'visa', r'passport', r'flight', r'hotel', r'accommodation',
                r'transportation', r'insurance', r'packing', r'luggage',
                # Travel logistics
                r'airport', r'border', r'customs', r'embassy', r'document',
                r'currency', r'exchange', r'sim.*card', r'wifi', r'roaming'
            ]
        }
        
        # Level 4: Subreddit defaults (most specific)
        self.subreddit_defaults = {
            # Regional subreddits
            'europe': 'europe',
            'travel_Europe': 'europe',
            'ItalyTravel': 'europe',
            'germany': 'europe',
            'JapanTravel': 'asia',
            'Thailand': 'asia',
            'IndiaTravel': 'asia',
            'SouthEastAsia': 'asia',
            'china': 'asia',
            'MexicoTravel': 'americas',
            'canada': 'americas',
            'VisitingIceland': 'europe',  # Iceland is in Europe
            'argentina': 'americas',
            'australia': 'oceania_africa',
            'newzealand': 'oceania_africa',
            'southafrica': 'oceania_africa',
            # Travel tips subreddits
            'travel': 'general',
            'TravelNoPics': 'general',
            'travelhacks': 'general',
            'onebag': 'general',
            'digitalnomad': 'general',
            'longtermtravel': 'general',
            'solotravel': 'solo',
            'backpacking': 'budget',
            'budgettravel': 'budget',
            'shoestring': 'budget'
        }
    
    def classify_post(self, title, flair=None, subreddit=None, selftext=""):
        """
        Classify a single post using hierarchical logic
        Returns: (category_name, confidence_level)
        """
        title_lower = title.lower()
        flair_lower = str(flair).lower() if flair else ""
        selftext_lower = str(selftext).lower() if pd.notnull(selftext) else ""
        text_to_analyze = f"{title_lower} {selftext_lower}"
        
        # Level 1: Check flair first (highest confidence)
        if flair_lower:
            for flair_key, category in self.flair_mapping.items():
                if flair_key in flair_lower:
                    return self.get_category_name(category), 'high'
        
        # Level 2: Subreddit defaults (very high confidence for specific subreddits)
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'high'
        
        # Special case: Question threads for travel tips
        if any(indicator in title_lower for indicator in ['question', 'help', 'advice', 'recommend', 'suggest']):
            # Check if it's solo or budget specific
            if any(term in text_to_analyze for term in ['solo', 'alone', 'by myself']):
                return self.get_category_name('solo'), 'high'
            elif any(term in text_to_analyze for term in ['budget', 'cheap', 'money', 'cost']):
                return self.get_category_name('budget'), 'high'
        
        # Level 3: Keyword matching (medium confidence)
        category_scores = defaultdict(int)
        
        for category, patterns in self.keyword_patterns.items():
            for pattern in patterns:
                matches = len(re.findall(pattern, text_to_analyze, re.IGNORECASE))
                category_scores[category] += matches
        
        # If we found keyword matches, return highest scoring category
        if category_scores:
            best_category = max(category_scores.items(), key=lambda x: x[1])
            if best_category[1] > 0:
                return self.get_category_name(best_category[0]), 'medium'
        
        # Level 4: Context analysis (basic heuristics)
        # Check for currency mentions (regional classification)
        currency_mapping = {
            'euro': 'europe',
            'eur': 'europe',
            'pound': 'europe',
            'gbp': 'europe',
            'yen': 'asia',
            'yuan': 'asia',
            'rupee': 'asia',
            'baht': 'asia',
            'dollar': 'americas',  # Could be multiple regions but often Americas
            'peso': 'americas',
            'real': 'americas'
        }
        
        for currency, region in currency_mapping.items():
            if currency in text_to_analyze:
                return self.get_category_name(region), 'low'
        
        # Check for language mentions
        language_mapping = {
            'spanish': 'americas',
            'portuguese': 'americas',
            'french': 'europe',
            'german': 'europe',
            'italian': 'europe',
            'japanese': 'asia',
            'chinese': 'asia',
            'thai': 'asia',
            'hindi': 'asia',
            'arabic': 'oceania_africa'
        }
        
        for language, region in language_mapping.items():
            if language in text_to_analyze:
                return self.get_category_name(region), 'low'
        
        # Posts mentioning specific dollar amounts likely budget-related
        if re.search(r'\$[\d,]+', text_to_analyze):
            return self.get_category_name('budget'), 'low'
        
        # Posts with safety concerns often solo travel related
        if any(word in text_to_analyze for word in ['safe', 'safety', 'dangerous', 'secure', 'risk']):
            if any(word in text_to_analyze for word in ['alone', 'solo', 'myself']):
                return self.get_category_name('solo'), 'low'
        
        # Long detailed posts likely general advice
        if len(selftext_lower) > 300:
            return self.get_category_name('general'), 'low'
        
        # Ultimate fallback - default to General Travel Advice
        return self.get_category_name('general'), 'fallback'
    
    def get_category_name(self, category_key):
        """Convert category key to full name"""
        for full_name, key in self.categories.items():
            if key == category_key:
                return full_name
        return 'General Travel Advice'
    
    def classify_dataframe(self, df):
        """Classify all posts in a dataframe"""
        results = []
        sentiment_results = []
        
        for idx, row in df.iterrows():
            category, confidence = self.classify_post(
                title=row['title'],
                flair=row.get('link_flair_text'),
                subreddit=row.get('subreddit'),
                selftext=row.get('selftext', '')
            )
            results.append({
                'category': category,
                'confidence': confidence
            })
            
            # Travel destination sentiment analysis
            sentiment_data = self.sentiment_analyzer.analyze_destination_mentions(
                title=row['title'],
                selftext=row.get('selftext', '')
            )
            sentiment_results.append(sentiment_data)
        
        # Add results to dataframe
        df_classified = df.copy()
        df_classified['category'] = [r['category'] for r in results]
        df_classified['classification_confidence'] = [r['confidence'] for r in results]
        
        # Add travel sentiment columns
        df_classified['travel_destinations'] = [s['destinations'] for s in sentiment_results]
        df_classified['sentiment_score'] = [s['sentiment_score'] for s in sentiment_results]
        df_classified['sentiment_label'] = [s['sentiment_label'] for s in sentiment_results]
        
        return df_classified
    
    def analyze_classification(self, df_classified):
        """Analyze classification results"""
        print("=== TRAVEL CLASSIFICATION ANALYSIS ===")
        print(f"Total posts classified: {len(df_classified)}")
        
        # Category distribution
        category_counts = df_classified['category'].value_counts()
        print(f"\nCategory Distribution:")
        for category, count in category_counts.items():
            percentage = (count / len(df_classified)) * 100
            print(f"  {category}: {count} posts ({percentage:.1f}%)")
        
        # Confidence distribution
        confidence_counts = df_classified['classification_confidence'].value_counts()
        print(f"\nConfidence Distribution:")
        for confidence, count in confidence_counts.items():
            percentage = (count / len(df_classified)) * 100
            print(f"  {confidence}: {count} posts ({percentage:.1f}%)")
        
        # Show examples from each category
        print(f"\nExamples from each category:")
        for category in category_counts.index:
            examples = df_classified[df_classified['category'] == category]['title'].head(3)
            print(f"\n{category}:")
            for i, title in enumerate(examples):
                print(f"  {i+1}. {title[:70]}{'...' if len(title) > 70 else ''}")

def test_classifier():
    """Test the classifier with sample data"""
    sample_data = [
        # Regional tests
        {"title": "Two weeks in Italy - Rome, Florence, Venice itinerary", "link_flair_text": None, "subreddit": "ItalyTravel"},
        {"title": "Budget backpacking through Thailand and Vietnam", "link_flair_text": None, "subreddit": "Thailand"}, 
        {"title": "Road trip across Canada - best national parks?", "link_flair_text": None, "subreddit": "canada"},
        {"title": "Solo travel to Japan - cultural etiquette tips", "link_flair_text": None, "subreddit": "JapanTravel"},
        {"title": "Overland travel from Cairo to Cape Town", "link_flair_text": None, "subreddit": "travel"},
        {"title": "Working holiday visa for Australia and New Zealand", "link_flair_text": None, "subreddit": "australia"},
        # Travel tips tests
        {"title": "Solo female travel to Thailand - safety tips?", "link_flair_text": None, "subreddit": "solotravel"},
        {"title": "Best budget airlines for Europe travel", "link_flair_text": "Budget", "subreddit": "budgettravel"}, 
        {"title": "Need visa advice for Japan travel", "link_flair_text": None, "subreddit": "travel"},
        {"title": "Traveling alone for the first time - nervous!", "link_flair_text": None, "subreddit": "travel"},
        {"title": "How to travel for $50/day in Southeast Asia", "link_flair_text": None, "subreddit": "shoestring"},
        {"title": "Ultimate packing guide for one-bag travel", "link_flair_text": "Guide", "subreddit": "onebag"}
    ]
    
    df_test = pd.DataFrame(sample_data)
    classifier = TravelClassifier()
    
    print("=== TESTING UNIFIED TRAVEL CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], row['link_flair_text'], row['subreddit']
        )
        print(f"'{row['title']}' → {category} ({confidence} confidence)")

if __name__ == "__main__":
    test_classifier()