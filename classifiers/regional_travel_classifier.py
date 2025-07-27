import pandas as pd
import re
from collections import defaultdict

class RegionalTravelClassifier:
    def __init__(self):
        # Define regional subcategories
        self.categories = {
            'Europe': 'europe',
            'Asia': 'asia', 
            'Americas': 'americas',
            'Oceania & Africa': 'oceania_africa'
        }
        
        # Level 1: Strong flair indicators
        self.flair_mapping = {
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
            'new zealand': 'oceania_africa'
        }
        
        # Level 2: Country/region keyword patterns
        self.keyword_patterns = {
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
            ]
        }
        
        # Level 4: Subreddit defaults (most specific)
        self.subreddit_defaults = {
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
            'southafrica': 'oceania_africa'
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
        
        # Level 2: Subreddit defaults (very high confidence for regional subreddits)
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'high'
        
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
        # Check for currency mentions
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
        
        # Ultimate fallback - default to Asia as it's the largest travel category
        return self.get_category_name('asia'), 'fallback'
    
    def get_category_name(self, category_key):
        """Convert category key to full name"""
        for full_name, key in self.categories.items():
            if key == category_key:
                return full_name
        return 'Asia'
    
    def classify_dataframe(self, df):
        """Classify all posts in a dataframe"""
        results = []
        
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
        
        # Add results to dataframe
        df_classified = df.copy()
        df_classified['category'] = [r['category'] for r in results]
        df_classified['classification_confidence'] = [r['confidence'] for r in results]
        
        return df_classified
    
    def analyze_classification(self, df_classified):
        """Analyze classification results"""
        print("=== REGIONAL TRAVEL CLASSIFICATION ANALYSIS ===")
        print(f"Total posts classified: {len(df_classified)}")
        
        # Category distribution
        category_counts = df_classified['category'].value_counts()
        print(f"\nRegional Distribution:")
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
        print(f"\nExamples from each region:")
        for category in category_counts.index:
            examples = df_classified[df_classified['category'] == category]['title'].head(3)
            print(f"\n{category}:")
            for i, title in enumerate(examples):
                print(f"  {i+1}. {title[:70]}{'...' if len(title) > 70 else ''}")

def test_classifier():
    """Test the classifier with sample data"""
    sample_data = [
        {"title": "Two weeks in Italy - Rome, Florence, Venice itinerary", "link_flair_text": None, "subreddit": "ItalyTravel"},
        {"title": "Budget backpacking through Thailand and Vietnam", "link_flair_text": None, "subreddit": "Thailand"}, 
        {"title": "Road trip across Canada - best national parks?", "link_flair_text": None, "subreddit": "canada"},
        {"title": "Solo travel to Japan - cultural etiquette tips", "link_flair_text": None, "subreddit": "JapanTravel"},
        {"title": "Overland travel from Cairo to Cape Town", "link_flair_text": None, "subreddit": "travel"},
        {"title": "Working holiday visa for Australia and New Zealand", "link_flair_text": None, "subreddit": "australia"}
    ]
    
    df_test = pd.DataFrame(sample_data)
    classifier = RegionalTravelClassifier()
    
    print("=== TESTING REGIONAL TRAVEL CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], row['link_flair_text'], row['subreddit']
        )
        print(f"'{row['title']}' → {category} ({confidence} confidence)")

if __name__ == "__main__":
    test_classifier()