import pandas as pd
import re
from collections import defaultdict

class TravelTipsClassifier:
    def __init__(self):
        # Define subcategories
        self.categories = {
            'General Travel Advice': 'general',
            'Solo Travel': 'solo', 
            'Budget Travel': 'budget'
        }
        
        # Level 1: Strong flair indicators
        self.flair_mapping = {
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
        
        # Level 4: Subreddit defaults
        self.subreddit_defaults = {
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
        
        # Special case: Question threads
        if any(indicator in title_lower for indicator in ['question', 'help', 'advice', 'recommend', 'suggest']):
            # Check if it's solo or budget specific
            if any(term in text_to_analyze for term in ['solo', 'alone', 'by myself']):
                return self.get_category_name('solo'), 'high'
            elif any(term in text_to_analyze for term in ['budget', 'cheap', 'money', 'cost']):
                return self.get_category_name('budget'), 'high'
            else:
                return self.get_category_name('general'), 'medium'
        
        # Level 2: Keyword matching (medium confidence)
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
        
        # Level 3: Context analysis (basic heuristics)
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
        
        # Level 4: Subreddit defaults
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'low'
        
        # Ultimate fallback
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
        print("=== TRAVEL TIPS CLASSIFICATION ANALYSIS ===")
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
        {"title": "Solo female travel to Thailand - safety tips?", "link_flair_text": None, "subreddit": "solotravel"},
        {"title": "Best budget airlines for Europe travel", "link_flair_text": "Budget", "subreddit": "budgettravel"}, 
        {"title": "Need visa advice for Japan travel", "link_flair_text": None, "subreddit": "travel"},
        {"title": "Traveling alone for the first time - nervous!", "link_flair_text": None, "subreddit": "travel"},
        {"title": "How to travel for $50/day in Southeast Asia", "link_flair_text": None, "subreddit": "shoestring"},
        {"title": "Ultimate packing guide for one-bag travel", "link_flair_text": "Guide", "subreddit": "onebag"}
    ]
    
    df_test = pd.DataFrame(sample_data)
    classifier = TravelTipsClassifier()
    
    print("=== TESTING TRAVEL TIPS CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], row['link_flair_text'], row['subreddit']
        )
        print(f"'{row['title']}' → {category} ({confidence} confidence)")

if __name__ == "__main__":
    test_classifier()