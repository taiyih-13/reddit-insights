import pandas as pd
import re
from collections import defaultdict

class FinanceClassifier:
    def __init__(self):
        # Define categories
        self.categories = {
            'Personal Trading Stories': 'personal',
            'Analysis & Education': 'analysis', 
            'Market News & Politics': 'news',
            'Memes & Entertainment': 'memes',
            'Questions & Help': 'questions',
            'Community Discussion': 'community'
        }
        
        # Level 1: Strong flair indicators
        self.flair_mapping = {
            'gain': 'personal',
            'loss': 'personal', 
            'yolo': 'personal',
            'meme': 'memes',
            'dd': 'analysis',
            'daily discussion': 'community',
            'discussion': 'community',
            'news': 'news',
            'markets': 'news',
            'question': 'questions',
            'help': 'questions',
            'advice': 'questions'
        }
        
        # Level 2: Keyword patterns
        self.keyword_patterns = {
            'personal': [
                # Trading results
                r'\$[\d,]+', r'gain', r'loss', r'profit', r'sold', r'bought', 
                r'yolo', r'portfolio', r'account', r'made \$', r'lost \$',
                # Personal milestones
                r'my first', r'finally', r'i did it', r'worst.*decision',
                r'quit.*job', r'full.?time.*trading', r'milestone'
            ],
            'analysis': [
                # Stock analysis
                r'\$[A-Z]{2,5}', r'dd', r'analysis', r'earnings', r'valuation',
                r'bullish', r'bearish', r'target price', r'pt:', r'catalyst',
                # Education
                r'strategy', r'how to', r'guide', r'tutorial', r'method',
                r'checklist', r'tips', r'lesson', r'learned'
            ],
            'news': [
                # Political/Economic
                r'trump', r'biden', r'fed', r'powell', r'interest rate',
                r'inflation', r'gdp', r'unemployment', r'congress',
                # Breaking news
                r'breaking', r'announces', r'reports', r'says:', r'according to',
                r'bloomberg', r'reuters', r'cnbc', r'wsj'
            ],
            'memes': [
                # Meme indicators
                r'🚀', r'💎', r'🦍', r'📈', r'📉', r'😂', r'💀', r'🤡',
                r'me when', r'me at', r'this is', r'that feel when',
                r'tfw', r'mood', r'relatable', r'too real'
            ],
            'questions': [
                # Question patterns
                r'\?', r'should i', r'what do you think', r'need advice',
                r'help', r'confused', r'beginner', r'new to', r'eli5',
                r'can someone explain', r'is it worth', r'recommend'
            ]
        }
        
        # Level 4: Subreddit defaults
        self.subreddit_defaults = {
            'wallstreetbets': 'personal',
            'investing': 'analysis', 
            'stocks': 'news',
            'SecurityAnalysis': 'analysis',
            'ValueInvesting': 'analysis',
            'options': 'analysis',
            'pennystocks': 'analysis',
            'daytrading': 'personal',
            'SwingTrading': 'personal',
            'forex': 'personal',
            'cryptocurrency': 'news',
            'Bitcoin': 'personal',
            'CryptoMarkets': 'analysis',
            'thetagang': 'analysis',
            'SPACs': 'analysis',
            'financialindependence': 'personal',
            'personalfinance': 'questions'
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
        
        # Special case: Daily discussion threads
        if 'daily discussion' in title_lower or 'what are your moves' in title_lower:
            return self.get_category_name('community'), 'high'
        
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
        # Long posts with detailed content likely analysis
        if len(selftext_lower) > 500 and any(word in text_to_analyze for word in ['because', 'analysis', 'think', 'believe']):
            return self.get_category_name('analysis'), 'low'
        
        # Posts with emotional language likely personal stories
        if any(word in text_to_analyze for word in ['worst', 'best', 'amazing', 'terrible', 'finally', 'omg']):
            return self.get_category_name('personal'), 'low'
        
        # Level 4: Subreddit defaults
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'low'
        
        # Ultimate fallback
        return self.get_category_name('community'), 'fallback'
    
    def get_category_name(self, category_key):
        """Convert category key to full name"""
        for full_name, key in self.categories.items():
            if key == category_key:
                return full_name
        return 'Community Discussion'
    
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
        print("=== CLASSIFICATION ANALYSIS ===")
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
        for category in category_counts.index[:6]:  # Top 6 categories
            examples = df_classified[df_classified['category'] == category]['title'].head(3)
            print(f"\n{category}:")
            for i, title in enumerate(examples):
                print(f"  {i+1}. {title[:70]}{'...' if len(title) > 70 else ''}")

def test_classifier():
    """Test the classifier with sample data"""
    sample_data = [
        {"title": "YOLO $50k into TSLA calls", "link_flair_text": "YOLO", "subreddit": "wallstreetbets"},
        {"title": "Should I invest in S&P 500?", "link_flair_text": None, "subreddit": "investing"}, 
        {"title": "Trump announces new tariffs on China", "link_flair_text": "News", "subreddit": "stocks"},
        {"title": "Me when I see my portfolio", "link_flair_text": "Meme", "subreddit": "wallstreetbets"},
        {"title": "DD: Why AAPL is undervalued", "link_flair_text": "DD", "subreddit": "stocks"},
        {"title": "Daily Discussion Thread", "link_flair_text": "Daily Discussion", "subreddit": "wallstreetbets"}
    ]
    
    df_test = pd.DataFrame(sample_data)
    classifier = FinanceClassifier()
    
    print("=== TESTING CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], row['link_flair_text'], row['subreddit']
        )
        print(f"'{row['title']}' → {category} ({confidence} confidence)")

if __name__ == "__main__":
    test_classifier()