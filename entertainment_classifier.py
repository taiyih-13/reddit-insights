import pandas as pd
import re
from collections import defaultdict

class EntertainmentClassifier:
    def __init__(self):
        # Define 10 entertainment categories  
        self.categories = {
            'Drama': 'drama',
            'Comedy': 'comedy',
            'Action': 'action', 
            'Horror': 'horror',
            'Sci-Fi': 'scifi',
            'Fantasy': 'fantasy',
            'Romance': 'romance',
            'Anime': 'anime',
            'Documentary': 'documentary',
            'Bad Movies': 'badmovies'
        }
        
        # Level 1: Strong flair indicators
        self.flair_mapping = {
            'horror': 'horror',
            'comedy': 'comedy',
            'drama': 'drama',
            'action': 'action',
            'sci-fi': 'scifi',
            'science fiction': 'scifi',
            'fantasy': 'fantasy',
            'romance': 'romance',
            'anime': 'anime',
            'documentary': 'documentary',
            'bad movie': 'badmovies',
            'terrible': 'badmovies',
            'worst': 'badmovies'
        }
        
        # Level 2: Keyword patterns for genre detection
        self.keyword_patterns = {
            'drama': [
                # Drama keywords
                r'drama', r'dramatic', r'emotional', r'heartbreaking', r'touching',
                r'family.*drama', r'character.*driven', r'deep.*story', r'powerful.*story',
                # Drama shows/movies
                r'the crown', r'succession', r'breaking bad', r'better call saul'
            ],
            'comedy': [
                # Comedy keywords  
                r'comedy', r'funny', r'hilarious', r'laugh', r'humor', r'humour',
                r'sitcom', r'stand.*up', r'parody', r'satire', r'comedic',
                # Comedy shows/movies
                r'the office', r'parks.*rec', r'brooklyn.*nine', r'friends',
                r'guardians.*galaxy', r'deadpool', r'thor.*ragnarok', r'community',
                r'arrested development', r'veep', r'always sunny', r'seinfeld'
            ],
            'action': [
                # Action keywords
                r'action', r'fight', r'explosion', r'chase', r'martial.*arts',
                r'superhero', r'adventure', r'thriller', r'fast.*paced', r'adrenaline',
                # MCU and action franchises
                r'marvel', r'mcu', r'avengers', r'batman', r'john wick', r'mission.*impossible',
                r'guardians.*galaxy', r'fast.*furious', r'james bond', r'die hard',
                r'jason bourne', r'mad max', r'indiana jones'
            ],
            'horror': [
                # Horror keywords
                r'horror', r'scary', r'terrifying', r'nightmare', r'creepy', r'disturbing',
                r'slasher', r'zombie', r'ghost', r'haunted', r'demon', r'supernatural',
                # Horror franchises
                r'saw', r'conjuring', r'insidious', r'paranormal.*activity', r'halloween',
                r'friday.*13th', r'nightmare.*elm', r'scream', r'evil dead',
                r'the ring', r'it follows', r'hereditary', r'midsommar'
            ],
            'scifi': [
                # Sci-fi keywords
                r'sci.*fi', r'science.*fiction', r'space', r'alien', r'futuristic', r'dystopian',
                r'time.*travel', r'robot', r'ai', r'cyberpunk', r'spaceship', r'galaxy',
                # Sci-fi franchises
                r'star wars', r'star trek', r'blade runner', r'matrix', r'terminator',
                r'stranger things', r'black mirror', r'westworld', r'altered carbon',
                r'the expanse', r'interstellar', r'dune', r'foundation'
            ],
            'fantasy': [
                # Fantasy keywords
                r'fantasy', r'magic', r'wizard', r'dragon', r'medieval', r'kingdom',
                r'supernatural', r'mythical', r'epic.*fantasy', r'sword.*sorcery',
                # Fantasy franchises
                r'game.*thrones', r'lord.*rings', r'harry potter', r'witcher', r'vikings'
            ],
            'romance': [
                # Romance keywords
                r'romance', r'romantic', r'love.*story', r'relationship', r'dating',
                r'wedding', r'couple', r'heartwarming', r'feel.*good', r'love.*triangle',
                # Romance shows/movies
                r'bridgerton', r'outlander', r'bachelor', r'love.*actually'
            ],
            'anime': [
                # Anime keywords
                r'anime', r'manga', r'japanese.*animation', r'otaku', r'weeb',
                r'studio.*ghibli', r'crunchyroll', r'funimation', r'dub', r'sub',
                # Popular anime
                r'naruto', r'one piece', r'attack.*titan', r'demon slayer', r'dragon ball',
                r'death note', r'fullmetal.*alchemist', r'hunter.*hunter', r'bleach',
                r'my hero academia', r'jujutsu kaisen', r'chainsaw man', r'mob psycho'
            ],
            'documentary': [
                # Documentary keywords  
                r'documentary', r'docuseries', r'real.*story', r'true.*story', r'based.*true',
                r'investigative', r'nature.*documentary', r'biography', r'historical',
                # Documentary platforms/creators
                r'national geographic', r'bbc', r'ken burns', r'david attenborough'
            ],
            'badmovies': [
                # Bad movie keywords
                r'bad.*movie', r'worst.*movie', r'terrible', r'awful', r'so.*bad.*good',
                r'guilty.*pleasure', r'cringe', r'flop', r'disaster', r'trainwreck',
                # Known bad movie franchises
                r'sharknado', r'the room', r'birdemic', r'plan 9',
                r'twilight', r'fifty shades', r'cats.*movie', r'movie 43',
                r'batman.*robin', r'green lantern', r'fantastic four'
            ]
        }
        
        
        # Level 4: Subreddit defaults
        self.subreddit_defaults = {
            'netflix': 'drama',  # Most Netflix content is drama
            'hulu': 'comedy',
            'DisneyPlus': 'fantasy',
            'PrimeVideo': 'action', 
            'HBOMax': 'drama',
            'AppleTVPlus': 'drama',
            'movies': 'drama',  # Default for general movie discussions
            'television': 'drama',  # Default for general TV discussions
            'letterboxd': 'drama',  # Film review site, mostly serious films
            'anime': 'anime',
            'animesuggest': 'anime',
            'horror': 'horror',
            'horrormovies': 'horror',
            'MovieSuggestions': 'drama',  # Default to drama for recommendations
            'televisionsuggestions': 'drama',
            'NetflixBestOf': 'drama',
            'documentaries': 'documentary'
        }
    
    def classify_post(self, title, flair=None, subreddit=None, selftext=""):
        """
        Classify a single entertainment post using hierarchical logic
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
        
        # Level 2: Keyword matching with priority system (medium confidence)
        category_scores = defaultdict(int)
        
        # Genre priority order (higher priority genres override lower ones)
        priority_order = ['anime', 'documentary', 'badmovies', 'horror', 'scifi', 
                         'fantasy', 'action', 'comedy', 'romance', 'drama']
        
        for category, patterns in self.keyword_patterns.items():
            for pattern in patterns:
                matches = len(re.findall(pattern, text_to_analyze, re.IGNORECASE))
                category_scores[category] += matches
        
        # Apply priority weighting - earlier genres get higher weights
        weighted_scores = {}
        for category, score in category_scores.items():
            if score > 0:
                try:
                    priority_weight = len(priority_order) - priority_order.index(category)
                    weighted_scores[category] = score * priority_weight
                except ValueError:
                    weighted_scores[category] = score  # If not in priority list, use raw score
        
        # Return highest weighted category
        if weighted_scores:
            best_category = max(weighted_scores.items(), key=lambda x: x[1])
            return self.get_category_name(best_category[0]), 'medium'
        
        # Level 3: Content analysis (basic heuristics)
        # Check for sentiment/opinion indicators
        if not self._has_opinion_indicators(text_to_analyze):
            # If no clear opinion, might not be suitable for our sentiment-focused approach
            return self.get_category_name('drama'), 'low_opinion'
            
        # Level 4: Subreddit defaults
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'low'
        
        # Ultimate fallback
        return self.get_category_name('drama'), 'fallback'
    
    def _has_opinion_indicators(self, text):
        """Check if text contains opinion/sentiment indicators"""
        opinion_patterns = [
            # Strong positive opinions
            r'amazing', r'brilliant', r'fantastic', r'excellent', r'outstanding', r'superb',
            r'phenomenal', r'incredible', r'awesome', r'stellar', r'epic', r'masterpiece',
            r'flawless', r'perfect', r'gorgeous', r'stunning', r'captivating', r'engaging',
            r'compelling', r'riveting', r'mind.*blowing', r'jaw.*dropping', r'breathtaking',
            
            # Strong negative opinions  
            r'terrible', r'awful', r'horrible', r'dreadful', r'atrocious', r'abysmal',
            r'pathetic', r'disappointing', r'boring', r'tedious', r'bland', r'mediocre',
            r'trash', r'garbage', r'unwatchable', r'cringe', r'painful', r'waste.*time',
            r'trainwreck', r'disaster', r'nightmare', r'torture', r'unbearable',
            
            # Mild positive opinions
            r'good', r'nice', r'decent', r'okay', r'fine', r'solid', r'enjoyable',
            r'pleasant', r'watchable', r'entertaining', r'fun', r'cool', r'sweet',
            r'pretty good', r'not bad', r'worth.*watch', r'liked it', r'enjoyed',
            
            # Mild negative opinions
            r'bad', r'poor', r'weak', r'lacking', r'forgettable', r'meh', r'subpar',
            r'disappointing', r'overrated', r'overhyped', r'letdown', r'failed',
            r'missed.*mark', r'fell.*flat', r'could.*better',
            
            # Comparative opinions
            r'best', r'worst', r'better than', r'worse than', r'superior', r'inferior',
            r'outshines', r'surpasses', r'falls short', r'pales.*comparison',
            
            # Recommendation language
            r'recommend', r'suggest', r'must watch', r'should watch', r'check out',
            r'give.*try', r'worth.*time', r'avoid', r'skip', r'stay.*away',
            r'don\'t.*bother', r'save.*time', r'highly.*recommend',
            
            # Personal preferences
            r'love', r'hate', r'adore', r'despise', r'can\'t.*stand', r'obsessed',
            r'favorite', r'favourite', r'least.*favorite', r'prefer', r'bias',
            
            # Emotional reactions
            r'crying', r'tears', r'laughed', r'scared', r'terrified', r'bored.*tears',
            r'edge.*seat', r'heart.*racing', r'goosebumps', r'chills', r'emotional',
            r'moved', r'touched', r'disturbed', r'shocked', r'surprised',
            
            # Quality assessments
            r'overrated', r'underrated', r'underappreciated', r'overpraised', r'overhyped',
            r'lives.*hype', r'exceeds.*expectations', r'fell.*short', r'disappoints',
            
            # Intensity modifiers
            r'extremely', r'incredibly', r'absolutely', r'totally', r'completely',
            r'utterly', r'ridiculously', r'insanely', r'surprisingly', r'unexpectedly',
            
            # Question-based opinions (seeking/giving recommendations)
            r'what.*think', r'thoughts on', r'opinions on', r'worth.*watching',
            r'should.*watch', r'recommend.*similar', r'any.*good', r'hidden.*gem',
            r'guilty.*pleasure', r'comfort.*watch'
        ]
        
        for pattern in opinion_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    
    def get_category_name(self, category_key):
        """Convert category key to full name"""
        for full_name, key in self.categories.items():
            if key == category_key:
                return full_name
        return 'Drama'  # Default fallback
    
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
        print("=== ENTERTAINMENT CLASSIFICATION ANALYSIS ===")
        print(f"Total posts classified: {len(df_classified)}")
        
        # Category distribution
        category_counts = df_classified['category'].value_counts()
        print(f"\nGenre Distribution:")
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
        print(f"\nExamples from each genre:")
        for category in category_counts.index[:6]:  # Top 6 categories
            examples = df_classified[df_classified['category'] == category]['title'].head(2)
            print(f"\n{category}:")
            for i, title in enumerate(examples):
                print(f"  {i+1}. {title[:80]}{'...' if len(title) > 80 else ''}")

def test_entertainment_classifier():
    """Test the entertainment classifier with sample data"""
    sample_data = [
        {"title": "The Conjuring is terrifying and available on Netflix", "link_flair_text": None, "subreddit": "horror"},
        {"title": "Best anime recommendations for beginners", "link_flair_text": None, "subreddit": "animesuggest"}, 
        {"title": "Marvel's new superhero movie on Disney+ is amazing", "link_flair_text": None, "subreddit": "movies"},
        {"title": "Worst movies that are actually entertaining to watch", "link_flair_text": None, "subreddit": "badMovies"},
        {"title": "Romantic comedies on Hulu worth watching", "link_flair_text": "Comedy", "subreddit": "hulu"},
        {"title": "David Attenborough's latest nature documentary", "link_flair_text": None, "subreddit": "documentaries"}
    ]
    
    df_test = pd.DataFrame(sample_data)
    classifier = EntertainmentClassifier()
    
    print("=== TESTING ENTERTAINMENT CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], row['link_flair_text'], row['subreddit']
        )
        print(f"'{row['title']}' → {category} ({confidence})")

if __name__ == "__main__":
    test_entertainment_classifier()