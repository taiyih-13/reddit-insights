import pandas as pd
import re
from collections import defaultdict
import sys
import os

# Add utils to path for sentiment analyzer
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from optimized_entertainment_sentiment_analyzer import OptimizedEntertainmentSentimentAnalyzer

class EntertainmentClassifier:
    def __init__(self):
        # Initialize optimized entertainment sentiment analyzer  
        self.sentiment_analyzer = OptimizedEntertainmentSentimentAnalyzer()
        # Define discussion-type categories  
        self.categories = {
            'Recommendation Requests': 'recommendations',
            'Reviews & Discussions': 'reviews_discussions', 
            'News & Announcements': 'news_announcements',
            'Identification & Help': 'identification_help',
            'Lists & Rankings': 'lists_rankings'
        }
        
        # Layer 1: Direct subreddit to discussion-type mapping (highest confidence)
        self.direct_subreddit_mapping = {
            'animesuggest': 'recommendations',  # Anime recommendation requests
            'MovieSuggestions': 'recommendations',  # Movie recommendation requests  
            'televisionsuggestions': 'recommendations',  # TV recommendation requests
            'NetflixBestOf': 'recommendations',  # Netflix recommendations
            'ifyoulikeblank': 'recommendations',  # "If you like X, try Y" recommendations
            'tipofmytongue': 'identification_help',  # "What movie is this?" identification
            'criterion': 'lists_rankings',  # Criterion Collection rankings/discussions
            'truefilm': 'lists_rankings',  # Film canon and serious rankings
            'flicks': 'reviews_discussions',  # Film reviews and discussions
        }
        
        # Layer 2: Flair indicators for discussion types
        self.flair_mapping = {
            'request': 'recommendations',
            'suggestion': 'recommendations', 
            'discussion': 'reviews_discussions',
            'review': 'reviews_discussions',
            'news': 'news_announcements',
            'announcement': 'news_announcements',
            'trailer': 'news_announcements',
            'help': 'identification_help',
            'list': 'lists_rankings'
        }
        
        # Layer 3: Discussion-type detection patterns
        self.discussion_patterns = {
            'recommendations': [
                # Direct recommendation requests
                r'\\b(?:recommend|suggestion|suggest)\\b',
                r'what.*(?:should|to).*watch',
                r'(?:need|want|looking\\s+for).*(?:movie|show|series|anime)',
                r'give.*me.*(?:your|some).*favorite',
                r'\\[request\\]',
                r'best.*(?:movie|show|series|anime).*(?:for|to|on)',
                r'any.*good.*(?:movie|show|series|anime)',
                r'hidden.*gem',
                r'underrated.*(?:movie|show|series|anime)',
                r'never.*watched.*(?:anime|genre)',
                r'new.*to.*(?:anime|horror|sci.*fi)',
                r'similar.*to.*(?:this|that)',
                r'like.*(?:this|that).*(?:movie|show)',
                r'(?:beginner|starter).*(?:anime|movie|show)',
                r'feel.*good.*(?:movie|show)',
                r'binge.*watch.*(?:recommendation|suggestion)'
            ],
            
            'reviews_discussions': [
                # Personal reviews and opinions
                r'(?:just|finally).*(?:finished|watched|binged)',
                r'(?:loved|hated|enjoyed|disappointed)',
                r'(?:amazing|terrible|brilliant|awful|meh)',
                r'\\bomfg\\b',
                r'(?:my|personal).*(?:opinion|review|thoughts?)',
                r'(?:exceeded|disappointed).*expectation',
                r'made.*me.*(?:cry|laugh|emotional)',
                
                # Discussion prompts
                r'what.*(?:do\\s+you\\s+think|your\\s+opinion)',
                r'(?:best|worst|favorite).*(?:season|character|episode)',
                r'which.*(?:is|do\\s+you).*(?:better|prefer)',
                r'\\[discussion\\]',
                r'what.*(?:scene|moment|part).*(?:favorite|memorable)',
                r'who.*(?:else|thinks|agrees)',
                r'am.*i.*the.*only.*one',
                r'unpopular.*opinion',
                r'hot.*take',
                r'change.*my.*mind'
            ],
            
            'news_announcements': [
                # Release and scheduling news
                r'(?:delayed|postponed|moved).*(?:to|until)',
                r'(?:official|new).*(?:poster|trailer|teaser)',
                r'(?:season|series).*(?:\\d+|two|three).*(?:announcement|confirmed)',
                r'(?:cast|casting|star|starring).*(?:announced|confirmed)',
                r'(?:premiere|release).*(?:date|schedule)',
                r'\\|\\|.*(?:trailer|teaser|poster)',
                r'has.*made.*(?:history|record)',
                r'(?:filming|production).*(?:starts|begins|wraps)',
                r'(?:renewed|cancelled|greenlit)',
                r'first.*(?:look|image|poster)',
                r'breaking.*news',
                r'just.*announced'
            ],
            
            'identification_help': [
                # Content identification
                r'(?:need.*to.*find|trying.*to.*find)',
                r'what.*(?:movie|show|series|anime).*(?:is.*this|this.*is)',
                r'(?:similar|like).*(?:to|this).*(?:movie|show|series)',
                r'what.*(?:films|movies|shows).*(?:have.*this|this.*vibe)',
                r'remind.*me.*of',
                r'same.*(?:vibe|energy|feel)',
                r'movies.*(?:like|similar)',
                r'can.*t.*remember.*(?:movie|show)',
                r'help.*me.*(?:find|remember|identify)',
                r'what.*was.*that.*(?:movie|show)',
                r'tip.*of.*my.*tongue'
            ],
            
            'lists_rankings': [
                # Lists and rankings
                r'(?:my|top|best).*(?:\\d+|three|five|ten).*(?:favorite|best)',
                r'(?:greatest|best).*(?:of.*all.*time|ever)',
                r'(?:underrated|overrated).*(?:list|collection)',
                r'(?:classic|old).*(?:movie|film).*(?:everyone|should)',
                r'(?:films|movies).*(?:that.*i|everyone).*(?:wish|should)',
                r'(?:ranking|list).*(?:of|my)',  
                r'collection.*of',
                r'tier.*list',
                r'\\d+.*(?:best|worst|favorite).*(?:movie|show|anime)',
                r'every.*(?:movie|show).*(?:ranked|rating)',
                r'definitive.*(?:list|ranking)'
            ]
        }
        
        # Layer 4: General discussion defaults by subreddit
        self.subreddit_defaults = {
            'netflix': 'reviews_discussions',
            'hulu': 'reviews_discussions', 
            'DisneyPlus': 'reviews_discussions',
            'PrimeVideo': 'reviews_discussions',
            'HBOMax': 'reviews_discussions',
            'AppleTVPlus': 'reviews_discussions',
            'movies': 'news_announcements',  # Lots of movie news
            'television': 'news_announcements',  # Lots of TV news
            'letterboxd': 'reviews_discussions',  # Film review focused
            'anime': 'reviews_discussions',  # Anime discussions
            'horror': 'reviews_discussions',  # Horror discussions  
            'horrormovies': 'reviews_discussions',  # Horror discussions
            'documentaries': 'news_announcements'  # Documentary announcements
        }
    
    def classify_post(self, title, flair=None, subreddit=None, selftext=""):
        """
        Classify a single entertainment post using discussion-type logic
        Returns: (category_name, confidence_level) or (None, 'no_media') if no media detected
        """
        title_lower = title.lower()
        flair_lower = str(flair).lower() if flair else ""
        selftext_lower = str(selftext).lower() if pd.notnull(selftext) else ""
        text_to_analyze = f"{title_lower} {selftext_lower}"
        
        # Level 0: Media identification - filter out non-media posts
        if not self._contains_media_reference(text_to_analyze, subreddit):
            return None, 'no_media'
        
        # Level 1: Direct subreddit mapping (highest confidence)
        if subreddit in self.direct_subreddit_mapping:
            return self.get_category_name(self.direct_subreddit_mapping[subreddit]), 'high'
        
        # Level 2: Check flair (high confidence)
        if flair_lower:
            for flair_key, category in self.flair_mapping.items():
                if flair_key in flair_lower:
                    return self.get_category_name(category), 'high'
        
        # Level 3: Discussion pattern matching with priority (medium confidence)
        category_scores = defaultdict(int)
        
        # Priority order - more specific patterns first
        priority_order = ['recommendations', 'identification_help', 'lists_rankings',
                         'news_announcements', 'reviews_discussions']
        
        for category, patterns in self.discussion_patterns.items():
            for pattern in patterns:
                matches = len(re.findall(pattern, text_to_analyze, re.IGNORECASE))
                category_scores[category] += matches
        
        # Apply priority weighting
        weighted_scores = {}
        for category, score in category_scores.items():
            if score > 0:
                priority_weight = len(priority_order) - priority_order.index(category) + 1
                weighted_scores[category] = score * priority_weight
        
        # Return highest weighted category
        if weighted_scores:
            best_category = max(weighted_scores.items(), key=lambda x: x[1])
            return self.get_category_name(best_category[0]), 'medium'
        
        # Level 4: Check for basic opinion indicators
        if not self._has_opinion_indicators(text_to_analyze):
            return self.get_category_name('news_announcements'), 'low_opinion'
            
        # Level 5: Subreddit defaults (low confidence)
        if subreddit in self.subreddit_defaults:
            return self.get_category_name(self.subreddit_defaults[subreddit]), 'low'
        
        # Ultimate fallback - most common discussion type
        return self.get_category_name('reviews_discussions'), 'fallback'
    
    def _contains_media_reference(self, text, subreddit=None):
        """Check if text contains actual media references - using optimized Layer 0"""
        
        # STEP 1: Immediate exclusions - these are definitely not media content
        immediate_exclusions = [
            # Technical/Platform issues
            r'streaming.*device', r'best.*device', r'roku', r'chromecast', r'apple.*tv.*device',
            r'subtitles?$', r'subtitle.*setting', r'closed.*caption',
            r'buffering', r'loading', r'internet.*speed', r'wifi', r'connection.*issue',
            r'app.*crash', r'login.*error', r'account.*problem', r'password.*reset',
            
            # Business/Financial news
            r'stock.*price', r'earnings', r'revenue', r'\\$\\d+.*billion', r'acquisition',
            r'lawsuit', r'legal.*battle', r'court.*case', r'settlement',
            r'subscription.*price', r'cost.*increase', r'billing.*issue', r'refund',
            
            # Platform comparisons (not about content)
            r'^(disney.*plus|netflix|hulu|prime.*video|hbo.*max)\\s*\\?\\s*$',
            r'(disney.*plus|netflix|hulu) vs (disney.*plus|netflix|hulu)',
            r'which.*platform.*better', r'best.*streaming.*service',
            
            # Celebrity death/news (not about their content)
            r'dead at \\d+', r'dies at \\d+', r'\\bdied\\b', r'cardiac.*arrest',
            r'accident.*death', r'tragic.*news', r'rip\\s+[A-Z][a-z]+',
            
            # Network/Business operations
            r'launches.*in.*country', r'available.*in.*region', r'coming.*to.*country',
            r'million.*viewers', r'ratings.*hit', r'series.*high.*viewers'
        ]
        
        for pattern in immediate_exclusions:
            if re.search(pattern, text, re.IGNORECASE):
                return False
        
        # STEP 2: Strong media indicators
        strong_media_indicators = [
            # Specific title patterns
            r'"[A-Z][^"]{3,}"', r"'[A-Z][^']{3,}'",
            r'\\bthe [A-Z][a-z]+(?:\\s+[A-Z][a-z]+){1,3}\\b',
            
            # Season/Episode references
            r'season \\d+', r'episode \\d+', r's\\d+e\\d+', r'finale', r'premiere',
            r'\\bseason\\s+(?:one|two|three|four|five|final)', r'series.*finale',
            
            # Viewing activity
            r'watching\\s+[A-Z][^\\s]{2,}', r'watched\\s+[A-Z][^\\s]{2,}',
            r'binge.*watch.*[A-Z]', r'binged?\\s+[A-Z][^\\s]{2,}',
            r'finished.*watching', r'started.*watching', r'rewatching',
            
            # Known titles and franchises
            r'american.*horror.*story', r'peaky.*blinders', r'black.*mirror',
            r'spider-?man', r'kpop.*demon.*hunters', r'stranger.*things',
            r'breaking.*bad', r'the.*office', r'game.*of.*thrones',
            r'\\btrainwreck\\b.*(?:pi|moms|documentary|episode)',
        ]
        
        for pattern in strong_media_indicators:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        # STEP 3: Additional media patterns
        additional_media_patterns = [
            r'top \\d+.*(?:favorite|best).*(?:on|netflix|hulu|disney|hbo)',
            r'(?:netflix|hulu|disney|hbo).*original.*(?:exceed|disappoint|good|bad)',
            r'what.*(?:movie|film|show|series).*(?:is|this|for|you)',
            r'need.*(?:find|identify).*(?:movie|film|show|series)',
            r'(?:movies|films|shows|series).*(?:that|which|with)\\s+\\w+',
            r'looking.*for.*(?:anime|movie|film|show|series).*(?:that|with)',
            r'any.*similar.*(?:shows|movies|films|series)',
            r'(?:classic|old).*(?:movies|films).*(?:everyone|should)',
            r'coming.*of.*age.*(?:films|movies)',
            r'animated.*(?:marvel|dc).*(?:television|series)',
            r'trailer.*(?:hulu|netflix|disney)',
            r'\\b(?:untamed|freakish)\\b.*(?:series|show|documentary)',
            r'films.*(?:that|I).*(?:wish|would)',
        ]
        
        for pattern in additional_media_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        # STEP 4: Genre-specific subreddits get lenient treatment
        if subreddit in ['anime', 'animesuggest', 'horror', 'horrormovies', 
                        'documentaries', 'MovieSuggestions', 'televisionsuggestions',
                        'tipofmytongue', 'ifyoulikeblank', 'criterion', 'truefilm', 'flicks']:
            lenient_patterns = [
                r'\\bmovie\\b', r'\\bfilm\\b', r'\\bshow\\b', r'\\bseries\\b', 
                r'\\banime\\b', r'\\bdocumentary\\b', r'recommend', r'suggest',
                r'what.*watch', r'best.*(?:movie|show|film|series)',
                r'favorite.*(?:movie|show|film|series)',
                r'looking.*for.*anime', r'need.*to.*(?:cry|laugh)',
                r'what.*(?:this|vibe)', r'similar.*to',
                r'anime.*that.*(?:takes|death|serious)'
            ]
            
            for pattern in lenient_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return True
        
        return False
    
    def _has_opinion_indicators(self, text):
        """Check if text contains opinion/sentiment indicators"""
        opinion_patterns = [
            # Strong opinions
            r'amazing', r'brilliant', r'fantastic', r'excellent', r'terrible', r'awful',
            r'love', r'hate', r'favorite', r'best', r'worst', r'good', r'bad',
            # Emotional reactions  
            r'made.*me.*(?:cry|laugh)', r'emotional', r'touching', r'boring',
            # Recommendations
            r'recommend', r'suggest', r'worth.*watch', r'must.*watch'
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
        return 'Reviews & Discussions'  # Default fallback
    
    def classify_dataframe(self, df):
        """Classify all posts in a dataframe, filtering out non-media posts"""
        results = []
        filtered_indices = []
        sentiment_results = []
        
        for idx, row in df.iterrows():
            category, confidence = self.classify_post(
                title=row['title'],
                flair=row.get('link_flair_text'),
                subreddit=row.get('subreddit'),
                selftext=row.get('selftext', '')
            )
            
            # Only include posts that contain media references
            if category is not None:
                results.append({
                    'category': category,
                    'confidence': confidence
                })
                filtered_indices.append(idx)
                
                # Entertainment title sentiment analysis
                sentiment_data = self.sentiment_analyzer.analyze_title_mentions(
                    title=row['title'],
                    selftext=row.get('selftext', '')
                )
                sentiment_results.append(sentiment_data)
        
        # Create dataframe with only media posts
        if filtered_indices:
            df_filtered = df.loc[filtered_indices].copy()
            df_filtered['category'] = [r['category'] for r in results]
            df_filtered['classification_confidence'] = [r['confidence'] for r in results]
            
            # Add entertainment sentiment columns
            df_filtered['entertainment_titles'] = [s['entertainment_titles'] for s in sentiment_results]
            df_filtered['sentiment_score'] = [s['sentiment_score'] for s in sentiment_results]
            df_filtered['sentiment_label'] = [s['sentiment_label'] for s in sentiment_results]
            
            return df_filtered
        else:
            # Return empty dataframe with same columns if no media posts found
            df_empty = df.iloc[0:0].copy()
            df_empty['category'] = []
            df_empty['classification_confidence'] = []
            df_empty['entertainment_titles'] = []
            df_empty['sentiment_score'] = []
            df_empty['sentiment_label'] = []
            return df_empty
    
    def analyze_classification(self, df_classified, df_original=None):
        """Analyze classification results with discussion-type focus"""
        print("=== ENTERTAINMENT DISCUSSION-TYPE CLASSIFICATION ANALYSIS ===")
        print(f"Total posts classified: {len(df_classified)}")
        
        # Show filtering stats if original dataframe provided
        if df_original is not None:
            filtered_out = len(df_original) - len(df_classified)
            filter_rate = (filtered_out / len(df_original)) * 100 if len(df_original) > 0 else 0
            print(f"Posts filtered out (no media): {filtered_out} ({filter_rate:.1f}%)")
        
        if len(df_classified) == 0:
            print("No media posts found to classify.")
            return
        
        # Category distribution
        category_counts = df_classified['category'].value_counts()
        print(f"\\nDiscussion Type Distribution:")
        for category, count in category_counts.items():
            percentage = (count / len(df_classified)) * 100
            print(f"  {category}: {count} posts ({percentage:.1f}%)")
        
        # Confidence distribution
        confidence_counts = df_classified['classification_confidence'].value_counts()
        print(f"\\nConfidence Distribution:")
        for confidence, count in confidence_counts.items():
            percentage = (count / len(df_classified)) * 100
            print(f"  {confidence}: {count} posts ({percentage:.1f}%)")
        
        # Show examples from each category
        print(f"\\nExamples from each discussion type:")
        for category in category_counts.index[:5]:  # Top 5 categories
            examples = df_classified[df_classified['category'] == category]['title'].head(2)
            print(f"\\n{category}:")
            for i, title in enumerate(examples):
                print(f"  {i+1}. {title[:80]+'...' if len(title) > 80 else title}")

def test_discussion_classifier():
    """Test the new discussion-type classifier"""
    
    test_data = [
        # Recommendation Requests
        {"title": "Need good horror movies for Halloween", "subreddit": "MovieSuggestions"},
        {"title": "What should I watch next on Netflix?", "subreddit": "netflix"},
        {"title": "Looking for anime that takes death seriously", "subreddit": "animesuggest"},
        
        # Reviews & Discussions  
        {"title": "Just finished The Bear season 4. A bit meh.", "subreddit": "hulu"},
        {"title": "American Horror Story - Best Season?", "subreddit": "hulu"},
        {"title": "What do you think of this new trailer?", "subreddit": "movies"},
        
        # News & Announcements
        {"title": "Spider-Man Beyond the Spider-Verse Delayed to June 2027", "subreddit": "movies"},
        {"title": "Official Poster for Avatar Fire and Ash", "subreddit": "movies"},
        
        # Identification & Help
        {"title": "Need to find this movie from the 90s", "subreddit": "letterboxd"},
        {"title": "What films have this dark vibe?", "subreddit": "letterboxd"},
        
        # Lists & Rankings
        {"title": "My top 5 favorite horror movies of all time", "subreddit": "horror"},
        {"title": "Classic movies everyone should have seen", "subreddit": "movies"},
    ]
    
    df_test = pd.DataFrame(test_data)
    classifier = EntertainmentClassifier()
    
    print("=== TESTING NEW DISCUSSION-TYPE CLASSIFIER ===")
    for idx, row in df_test.iterrows():
        category, confidence = classifier.classify_post(
            row['title'], subreddit=row['subreddit']
        )
        print(f"'{row['title'][:60]}...' → {category} ({confidence})")

if __name__ == "__main__":
    test_discussion_classifier()