import re
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import defaultdict
from transformers import pipeline
import ast
import warnings
warnings.filterwarnings("ignore")

class OptimizedEntertainmentSentimentAnalyzer:
    def __init__(self):
        # Use fast VADER sentiment analysis for better performance
        print("Using VADER sentiment analysis for optimal performance...")
        self.analyzer = SentimentIntensityAnalyzer()
        self.use_pretrained_sentiment = False
        print("✅ VADER sentiment analyzer loaded successfully!")
        
        # Initialize better NER pipeline (dslim/bert-base-NER performs better for entertainment)
        try:
            print("Loading optimized NER pipeline (dslim/bert-base-NER)...")
            self.ner_pipeline = pipeline("ner", 
                                        model="dslim/bert-base-NER",
                                        aggregation_strategy="simple")
            self.use_ner = True
            print("✅ Optimized NER pipeline loaded successfully!")
        except Exception as e:
            print(f"⚠️  Could not load optimized NER pipeline: {e}")
            print("Falling back to default model...")
            try:
                self.ner_pipeline = pipeline("ner", aggregation_strategy="simple")
                self.use_ner = True
                print("✅ Fallback NER pipeline loaded!")
            except Exception as e2:
                print(f"❌ Could not load any NER pipeline: {e2}")
                self.ner_pipeline = None
                self.use_ner = False
        
        # Expanded popular titles list with newer content (removed blacklisted terms)
        self.popular_titles = {
            # TV Shows - Classic
            'breaking bad', 'game of thrones', 'the office', 'stranger things', 
            'house of dragon', 'true detective', 'the sopranos', 'the wire',
            'seinfeld', 'westworld', 'the mandalorian',
            'ozark', 'narcos', 'mindhunter', 'black mirror', 'sherlock',
            'dexter', 'homeland', 'the crown', 'better call saul', 'fargo',
            'succession', 'euphoria', 'the boys', 'squid game', 'money heist',
            'chernobyl', 'band of brothers', 'the walking dead',
            'american horror story', 'rick and morty', 'the simpsons',
            
            # TV Shows - Recent/Newer 
            'the bear', 'wednesday', 'house of the dragon', 'rings of power',
            'andor', 'the last of us', 'yellowjackets', 'severance', 'abbott elementary',
            'only murders in the building', 'ted lasso', 'bridgerton', 'love death robots',
            'the umbrella academy', 'cobra kai', 'outer banks', 'emily in paris',
            'shadow and bone', 'the witcher', 'kpop demon hunters', 'demon slayer',
            'attack on titan', 'jujutsu kaisen', 'one piece', 'my hero academia',
            
            # Movies - Classic
            'john wick', 'fast and furious', 'avengers', 'star wars',
            'lord of the rings', 'harry potter', 'the dark knight',
            'pulp fiction', 'the godfather', 'inception', 'interstellar',
            'parasite', 'joker', 'dune', 'spider-man', 'batman',
            'mission impossible', 'james bond', 'indiana jones',
            'jurassic park', 'alien', 'terminator', 'matrix',
            'back to the future', 'ghostbusters', 'die hard',
            'shrek', 'toy story', 'finding nemo', 'frozen',
            'black panther', 'iron man', 'captain america',
            'deadpool', 'x-men', 'fantastic beasts',
            
            # Movies - Recent
            'oppenheimer', 'barbie', 'dune part two', 'top gun maverick',
            'everything everywhere all at once', 'the batman', 'no way home',
            'multiverse of madness', 'wakanda forever', 'avatar way of water',
            'turning red', 'encanto', 'luca', 'soul', 'onward'
        }
        
        # Enhanced context patterns
        self.entertainment_context_patterns = [
            r'\b(watch|watched|watching|episode|season|series|show|movie|film|cinema|streaming)\b',
            r'\b(netflix|hbo|disney|amazon prime|hulu|paramount|apple tv|max|peacock)\b',
            r'\b(director|actor|actress|cast|plot|storyline|script|cinematography)\b',
            r'\b(recommend|recommendation|suggest|review|rating|trailer|premiere)\b',
            r'\b(binge|marathon|rewatch|sequel|prequel|spin-off|finale|pilot)\b',
            r'\b(anime|manga|k-drama|kdrama|bollywood|hollywood)\b'
        ]
        
    def extract_entertainment_titles_ner(self, text):
        """Extract entertainment titles using optimized NER"""
        if not text or pd.isna(text) or not self.use_ner:
            return []
        
        try:
            # Run NER on the text
            entities = self.ner_pipeline(text)
            
            entertainment_entities = []
            
            for entity in entities:
                word = entity['word'].strip()
                label = entity['entity_group']
                confidence = entity['score']
                
                # More inclusive detection for entertainment
                is_entertainment = (
                    # High confidence MISC entities (often entertainment titles)
                    (label == 'MISC' and confidence > 0.7) or
                    # ORG entities with entertainment context
                    (label == 'ORG' and confidence > 0.7 and 
                     self._has_entertainment_context(text)) or
                    # High confidence entities that look like titles
                    (confidence > 0.85 and len(word) > 4 and 
                     self._looks_like_title(word))
                )
                
                if is_entertainment:
                    clean_title = self._clean_ner_entity(word)
                    if clean_title and not self._is_excluded_title(clean_title):
                        entertainment_entities.append(clean_title.lower())
            
            return entertainment_entities
            
        except Exception as e:
            print(f"NER processing error: {e}")
            return []
    
    def _has_entertainment_context(self, text):
        """Check if text has entertainment-related context"""
        text_lower = text.lower()
        return any(re.search(pattern, text_lower, re.IGNORECASE) 
                  for pattern in self.entertainment_context_patterns)
    
    def _looks_like_title(self, word):
        """Check if word looks like an entertainment title"""
        # Title case with multiple words or contains common title elements
        words = word.split()
        if len(words) >= 2:
            return True
        
        # Contains numbers (seasons, sequels)
        if re.search(r'\d', word):
            return True
            
        # Contains title-like punctuation
        if ':' in word or '-' in word:
            return True
            
        return False
    
    def _clean_ner_entity(self, entity):
        """Clean NER detected entity"""
        # Remove special characters but keep title punctuation
        cleaned = re.sub(r'[^a-zA-Z0-9\s:&\-\']', '', entity)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Skip if too short or common words
        if len(cleaned) < 3 or cleaned.lower() in ['the', 'and', 'of', 'in', 'on', 'at', 'is', 'was']:
            return None
            
        return cleaned
    
    def extract_entertainment_titles_conservative(self, text):
        """Enhanced conservative detection method"""
        if not text or pd.isna(text):
            return []
        
        text_lower = text.lower()
        mentioned_titles = set()
        
        # Check if text has entertainment context
        has_entertainment_context = self._has_entertainment_context(text)
        
        # 1. Extract quoted titles (highest confidence)
        quoted_titles = re.findall(r'"([^"]+)"', text)
        for title in quoted_titles:
            clean_title = title.strip()
            if (len(clean_title.split()) >= 1 and 
                2 <= len(clean_title) <= 60 and
                not self._is_excluded_title(clean_title)):
                mentioned_titles.add(clean_title.lower())
        
        # 2. Look for popular titles from expanded seed list
        if has_entertainment_context:
            for title in self.popular_titles:
                if self._find_title_in_context(title, text_lower):
                    mentioned_titles.add(title)
        
        # 3. Pattern-based detection for new titles
        if has_entertainment_context:
            # Look for title-case sequences near entertainment keywords
            title_patterns = [
                r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)*)\b(?=.*\b(?:series|show|movie|film|season|episode)\b)',
                r'\b([A-Z][a-z]+(?: (?:of|the|and|in|on|at|to|for) [A-Z][a-z]+)+)\b',
                r'\b([A-Z][a-z]+:? [A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
            ]
            
            for pattern in title_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    clean_match = match.strip()
                    if (3 <= len(clean_match) <= 60 and 
                        not self._is_excluded_title(clean_match) and
                        len(clean_match.split()) >= 2):
                        mentioned_titles.add(clean_match.lower())
        
        return list(mentioned_titles)
    
    def extract_entertainment_titles(self, text):
        """Main method that combines optimized NER and enhanced conservative approaches"""
        if not text or pd.isna(text):
            return []
        
        all_titles = set()
        
        # Try optimized NER first
        if self.use_ner:
            ner_titles = self.extract_entertainment_titles_ner(text)
            all_titles.update(ner_titles)
        
        # Enhanced conservative detection as backup/supplement
        conservative_titles = self.extract_entertainment_titles_conservative(text)
        all_titles.update(conservative_titles)
        
        # Remove duplicates and clean up
        unique_titles = []
        for title in all_titles:
            if title and len(title.strip()) > 2:
                clean_title = title.strip().lower()
                if clean_title not in unique_titles:
                    unique_titles.append(clean_title)
        
        return unique_titles
    
    def _is_excluded_title(self, title):
        """Exclude obvious false positives while keeping real entertainment titles"""
        title_lower = title.lower().strip()
        
        # Original problematic titles
        false_positives = {
            'italian', 'world war'
        }
        
        # Generic brand/company names that aren't entertainment titles
        generic_brands = {
            'disney', 'amazon', 'netflix', 'hulu', 'hbo', 'max', 'apple', 'prime',
            'paramount', 'peacock', 'sony', 'warner', 'universal', 'fox', 'cbs',
            'nbc', 'abc', 'espn', 'discovery', 'tnt', 'fx', 'amc', 'showtime',
            'starz', 'epix', 'crunchyroll', 'funimation'
        }
        
        # Generic single words that aren't titles
        generic_words = {
            'land', 'world', 'house', 'home', 'time', 'life', 'love', 'death',
            'war', 'peace', 'fire', 'water', 'earth', 'air', 'gold', 'silver',
            'black', 'white', 'red', 'blue', 'green', 'yellow', 'orange', 'purple'
        }
        
        # TV/broadcast related terms
        broadcast_terms = {
            'b & n', 'tv', 'channel', 'network', 'news', 'sports', 'live',
            'stream', 'broadcast', 'cable', 'satellite'
        }
        
        if (title_lower in false_positives or 
            title_lower in generic_brands or
            title_lower in generic_words or
            title_lower in broadcast_terms):
            return True
        
        # Exclude single letter/number combinations
        if len(title_lower) <= 2:
            return True
            
        # Exclude pure numbers
        if title_lower.isdigit():
            return True
            
        return False
    
    def _find_title_in_context(self, title, text_lower):
        """Enhanced title context validation"""
        escaped_title = re.escape(title)
        pattern = r'\b' + escaped_title + r'\b'
        
        if not re.search(pattern, text_lower, re.IGNORECASE):
            return False
        
        # Enhanced context words for entertainment
        context_words = [
            'watch', 'watched', 'watching', 'recommend', 'love', 'hate',
            'episode', 'season', 'series', 'show', 'movie', 'film',
            'binged', 'finished', 'started', 'streaming', 'netflix',
            'hulu', 'disney', 'hbo', 'amazon', 'apple tv', 'max',
            'review', 'rating', 'trailer', 'premiere', 'finale',
            'rewatch', 'marathon', 'binge', 'anime', 'manga'
        ]
        
        # Look for context words within 40 characters of the title
        title_matches = list(re.finditer(pattern, text_lower, re.IGNORECASE))
        for match in title_matches:
            start = max(0, match.start() - 40)
            end = min(len(text_lower), match.end() + 40)
            context = text_lower[start:end]
            
            if any(word in context for word in context_words):
                return True
        
        return False
    
    def analyze_title_mentions(self, title, selftext):
        """Analyze sentiment for mentions of a specific entertainment title"""
        # Simple approach - just use VADER on the combined text
        title_text = str(title) if not pd.isna(title) else ""
        content_text = str(selftext) if not pd.isna(selftext) else ""
        
        # Use VADER sentiment analysis (simple and fast)
        title_sentiment = self.analyzer.polarity_scores(title_text)
        content_sentiment = self.analyzer.polarity_scores(content_text)
        weighted_sentiment = (title_sentiment['compound'] * 0.7) + (content_sentiment['compound'] * 0.3)
        
        # Extract mentioned titles using original approach
        mentioned_titles = []
        mentioned_titles.extend(self.extract_entertainment_titles(title_text))
        mentioned_titles.extend(self.extract_entertainment_titles(content_text))
        
        # Remove duplicates while preserving order
        unique_titles = []
        seen = set()
        for title in mentioned_titles:
            if title not in seen:
                unique_titles.append(title)
                seen.add(title)
        
        return {
            'entertainment_titles': unique_titles,
            'sentiment_score': weighted_sentiment,
            'sentiment_label': self._get_sentiment_label(weighted_sentiment)
        }
    
    
    def _get_sentiment_label(self, sentiment_score):
        """Convert sentiment score to label"""
        if sentiment_score >= 0.1:
            return 'positive'
        elif sentiment_score <= -0.1:
            return 'negative'
        else:
            return 'neutral'
    
    def aggregate_title_sentiment(self, df):
        """Aggregate sentiment data for all entertainment titles"""
        if df.empty or 'entertainment_titles' not in df.columns:
            return []
        
        title_data = defaultdict(lambda: {'scores': [], 'posts': 0})
        
        for _, row in df.iterrows():
            # Parse titles from string representation if needed
            titles = row['entertainment_titles']
            if isinstance(titles, str):
                try:
                    titles = ast.literal_eval(titles)
                except:
                    continue
            
            if titles is None or (hasattr(titles, '__len__') and len(titles) == 0):
                continue
                
            sentiment_score = row.get('sentiment_score', 0)
            if pd.isna(sentiment_score):
                continue
                
            for title in titles:
                if title and len(title.strip()) > 0:
                    clean_title = title.strip().title()  # Clean and title case
                    # Skip if title is excluded (applies blacklist during aggregation)
                    if not self._is_excluded_title(clean_title):
                        title_data[clean_title]['scores'].append(sentiment_score)
                        title_data[clean_title]['posts'] += 1
        
        # Calculate aggregated sentiment for each title
        aggregated_data = []
        for title, data in title_data.items():
            if data['posts'] > 0 and data['scores']:
                avg_sentiment = sum(data['scores']) / len(data['scores'])
                aggregated_data.append({
                    'title': title,
                    'avg_sentiment': avg_sentiment,
                    'post_count': data['posts'],
                    'sentiment_label': self._get_sentiment_label(avg_sentiment)
                })
        
        # Sort by post count (most mentioned first)
        aggregated_data.sort(key=lambda x: x['post_count'], reverse=True)
        
        return aggregated_data
    
    def _categorize_entertainment_title(self, title):
        """Categorize a title as movie, tv_show, or anime"""
        title_lower = title.lower()
        
        # Anime titles (common anime titles and keywords)
        anime_titles = {
            'demon slayer', 'jujutsu kaisen', 'one punch man', 'death note', 'attack on titan',
            'mob psycho', 'demon school', 'asobi asobase', 'high school boys', 'violet evergarden',
            'naruto', 'one piece', 'dragon ball', 'my hero academia', 'chainsaw man',
            'tokyo ghoul', 'fullmetal alchemist', 'cowboy bebop', 'evangelion', 'bleach',
            'hunter x hunter', 'fairy tail', 'black clover', 'fire force', 'seven deadly sins',
            'overlord', 'that time i got reincarnated as a slime', 'konosuba', 'no game no life',
            'code geass', 'steins gate', 'parasyte', 'akira', 'spirited away', 'your name',
            'weathering with you', 'princess mononoke', 'totoro', 'kiki delivery service',
            # Additional anime that were miscategorized
            'demon king academy', 'the misfit of demon king academy', 'welcome to demon school iruma kun',
            'mob psycho 100', 'daily lives of high school boys', 'asobi asobase workshop of fun'
        }
        
        # Movie titles (common movies)
        movie_titles = {
            'avatar', 'titanic', 'star wars', 'avengers', 'spider-man', 'batman', 'superman',
            'iron man', 'captain america', 'thor', 'black panther', 'wonder woman', 'aquaman',
            'justice league', 'x-men', 'deadpool', 'wolverine', 'fantastic four', 'guardians',
            'ant-man', 'doctor strange', 'captain marvel', 'black widow', 'the batman',
            'joker', 'dune', 'blade runner', 'matrix', 'terminator', 'alien', 'predator',
            'jurassic park', 'indiana jones', 'mission impossible', 'fast and furious',
            'john wick', 'taken', 'die hard', 'rambo', 'rocky', 'godfather', 'goodfellas',
            'scarface', 'casino', 'pulp fiction', 'kill bill', 'django unchained',
            'inglourious basterds', 'once upon a time in hollywood', 'parasite', 'moonlight',
            'la la land', 'whiplash', 'birdman', 'the revenant', 'mad max', 'interstellar',
            'inception', 'the dark knight', 'saving private ryan', 'gladiator',
            'soul', 'coco', 'moana', 'frozen', 'toy story', 'finding nemo', 'monsters inc',
            'the incredibles', 'up', 'wall-e', 'ratatouille', 'cars', 'brave', 'inside out'
        }
        
        # TV Show titles (common TV series)
        tv_show_titles = {
            'breaking bad', 'better call saul', 'the sopranos', 'the wire', 'game of thrones',
            'house of dragon', 'stranger things', 'the office', 'parks and recreation',
            'friends', 'seinfeld', 'how i met your mother', 'the big bang theory', 'scrubs',
            'arrested development', 'community', 'brooklyn nine-nine', 'modern family',
            'the good place', 'schitts creek', 'fleabag', 'the crown', 'downton abbey',
            'sherlock', 'doctor who', 'black mirror', 'westworld', 'lost', 'the x-files',
            'twin peaks', 'true detective', 'fargo', 'american horror story', 'the walking dead',
            'fear the walking dead', 'better things', 'atlanta', 'insecure', 'euphoria',
            'succession', 'ozark', 'narcos', 'mindhunter', 'house of cards', 'orange is the new black',
            'stranger things', 'the mandalorian', 'the boys', 'the marvelous mrs maisel',
            'the handmaids tale', 'this is us', 'south park', 'rick and morty', 'the simpsons',
            'family guy', 'american dad', 'archer', 'bojack horseman', 'big mouth',
            'severance', 'ted lasso', 'the bear', 'wednesday', 'squid game', 'money heist',
            'dark', 'the umbrella academy', 'cobra kai', 'outer banks', 'bridgerton',
            'the witcher', 'lucifer', 'dexter', 'homeland', '24', 'prison break'
        }
        
        # Check exact matches first
        if title_lower in anime_titles:
            return 'anime'
        elif title_lower in movie_titles:
            return 'movie'
        elif title_lower in tv_show_titles:
            return 'tv_show'
        
        # Check partial matches for multi-word titles
        for anime in anime_titles:
            if anime in title_lower or title_lower in anime:
                return 'anime'
        
        for movie in movie_titles:
            if movie in title_lower or title_lower in movie:
                return 'movie'
        
        for show in tv_show_titles:
            if show in title_lower or title_lower in show:
                return 'tv_show'
        
        # Default categorization based on common patterns
        # Anime often has Japanese-style titles or specific keywords
        anime_keywords = ['episode', 'season', 'arc', 'sensei', 'kun', 'chan', 'sama', 'san']
        if any(keyword in title_lower for keyword in anime_keywords):
            return 'anime'
        
        # Default to movie for shorter titles, TV show for longer ones
        if len(title.split()) <= 2:
            return 'movie'
        else:
            return 'tv_show'
    
    def get_balanced_sentiment_display(self, df, items_per_category=5):
        """Get balanced sentiment display with equal representation of movies, TV shows, and anime"""
        # Get all aggregated data
        all_data = self.aggregate_title_sentiment(df)
        
        if not all_data:
            return []
        
        # Categorize all titles
        categorized = {'movie': [], 'tv_show': [], 'anime': []}
        
        for item in all_data:
            category = self._categorize_entertainment_title(item['title'])
            categorized[category].append(item)
        
        # Sort each category by post count and sentiment
        for category in categorized:
            categorized[category].sort(key=lambda x: (x['post_count'], x['avg_sentiment']), reverse=True)
        
        # Select top items from each category
        balanced_results = []
        
        # Add top movies
        balanced_results.extend(categorized['movie'][:items_per_category])
        
        # Add top TV shows
        balanced_results.extend(categorized['tv_show'][:items_per_category])
        
        # Add top anime
        balanced_results.extend(categorized['anime'][:items_per_category])
        
        # Sort final results by post count to maintain prominence
        balanced_results.sort(key=lambda x: x['post_count'], reverse=True)
        
        # Add category labels for display
        for item in balanced_results:
            item['category'] = self._categorize_entertainment_title(item['title'])
        
        return balanced_results