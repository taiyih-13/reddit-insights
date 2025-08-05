import re
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import defaultdict
from transformers import pipeline
import ast
import warnings
warnings.filterwarnings("ignore")

class TravelSentimentAnalyzer:
    def __init__(self):
        # Use fast VADER sentiment analysis for optimal performance
        print("Using VADER sentiment analysis for optimal performance...")
        self.analyzer = SentimentIntensityAnalyzer()
        print("✅ VADER sentiment analyzer loaded successfully!")
        
        # Initialize NER pipeline for destination extraction
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
        
        # Popular travel destinations seed list
        self.popular_destinations = {
            # Major Cities
            'tokyo', 'paris', 'london', 'new york', 'rome', 'barcelona', 'amsterdam',
            'berlin', 'prague', 'vienna', 'budapest', 'istanbul', 'athens', 'lisbon',
            'madrid', 'florence', 'venice', 'milan', 'munich', 'zurich', 'geneva',
            'stockholm', 'copenhagen', 'oslo', 'helsinki', 'reykjavik', 'dublin',
            'edinburgh', 'glasgow', 'manchester', 'liverpool', 'bristol', 'bath',
            'york', 'cambridge', 'oxford', 'canterbury', 'brighton', 'exeter',
            
            # Countries
            'japan', 'france', 'italy', 'germany', 'spain', 'portugal', 'greece',
            'turkey', 'austria', 'switzerland', 'netherlands', 'belgium', 'denmark',
            'sweden', 'norway', 'finland', 'iceland', 'ireland', 'united kingdom',
            'poland', 'czech republic', 'hungary', 'croatia', 'slovenia', 'slovakia',
            'romania', 'bulgaria', 'serbia', 'bosnia', 'montenegro', 'albania',
            'thailand', 'vietnam', 'cambodia', 'laos', 'myanmar', 'malaysia',
            'singapore', 'indonesia', 'philippines', 'south korea', 'china', 'india',
            'nepal', 'bhutan', 'sri lanka', 'maldives', 'australia', 'new zealand',
            'fiji', 'canada', 'united states', 'mexico', 'guatemala', 'belize',
            'costa rica', 'panama', 'colombia', 'ecuador', 'peru', 'bolivia',
            'chile', 'argentina', 'uruguay', 'brazil', 'venezuela', 'guyana',
            'suriname', 'egypt', 'morocco', 'tunisia', 'south africa', 'kenya',
            'tanzania', 'uganda', 'rwanda', 'ethiopia', 'madagascar', 'mauritius',
            
            # Famous Attractions & Landmarks
            'machu picchu', 'great wall', 'colosseum', 'eiffel tower', 'big ben',
            'statue of liberty', 'golden gate bridge', 'taj mahal', 'petra',
            'angkor wat', 'great barrier reef', 'mount fuji', 'mount everest',
            'kilimanjaro', 'mont blanc', 'matterhorn', 'santorini', 'mykonos',
            'ibiza', 'bali', 'phuket', 'goa', 'dubai', 'abu dhabi', 'doha',
            'hong kong', 'macau', 'las vegas', 'miami', 'los angeles', 'san francisco',
            'seattle', 'chicago', 'boston', 'washington dc', 'philadelphia',
            'new orleans', 'nashville', 'austin', 'denver', 'portland', 'vancouver',
            'toronto', 'montreal', 'quebec city', 'rio de janeiro', 'buenos aires',
            'lima', 'cusco', 'santiago', 'montevideo', 'cape town', 'johannesburg',
            'cairo', 'marrakech', 'casablanca', 'mumbai', 'delhi', 'bangalore',
            'chennai', 'kolkata', 'kerala', 'rajasthan', 'goa', 'kathmandu',
            'pokhara', 'colombo', 'kandy', 'male', 'sydney', 'melbourne',
            'brisbane', 'perth', 'adelaide', 'auckland', 'wellington', 'christchurch',
            'queenstown', 'rotorua', 'suva', 'nadi'
        }
        
        # Travel-specific context patterns
        self.travel_context_patterns = [
            r'\b(visit|visited|visiting|travel|traveled|travelling|trip|vacation|holiday)\b',
            r'\b(stayed|stay|hotel|hostel|accommodation|resort|airbnb)\b',
            r'\b(recommend|beautiful|amazing|stunning|disappointing|crowded|overrated)\b',
            r'\b(flight|airport|train|bus|transport|getting to|how to get)\b',
            r'\b(tourist|tourism|sightseeing|attraction|landmark|museum|gallery)\b',
            r'\b(backpack|backpacking|solo travel|group travel|family trip)\b',
            r'\b(weather|climate|season|best time|when to visit)\b',
            r'\b(budget|expensive|cheap|cost|price|money|currency)\b'
        ]
    
    def extract_destinations_ner(self, text):
        """Extract travel destinations using NER"""
        if not text or pd.isna(text) or not self.use_ner:
            return []
        
        try:
            # Run NER on the text
            entities = self.ner_pipeline(text)
            
            destinations = []
            
            for entity in entities:
                word = entity['word'].strip()
                label = entity['entity_group']
                confidence = entity['score']
                
                # Focus on LOCATION entities and high-confidence MISC entities
                is_destination = (
                    # High confidence LOCATION entities
                    (label == 'LOC' and confidence > 0.7) or
                    # MISC entities with travel context (often place names)
                    (label == 'MISC' and confidence > 0.8 and 
                     self._has_travel_context(text)) or
                    # High confidence entities that look like place names
                    (confidence > 0.9 and len(word) > 3 and 
                     self._looks_like_place_name(word))
                )
                
                if is_destination:
                    clean_destination = self._clean_destination_name(word)
                    if clean_destination and not self._is_excluded_destination(clean_destination):
                        destinations.append(clean_destination.lower())
            
            return destinations
            
        except Exception as e:
            print(f"NER processing error: {e}")
            return []
    
    def _has_travel_context(self, text):
        """Check if text has travel-related context"""
        text_lower = text.lower()
        return any(re.search(pattern, text_lower, re.IGNORECASE) 
                  for pattern in self.travel_context_patterns)
    
    def _looks_like_place_name(self, word):
        """Check if word looks like a place name"""
        # Multiple words (likely place names)
        words = word.split()
        if len(words) >= 2:
            return True
        
        # Contains common place name suffixes
        place_suffixes = ['land', 'burg', 'wick', 'ford', 'ton', 'ham', 'shire', 'stad', 'ville']
        if any(word.lower().endswith(suffix) for suffix in place_suffixes):
            return True
            
        return False
    
    def _clean_destination_name(self, destination):
        """Clean destination name"""
        # Remove special characters but keep spaces and hyphens
        cleaned = re.sub(r'[^a-zA-Z0-9\s\-\']', '', destination)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Skip if too short or common words
        if len(cleaned) < 3 or cleaned.lower() in ['the', 'and', 'of', 'in', 'on', 'at', 'is', 'was', 'new', 'old']:
            return None
            
        return cleaned
    
    def extract_destinations_conservative(self, text):
        """Conservative destination detection using popular destinations list"""
        if not text or pd.isna(text):
            return []
        
        text_lower = text.lower()
        mentioned_destinations = set()
        
        # Check if text has travel context
        has_travel_context = self._has_travel_context(text)
        
        # Only extract destinations if there's clear travel context
        if has_travel_context:
            for destination in self.popular_destinations:
                if self._find_destination_in_context(destination, text_lower):
                    mentioned_destinations.add(destination)
        
        return list(mentioned_destinations)
    
    def extract_destinations(self, text):
        """Main method that combines NER and conservative approaches"""
        if not text or pd.isna(text):
            return []
        
        all_destinations = set()
        
        # Try NER first
        if self.use_ner:
            ner_destinations = self.extract_destinations_ner(text)
            all_destinations.update(ner_destinations)
        
        # Conservative detection as backup/supplement
        conservative_destinations = self.extract_destinations_conservative(text)
        all_destinations.update(conservative_destinations)
        
        # Remove duplicates and clean up
        unique_destinations = []
        for destination in all_destinations:
            if destination and len(destination.strip()) > 2:
                clean_destination = destination.strip().lower()
                if clean_destination not in unique_destinations:
                    unique_destinations.append(clean_destination)
        
        return unique_destinations
    
    def _is_excluded_destination(self, destination):
        """Exclude generic travel terms that aren't actual destinations"""
        destination_lower = destination.lower().strip()
        
        # Generic travel terms
        generic_terms = {
            'hotel', 'airport', 'train', 'bus', 'car', 'flight', 'ticket',
            'station', 'terminal', 'gate', 'platform', 'road', 'street',
            'restaurant', 'cafe', 'bar', 'shop', 'market', 'mall',
            'museum', 'gallery', 'church', 'temple', 'mosque', 'park',
            'beach', 'mountain', 'river', 'lake', 'sea', 'ocean',
            'north', 'south', 'east', 'west', 'central', 'downtown',
            'city', 'town', 'village', 'country', 'region', 'area',
            'place', 'location', 'destination', 'trip', 'travel', 'tour',
            'vacation', 'holiday', 'journey', 'adventure', 'experience',
            'budget', 'money', 'cost', 'price', 'cheap', 'expensive',
            'time', 'day', 'night', 'morning', 'evening', 'week', 'month',
            'people', 'person', 'tourist', 'traveler', 'local', 'guide'
        }
        
        # Transportation companies
        transport_brands = {
            'uber', 'lyft', 'airbnb', 'booking', 'expedia', 'trivago',
            'ryanair', 'easyjet', 'lufthansa', 'emirates', 'delta',
            'american airlines', 'british airways', 'klm', 'air france'
        }
        
        if (destination_lower in generic_terms or 
            destination_lower in transport_brands):
            return True
        
        # Exclude single letter/number combinations
        if len(destination_lower) <= 2:
            return True
            
        # Exclude pure numbers
        if destination_lower.isdigit():
            return True
            
        return False
    
    def _find_destination_in_context(self, destination, text_lower):
        """Enhanced destination context validation"""
        escaped_destination = re.escape(destination)
        pattern = r'\b' + escaped_destination + r'\b'
        
        if not re.search(pattern, text_lower, re.IGNORECASE):
            return False
        
        # Enhanced context words for travel
        context_words = [
            'visit', 'visited', 'visiting', 'travel', 'traveled', 'trip',
            'vacation', 'holiday', 'stayed', 'stay', 'recommend', 'love',
            'beautiful', 'amazing', 'stunning', 'disappointing', 'crowded',
            'overrated', 'underrated', 'hidden gem', 'must see', 'avoid',
            'flight', 'hotel', 'hostel', 'backpack', 'solo', 'budget',
            'expensive', 'cheap', 'weather', 'season', 'best time',
            'tourist', 'sightseeing', 'attraction', 'landmark', 'culture'
        ]
        
        # Look for context words within 50 characters of the destination
        destination_matches = list(re.finditer(pattern, text_lower, re.IGNORECASE))
        for match in destination_matches:
            start = max(0, match.start() - 50)
            end = min(len(text_lower), match.end() + 50)
            context = text_lower[start:end]
            
            if any(word in context for word in context_words):
                return True
        
        return False
    
    def _categorize_destination(self, destination):
        """Categorize destination as city, country, or attraction"""
        destination_lower = destination.lower()
        
        # Known countries
        countries = {
            'japan', 'france', 'italy', 'germany', 'spain', 'portugal', 'greece',
            'turkey', 'austria', 'switzerland', 'netherlands', 'belgium', 'denmark',
            'sweden', 'norway', 'finland', 'iceland', 'ireland', 'united kingdom',
            'thailand', 'vietnam', 'cambodia', 'malaysia', 'singapore', 'indonesia',
            'philippines', 'south korea', 'china', 'india', 'australia', 'new zealand',
            'canada', 'united states', 'mexico', 'brazil', 'argentina', 'chile',
            'peru', 'colombia', 'egypt', 'morocco', 'south africa'
        }
        
        # Known attractions/landmarks
        attractions = {
            'machu picchu', 'great wall', 'colosseum', 'eiffel tower', 'big ben',
            'statue of liberty', 'golden gate bridge', 'taj mahal', 'petra',
            'angkor wat', 'great barrier reef', 'mount fuji', 'mount everest',
            'kilimanjaro', 'santorini', 'mykonos', 'bali', 'phuket'
        }
        
        if destination_lower in countries:
            return 'country'
        elif destination_lower in attractions:
            return 'attraction'
        else:
            return 'city'  # Default to city
    
    def analyze_destination_mentions(self, title, selftext):
        """Analyze sentiment for mentions of travel destinations"""
        # Simple approach - use VADER on the combined text
        title_text = str(title) if not pd.isna(title) else ""
        content_text = str(selftext) if not pd.isna(selftext) else ""
        
        # Use VADER sentiment analysis
        title_sentiment = self.analyzer.polarity_scores(title_text)
        content_sentiment = self.analyzer.polarity_scores(content_text)
        weighted_sentiment = (title_sentiment['compound'] * 0.7) + (content_sentiment['compound'] * 0.3)
        
        # Extract mentioned destinations
        mentioned_destinations = []
        mentioned_destinations.extend(self.extract_destinations(title_text))
        mentioned_destinations.extend(self.extract_destinations(content_text))
        
        # Remove duplicates while preserving order
        unique_destinations = []
        seen = set()
        for destination in mentioned_destinations:
            if destination not in seen:
                unique_destinations.append(destination)
                seen.add(destination)
        
        return {
            'destinations': unique_destinations,
            'sentiment_score': weighted_sentiment,
            'sentiment_label': self._get_sentiment_label(weighted_sentiment)
        }
    
    def _get_sentiment_label(self, score):
        """Convert sentiment score to label"""
        if score >= 0.1:
            return 'positive'
        elif score <= -0.1:
            return 'negative'
        else:
            return 'neutral'
    
    def aggregate_destination_sentiment(self, df):
        """Aggregate sentiment for all destinations in the dataset"""
        destination_data = defaultdict(lambda: {'scores': [], 'posts': 0})
        
        for _, row in df.iterrows():
            # Check if destinations column exists
            if 'travel_destinations' in df.columns and not pd.isna(row['travel_destinations']):
                try:
                    destinations = ast.literal_eval(row['travel_destinations'])
                except:
                    continue
            else:
                # Extract destinations on the fly
                analysis = self.analyze_destination_mentions(row['title'], row['selftext'])
                destinations = analysis['destinations']
            
            # Get sentiment score
            if 'sentiment_score' in df.columns and not pd.isna(row['sentiment_score']):
                sentiment_score = row['sentiment_score']
            else:
                # Calculate sentiment on the fly
                analysis = self.analyze_destination_mentions(row['title'], row['selftext'])
                sentiment_score = analysis['sentiment_score']
            
            # Aggregate data for each destination
            for destination in destinations:
                if destination and len(destination.strip()) > 2:
                    clean_destination = destination.strip().lower()
                    destination_data[clean_destination]['scores'].append(sentiment_score)
                    destination_data[clean_destination]['posts'] += 1
        
        # Calculate average sentiment for each destination
        result = []
        for destination, data in destination_data.items():
            if data['posts'] >= 1:  # At least 1 mention
                avg_sentiment = sum(data['scores']) / len(data['scores'])
                result.append({
                    'destination': destination.title(),
                    'avg_sentiment': avg_sentiment,
                    'post_count': data['posts'],
                    'sentiment_label': self._get_sentiment_label(avg_sentiment),
                    'category': self._categorize_destination(destination)
                })
        
        # Sort by post count (popularity) then by sentiment
        result.sort(key=lambda x: (x['post_count'], x['avg_sentiment']), reverse=True)
        return result
    
    def get_balanced_destination_display(self, df, items_per_category=5):
        """Get balanced display of destinations: 5 cities, 5 countries, 5 attractions"""
        all_destinations = self.aggregate_destination_sentiment(df)
        
        if not all_destinations:
            return []
        
        # Group by category
        by_category = {'city': [], 'country': [], 'attraction': []}
        for dest in all_destinations:
            category = dest['category']
            if category in by_category:
                by_category[category].append(dest)
        
        # Select top items from each category
        balanced_results = []
        
        # Add cities (sorted by post count, then sentiment)
        for dest in by_category['city'][:items_per_category]:
            balanced_results.append(dest)
        
        # Add countries 
        for dest in by_category['country'][:items_per_category]:
            balanced_results.append(dest)
        
        # Add attractions
        for dest in by_category['attraction'][:items_per_category]:
            balanced_results.append(dest)
        
        # If we don't have enough in some categories, fill with remaining top destinations
        total_needed = items_per_category * 3
        if len(balanced_results) < total_needed:
            added_destinations = {dest['destination'] for dest in balanced_results}
            for dest in all_destinations:
                if dest['destination'] not in added_destinations and len(balanced_results) < total_needed:
                    balanced_results.append(dest)
        
        return balanced_results[:total_needed]