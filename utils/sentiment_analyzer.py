import re
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import defaultdict

class StockSentimentAnalyzer:
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
        
        # Stock ticker pattern - matches $TSLA, $AAPL, etc.
        self.stock_pattern = r'\$([A-Z]{1,5})\b'
        
        # Common false positives to filter out
        self.excluded_tickers = {'USD', 'CAD', 'EUR', 'GBP', 'JPY', 'AUD', 'CEO', 'IPO', 'ETF', 'SEC'}
        
        # Enhanced detection: Company name to ticker mapping
        self.company_to_ticker = {
            # Tech Giants
            'apple': 'AAPL',
            'microsoft': 'MSFT', 
            'google': 'GOOGL',
            'alphabet': 'GOOGL',
            'amazon': 'AMZN',
            'meta': 'META',
            'facebook': 'META',
            'tesla': 'TSLA',
            'nvidia': 'NVDA',
            'intel': 'INTC',
            'amd': 'AMD',
            'netflix': 'NFLX',
            
            # Finance
            'jpmorgan': 'JPM',
            'jp morgan': 'JPM',
            'bank of america': 'BAC',
            'wells fargo': 'WFC',
            'goldman sachs': 'GS',
            'morgan stanley': 'MS',
            'berkshire hathaway': 'BRK.B',
            'berkshire': 'BRK.B',
            
            # Popular Stocks
            'gamestop': 'GME',
            'amc': 'AMC',
            'palantir': 'PLTR',
            'rocket companies': 'RKT',
            'rocket mortgage': 'RKT',
            'coinbase': 'COIN',
            'robinhood': 'HOOD',
            'snowflake': 'SNOW',
            'zoom': 'ZM',
            'peloton': 'PTON',
            'rivian': 'RIVN',
            'lucid': 'LCID',
            'lucid motors': 'LCID',
            
            # Traditional
            'walmart': 'WMT',
            'coca cola': 'KO',
            'coca-cola': 'KO',
            'pepsi': 'PEP',
            'johnson & johnson': 'JNJ',
            'procter & gamble': 'PG',
            'exxon': 'XOM',
            'chevron': 'CVX',
            'pfizer': 'PFE',
            'moderna': 'MRNA',
            
            # Airlines/Travel
            'boeing': 'BA',
            'delta': 'DAL',
            'united airlines': 'UAL',
            'american airlines': 'AAL',
            'southwest': 'LUV',
            
            # Retail
            'target': 'TGT',
            'home depot': 'HD',
            'costco': 'COST',
            'lowes': 'LOW',
            "lowe's": 'LOW',
            
            # Auto
            'ford': 'F',
            'general motors': 'GM',
            'gm': 'GM',
            'toyota': 'TM',
            
            # Others frequently mentioned
            'disney': 'DIS',
            'mcdonald': 'MCD',
            "mcdonald's": 'MCD',
            'mcdonalds': 'MCD',
            'starbucks': 'SBUX',
            'nike': 'NKE',
            'spotify': 'SPOT',
            'uber': 'UBER',
            'lyft': 'LYFT',
            'square': 'SQ',
            'block': 'SQ',  # Square rebranded to Block
            'paypal': 'PYPL',
            'visa': 'V',
            'mastercard': 'MA',
        }
        
        # Exclusion patterns to avoid false positives
        self.exclusion_patterns = [
            r'\bapple\s+(sauce|juice|pie|store|watch|tv|music|pay)\b',
            r'\bmeta\s+(description|tag|data)\b',
            r'\btarget\s+(audience|market|price|date)\b',
            r'\bford\s+(truck|car|vehicle|dealership)\b',
            r'\bdelta\s+(variant|covid|change|difference)\b',
            r'\bsquare\s+(feet|foot|meter|inch|root)\b',
            r'\bblock\s+(chain|party|building|city)\b',
            r'\bgeneral\s+(discussion|question|advice)\b',
            r'\bamerican\s+(citizen|people|culture|dream)\b',
            r'\bunited\s+(states|kingdom|nation)\b',
        ]
        
        # Context patterns that suggest stock discussion
        self.stock_context_patterns = [
            r'\b(stock|shares|ticker|buy|sell|hold|calls|puts|options)\b',
            r'\b(earnings|revenue|profit|loss|eps|pe\s+ratio)\b',
            r'\b(bullish|bearish|moon|crash|pump|dump)\b',
            r'\b(portfolio|investment|trading|yolo|dd)\b',
            r'\$[\d,]+',  # Dollar amounts
            r'\b\d+%\b',  # Percentages
        ]
    
    def extract_stock_tickers(self, text):
        """Extract both ticker symbols and company name mentions with enhanced detection"""
        if not text or pd.isna(text):
            return []
        
        text_lower = text.lower()
        mentioned_tickers = set()
        
        # 1. Extract explicit ticker symbols (existing functionality)
        explicit_tickers = re.findall(self.stock_pattern, str(text).upper())
        mentioned_tickers.update(explicit_tickers)
        
        # 2. Check if text has stock context (required for company name detection)
        has_stock_context = any(
            re.search(pattern, text_lower, re.IGNORECASE) 
            for pattern in self.stock_context_patterns
        )
        
        # 3. Only look for company names if there's stock context
        if has_stock_context:
            for company, ticker in self.company_to_ticker.items():
                # Check if company name is mentioned
                company_pattern = rf'\b{re.escape(company)}\b'
                if re.search(company_pattern, text_lower):
                    # Check for exclusion patterns
                    excluded = any(
                        re.search(exclusion, text_lower, re.IGNORECASE)
                        for exclusion in self.exclusion_patterns
                    )
                    
                    if not excluded:
                        mentioned_tickers.add(ticker)
        
        # Filter out currency codes and common false positives
        valid_tickers = [ticker for ticker in mentioned_tickers if ticker not in self.excluded_tickers]
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(valid_tickers))
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of text using VADER"""
        if not text or pd.isna(text):
            return {
                'compound': 0.0,
                'positive': 0.0,
                'neutral': 0.0,
                'negative': 0.0,
                'label': 'neutral'
            }
        
        scores = self.analyzer.polarity_scores(str(text))
        
        # Convert compound score to label
        compound = scores['compound']
        if compound >= 0.1:
            label = 'positive'
        elif compound <= -0.1:
            label = 'negative'
        else:
            label = 'neutral'
        
        return {
            'compound': compound,
            'positive': scores['pos'],
            'neutral': scores['neu'],
            'negative': scores['neg'],
            'label': label
        }
    
    def analyze_post_sentiment(self, title, selftext=""):
        """Analyze sentiment for a Reddit post (title + content)"""
        # Combine title and content, with title weighted more heavily
        title_text = str(title) if title and not pd.isna(title) else ""
        content_text = str(selftext) if selftext and not pd.isna(selftext) else ""
        
        if not title_text and not content_text:
            return self.analyze_sentiment("")
        
        # If we have both title and content, weight title more heavily (70/30)
        if title_text and content_text and len(content_text.strip()) > 50:
            title_sentiment = self.analyzer.polarity_scores(title_text)
            content_sentiment = self.analyzer.polarity_scores(content_text)
            
            # Weighted average of compound scores
            weighted_compound = (title_sentiment['compound'] * 0.7 + 
                               content_sentiment['compound'] * 0.3)
            
            # Create combined result
            result = self.analyze_sentiment("")  # Get structure
            result['compound'] = weighted_compound
            
            # Recalculate label based on weighted compound
            if weighted_compound >= 0.1:
                result['label'] = 'positive'
            elif weighted_compound <= -0.1:
                result['label'] = 'negative'
            else:
                result['label'] = 'neutral'
            
            return result
        else:
            # Just analyze title if no substantial content
            return self.analyze_sentiment(title_text)
    
    def analyze_stock_mentions(self, title, selftext=""):
        """Analyze sentiment for posts and extract stock tickers"""
        # Extract tickers from both title and content
        title_tickers = self.extract_stock_tickers(title)
        content_tickers = self.extract_stock_tickers(selftext)
        
        # Combine and deduplicate tickers
        all_tickers = list(dict.fromkeys(title_tickers + content_tickers))
        
        # Analyze sentiment of the post
        sentiment = self.analyze_post_sentiment(title, selftext)
        
        return {
            'tickers': all_tickers,
            'sentiment_compound': sentiment['compound'],
            'sentiment_label': sentiment['label'],
            'sentiment_positive': sentiment['positive'],
            'sentiment_neutral': sentiment['neutral'],
            'sentiment_negative': sentiment['negative']
        }
    
    def aggregate_stock_sentiment(self, df):
        """Aggregate sentiment by stock ticker across multiple posts"""
        stock_sentiment = defaultdict(list)
        
        # Collect sentiment scores for each ticker
        for _, row in df.iterrows():
            if 'stock_tickers' in row and row['stock_tickers']:
                # Handle both list objects (runtime) and string representations (from CSV)
                if isinstance(row['stock_tickers'], list):
                    tickers = row['stock_tickers']
                else:
                    # Parse string representation of list (from CSV)
                    try:
                        import ast
                        tickers = ast.literal_eval(str(row['stock_tickers']))
                        if not isinstance(tickers, list):
                            tickers = []
                    except (ValueError, SyntaxError):
                        tickers = []
                
                sentiment_score = row.get('sentiment_compound', 0)
                
                for ticker in tickers:
                    stock_sentiment[ticker].append(sentiment_score)
        
        # Calculate aggregated sentiment for each stock
        aggregated = []
        for ticker, scores in stock_sentiment.items():
            avg_sentiment = sum(scores) / len(scores)
            post_count = len(scores)
            
            # Determine label based on average
            if avg_sentiment >= 0.1:
                label = 'positive'
            elif avg_sentiment <= -0.1:
                label = 'negative'
            else:
                label = 'neutral'
            
            aggregated.append({
                'ticker': ticker,
                'avg_sentiment': round(avg_sentiment, 3),
                'sentiment_label': label,
                'post_count': post_count,
                'total_sentiment': round(sum(scores), 3)
            })
        
        # Sort by post count (popularity) then by sentiment
        aggregated.sort(key=lambda x: (x['post_count'], abs(x['avg_sentiment'])), reverse=True)
        
        return aggregated

def test_sentiment_analyzer():
    """Test the sentiment analyzer with sample finance posts"""
    analyzer = StockSentimentAnalyzer()
    
    test_posts = [
        {
            'title': 'YOLO $50k into $TSLA calls, to the moon! 🚀',
            'selftext': 'This is going to be epic, Tesla is going to crush earnings!'
        },
        {
            'title': 'Lost everything on $GME options, need advice',
            'selftext': 'I feel terrible, down 80% on my portfolio. What should I do?'
        },
        {
            'title': 'What do you think about $AAPL for long term?',
            'selftext': 'Looking for opinions on Apple stock for 5-year hold.'
        },
        {
            'title': '$NVDA earnings beat expectations!',
            'selftext': ''
        }
    ]
    
    print("=== Testing Stock Sentiment Analyzer ===")
    print()
    
    results = []
    for i, post in enumerate(test_posts, 1):
        result = analyzer.analyze_stock_mentions(post['title'], post['selftext'])
        results.append(result)
        
        print(f"Post {i}: {post['title'][:50]}...")
        print(f"  Tickers: {result['tickers']}")
        print(f"  Sentiment: {result['sentiment_compound']:.3f} ({result['sentiment_label']})")
        print()
    
    # Test aggregation
    print("=== Testing Aggregation ===")
    df = pd.DataFrame([
        {
            'stock_tickers': ['TSLA'],
            'sentiment_compound': 0.8
        },
        {
            'stock_tickers': ['TSLA', 'AAPL'],
            'sentiment_compound': 0.5
        },
        {
            'stock_tickers': ['AAPL'],
            'sentiment_compound': -0.2
        }
    ])
    
    aggregated = analyzer.aggregate_stock_sentiment(df)
    for stock in aggregated:
        print(f"{stock['ticker']}: {stock['avg_sentiment']:.3f} ({stock['sentiment_label']}) - {stock['post_count']} posts")

if __name__ == "__main__":
    test_sentiment_analyzer()