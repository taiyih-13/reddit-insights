# Claude Development Guide

## Project Overview
Reddit Insights Platform - AI-powered content analysis and dashboard generator for tracking Reddit community discussions and trends across 3 unified categories: Finance, Entertainment, and Travel.

**🚀 NOW FULLY DATABASE-POWERED**: All data operations use Supabase database - zero CSV dependencies!

## Key Commands

### Data Generation (Database-First)
```bash  
# ULTRA-OPTIMIZED FULL REGENERATION (70%+ faster - recommended)
python services/ultra_optimized_database_pipeline.py
python services/ultra_optimized_database_pipeline.py --force  # Force refresh, ignore all caches

# OPTIMIZED FULL REGENERATION (50%+ faster)
python services/optimized_database_generate_all_data.py
python services/optimized_database_generate_all_data.py --force  # Force refresh, ignore cache

# LEGACY FULL REGENERATION (slower, for fallback)
python services/database_generate_all_data.py

# INCREMENTAL UPDATE (recommended for daily use - 10x faster)
python services/incremental_database_update.py week   # Add new weekly posts, remove expired
python services/incremental_database_update.py day    # Add new daily posts, remove expired

# Advanced: Manual pipeline runs
python services/ultra_optimized_database_pipeline.py  # Ultra-optimized with smart caching
python services/optimized_database_pipeline.py        # Single-pass optimized extraction
python services/database_unified_pipeline.py week    # Full weekly extraction (legacy)
python services/database_unified_pipeline.py day     # Full daily extraction (legacy)
```

### Services
```bash
# Start AI summarization service  
python services/ai_summarizer.py

# Start live comment API (for "View Comments" feature)
python utils/live_comment_fetcher.py

# Start incremental update API (for "Update Feed" button)
python services/incremental_database_update.py --api
```

### Development  
```bash
# Generate dashboard from database (original design)
python utils/original_style_database_dashboard.py

# View dashboard (now includes "Update Feed" button)
open assets/reddit_dashboard.html
```

### Dashboard Features
- **"Update Feed" Button**: Click next to "Reddit Insights" title for one-click incremental updates
- **Real-time Progress**: Shows "Added X posts, Removed Y posts" with auto-refresh
- **Smart Updates**: Only fetches new posts since last update (10x faster than full regeneration)
- **Category Integration**: Updates all sentiment widgets and category carousels

### Individual Domain Extraction (Database-First)
```bash
# Extract specific domains directly to database
python extractors/finance_database_extractor.py week
python extractors/entertainment_database_extractor.py week  
python extractors/travel_database_extractor.py week
```

## Project Structure

- **extractors/**: Database-first Reddit data collection modules (finance_database_extractor.py, entertainment_database_extractor.py, travel_database_extractor.py)
- **classifiers/**: Content categorization with subcategory logic for all 3 categories
- **services/**: Database-first pipeline and AI processing (database_unified_pipeline.py, database_generate_all_data.py, enhanced_database_service.py, ai_summarizer.py)
- **utils/**: Database dashboard generation (original_style_database_dashboard.py, live_comment_fetcher.py, sentiment analyzers)
- **prompts/**: AI prompt templates for all 3 categories
- **assets/**: Generated HTML dashboard (reddit_dashboard.html) - all data sourced from Supabase

## Configuration Files

- **.env**: API keys (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, GROQ_API_KEY, SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY)
- **Database**: All data stored in Supabase posts table with computed fields

## Testing
Currently no automated tests - manual verification via dashboard output.

## API Efficiency & Performance

### Reddit API Call Structure
- **Efficient Design**: 1 API call per subreddit (for posts only) = ~50 total calls during extraction
- **On-Demand Comments**: Comments fetched only when users click "View Top Comments" (3 comments max per request)
- **Live Comment API**: Separate service on port 5001 for real-time comment fetching
- **Zero Rate Limiting**: No bulk comment fetching during extraction prevents 429 errors
- **Parallel Processing**: All 3 categories extract simultaneously using ThreadPoolExecutor

### Category Breakdown
- **Finance**: ~17 subreddits (stocks, investing, personal finance)
- **Entertainment**: ~22 subreddits (movies, TV, gaming, music)
- **Travel**: ~40 subreddits combining regional travel and travel tips
  - **Regional**: Europe, Asia, Americas, Oceania & Africa (16+ subreddits)
  - **Tips & Advice**: General, Solo Travel, Budget Travel, Backpacking (10+ subreddits)

## Common Issues
- Rate limiting: Built-in 5-second delays between API calls
- API key errors: Check .env configuration
- Missing data: Ensure Reddit API credentials are valid
- Long initial runs: First-time extraction from ~40 travel subreddits takes time

## Development Notes
- Modular architecture allows easy addition of new categories
- Each category requires: extractor, classifier, and prompt modules
- Dashboard automatically detects and displays available data files
- On-demand comments: All categories use live comment fetching to eliminate API rate limiting