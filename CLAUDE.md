# Claude Development Guide

## Project Overview
Reddit Insights Platform - AI-powered content analysis and dashboard generator for tracking Reddit community discussions and trends across Finance, Entertainment, and Travel categories (Travel Tips, Nature & Adventure, Regional Travel).

## Key Commands

### Data Generation
```bash
# Generate complete dataset (daily + weekly for all categories)
python services/generate_all_data.py

# Start AI summarization service
python services/ai_summarizer.py

# Individual pipeline runs (if needed)
python services/unified_update_pipeline.py --weekly
python services/unified_update_pipeline.py --daily
```

### Development
```bash
# Generate dashboard only
python utils/dashboard_generator.py

# Start live comment API (for "View Comments" feature)
python utils/live_comment_fetcher.py

# View dashboard
open assets/reddit_dashboard.html
```

## Project Structure

- **extractors/**: Reddit data collection modules (5 categories: finance, entertainment, travel_tips, nature_adventure, regional_travel)
- **classifiers/**: Content categorization with subcategory logic for all 5 categories
- **services/**: Core pipeline and AI processing (unified_update_pipeline.py, ai_summarizer.py, generate_all_data.py)
- **utils/**: Dashboard generation and utilities (dashboard_generator.py, popularity_ranker.py)
- **prompts/**: AI prompt templates for all 5 categories
- **assets/**: Generated data files and HTML dashboard

## Configuration Files

- **.env**: API keys (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, GROQ_API_KEY)
- **assets/unified_pipeline_state_*.json**: Pipeline state tracking
- **assets/*_posts.csv**: Generated data files

## Testing
Currently no automated tests - manual verification via dashboard output.

## API Efficiency & Performance

### Reddit API Call Structure
- **Efficient Design**: 1 API call per subreddit (for posts only) = ~76 total calls during extraction
- **On-Demand Comments**: Comments fetched only when users click "View Top Comments" (3 comments max per request)
- **Live Comment API**: Separate service on port 5001 for real-time comment fetching
- **Zero Rate Limiting**: No bulk comment fetching during extraction prevents 429 errors
- **Parallel Processing**: All 5 categories extract simultaneously using ThreadPoolExecutor

### Category Breakdown
- **Finance**: ~17 subreddits
- **Entertainment**: ~22 subreddits  
- **Travel Tips**: 10 subreddits (General, Solo Travel, Budget)
- **Nature & Adventure**: 11 subreddits (Backpacking, Camping, Hiking)
- **Regional Travel**: 16 subreddits (Europe, Asia, Americas, Oceania & Africa)

## Common Issues
- Rate limiting: Built-in 5-second delays between API calls
- API key errors: Check .env configuration
- Missing data: Ensure Reddit API credentials are valid
- Long initial runs: First-time extraction from 37 new travel subreddits takes time

## Development Notes
- Modular architecture allows easy addition of new categories
- Each category requires: extractor, classifier, and prompt modules
- Dashboard automatically detects and displays available data files
- On-demand comments: All categories use live comment fetching to eliminate API rate limiting