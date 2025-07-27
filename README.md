# Reddit Insights Platform

AI-powered Reddit content analysis and dashboard generator for tracking community discussions and trends.

## Features

- **Multi-Category Support**: Finance and Entertainment (with expansion roadmap)
- **Real-time Data Extraction**: Automated Reddit post collection via PRAW
- **AI Classification**: Content categorization and summarization using Groq API
- **Interactive Dashboard**: HTML-based interface with search, filtering, and visualization
- **Modular Architecture**: Easily extensible for new categories and subreddits

## Quick Start

### Prerequisites
```bash
pip install praw pandas flask flask-cors python-dotenv groq
```

### Setup
1. Create Reddit app at https://www.reddit.com/prefs/apps
2. Get Groq API key from https://console.groq.com/keys
3. Configure environment:
```bash
cp .env.example .env
# Edit .env with your API keys
```

### Usage
```bash
# Generate complete dataset (recommended)
python services/generate_all_data.py

# Optional: Start AI summarization service
python services/ai_summarizer.py

# Open dashboard
open assets/reddit_dashboard.html
```

## Architecture

```
reddit-insights/
├── extractors/          # Reddit data collection
├── classifiers/         # Content categorization
├── services/           # AI processing and pipeline
├── utils/              # Dashboard generation and utilities
├── prompts/            # AI prompt templates
└── assets/             # Generated data and dashboard
```

## Supported Categories

### Finance
- **Subreddits**: wallstreetbets, investing, stocks, cryptocurrency, personalfinance
- **Classifications**: Analysis, News, Trading Stories, Questions, Discussion, Memes

### Entertainment  
- **Subreddits**: movies, television, netflix, entertainment
- **Classifications**: Reviews, News, Discussions, Recommendations

## Configuration

Categories and subreddits are easily configurable through the modular extractor and classifier system. Each category has its own:
- Extractor module for data collection
- Classifier for content categorization  
- Prompt template for AI processing

## License

MIT License