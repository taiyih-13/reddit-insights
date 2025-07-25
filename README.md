# Reddit Insights Platform

> **Intelligent content aggregation and analysis across Reddit communities**

A scalable pipeline that transforms Reddit discussions into actionable insights through AI-powered categorization, sentiment analysis, and automated summarization. Currently optimized for **Finance** communities, with architecture designed for expansion into Travel, Food, University, Entertainment, and other verticals.

## 🎯 Vision

This platform represents the foundation for comprehensive Reddit intelligence across diverse interest categories. By combining real-time data extraction, intelligent classification, and AI-driven insights, it provides communities and researchers with deep understanding of social sentiment and trending discussions.

## 🏗️ Architecture Overview

```
📊 REDDIT INSIGHTS PLATFORM
├── 🔄 Data Pipeline
│   ├── Multi-subreddit extraction
│   ├── Intelligent content filtering  
│   ├── Category-specific classification
│   └── Incremental data management
├── 🤖 AI Intelligence Layer
│   ├── Content categorization
│   ├── Engagement scoring
│   ├── AI-powered summarization
│   └── Trend analysis
└── 📈 Visualization & Interface
    ├── Interactive dashboards
    ├── Real-time search & filtering
    ├── Category-specific insights
    └── Temporal analysis (daily/weekly)
```

## 🚀 Current Implementation: Finance Intelligence

### **Supported Communities**
- **Trading**: wallstreetbets, daytrading, SwingTrading, options, thetagang
- **Investment**: investing, ValueInvesting, SecurityAnalysis  
- **Markets**: stocks, pennystocks, SPACs
- **Crypto**: cryptocurrency, Bitcoin, CryptoMarkets
- **Personal Finance**: personalfinance, financialindependence
- **International**: forex

### **Content Classification**
- **Analysis & Education**: DD, tutorials, market analysis
- **Market News & Politics**: Breaking news, policy impacts
- **Personal Trading Stories**: Gains, losses, experiences
- **Questions & Help**: Beginner questions, advice seeking
- **Community Discussion**: General conversation, sentiment
- **Memes & Entertainment**: Social content, humor

### **Key Metrics**
- **Data Volume**: 600+ posts per extraction cycle
- **Update Frequency**: Every 6 hours with incremental updates
- **Data Retention**: 7-day rolling window (weekly), 24-hour (daily)
- **AI Processing**: Category-specific summarization via Groq API
- **Classification Accuracy**: Multi-tier confidence scoring

## 🛠️ Technology Stack

### **Core Pipeline**
- **Python 3.11+**: Primary development language
- **PRAW**: Reddit API integration
- **Pandas**: Data processing and analysis
- **Groq API**: AI-powered content summarization

### **Data Management**
- **CSV Storage**: Lightweight, portable data format
- **JSON State Management**: Pipeline status and scheduling
- **Incremental Updates**: Efficient data synchronization
- **Automated Retention**: Rolling window data management

### **Frontend & Visualization**
- **Pure HTML/CSS/JS**: No framework dependencies
- **Interactive Dashboard**: Real-time filtering and search
- **Responsive Design**: Mobile-friendly interface
- **AI Integration**: On-demand category summarization

## 📋 Installation & Setup

### **Prerequisites**
```bash
# Python dependencies
pip install praw pandas flask flask-cors python-dotenv groq

# Reddit API Setup
# 1. Visit https://www.reddit.com/prefs/apps
# 2. Create new application (script type)
# 3. Note client_id and client_secret

# AI API Setup  
# 1. Get Groq API key from https://console.groq.com/keys
```

### **Environment Configuration**
Create `.env` file:
```env
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret  
REDDIT_USER_AGENT=reddit-insights/1.0
GROQ_API_KEY=your_groq_api_key
```

### **Quick Start**
```bash
# Clone repository
git clone <repository_url>
cd reddit-insights

# Set up environment
cp .env.example .env
# Edit .env with your API keys

# Run single extraction
python update_pipeline.py --week

# Start AI service (in separate terminal)
python start_ai_service.py

# Open dashboard
open reddit_dashboard.html
```

## 🔄 Pipeline Operations

### **Data Extraction Modes**
```bash
# Weekly analysis (7-day window)
python update_pipeline.py --week

# Daily analysis (24-hour window)  
python update_pipeline.py --day

# Continuous monitoring
python update_pipeline.py --continuous
```

### **Scheduling & Automation**
```bash
# Run automated scheduler
python scheduler.py

# Custom scheduling intervals:
# - Extraction: Every 6 hours
# - Full refresh: Daily at 3:00 AM
# - Data retention: Automatic cleanup
```

### **Dashboard Features**
- **Unified Interface**: Single dashboard for all time ranges
- **Interactive Search**: Full-text search across titles and content
- **Category Filtering**: Focus on specific discussion types
- **AI Summarization**: On-demand category insights
- **Sorting Options**: By popularity, upvotes, comments, recency
- **Temporal Analysis**: Toggle between daily/weekly views

## 🎯 Expansion Roadmap

### **Planned Categories**

#### **🧳 Travel Intelligence**
- **Communities**: travel, solotravel, backpacking, digitalnomad
- **Classifications**: Destinations, Tips, Experiences, Planning, Budget
- **Insights**: Trending destinations, seasonal patterns, cost analysis

#### **🍕 Food & Culinary**
- **Communities**: food, recipes, cooking, MealPrepSunday, AskCulinary
- **Classifications**: Recipes, Restaurant Reviews, Techniques, Nutrition
- **Insights**: Trending cuisines, seasonal ingredients, dietary trends

#### **🎓 University & Education**
- **Communities**: college, ApplyingToCollege, GradSchool, studytips
- **Classifications**: Admissions, Academic Life, Career Prep, Resources
- **Insights**: Application trends, university sentiment, academic challenges

#### **🎬 Entertainment & Media**
- **Communities**: movies, television, netflix, MovieSuggestions
- **Classifications**: Reviews, Recommendations, Discussions, News
- **Insights**: Trending content, genre preferences, platform analysis

### **Platform Enhancements**
- **Multi-Category Dashboard**: Unified view across all verticals
- **Cross-Category Analysis**: Identify overlapping trends and communities
- **Advanced AI Models**: Enhanced classification and sentiment analysis
- **API Development**: External access to insights and data
- **Real-time Streaming**: Live discussion monitoring and alerts

## 📊 Current Data Pipeline

### **Extraction Process**
1. **Raw Collection**: Fetch posts from configured subreddits
2. **Quality Filtering**: Apply engagement thresholds (300+ popularity score)
3. **Intelligent Classification**: Multi-tier categorization system
4. **Duplicate Management**: Smart deduplication across time windows
5. **Incremental Updates**: Merge new data with existing datasets
6. **Retention Management**: Automatic cleanup based on time filters

### **AI Processing**
1. **Content Analysis**: Process up to 50 top posts per category
2. **Token Optimization**: Efficient use of API limits (4500/6000 tokens)
3. **Category Summarization**: Generate insights for each discussion type
4. **Confidence Scoring**: Track classification accuracy and reliability

### **Output Generation**
1. **Data Storage**: Structured CSV format for analysis
2. **Dashboard Creation**: Interactive HTML interface
3. **State Management**: Track pipeline status and scheduling
4. **Performance Monitoring**: Log extraction metrics and success rates

## 🔧 Technical Architecture

### **Modular Design**
- **`update_pipeline.py`**: Main orchestration and scheduling
- **`balanced_extractor.py`**: Multi-subreddit data collection
- **`post_classifier.py`**: Content categorization engine
- **`popularity_ranker_v2.py`**: Engagement scoring algorithm
- **`ai_summarizer.py`**: AI-powered content analysis
- **`dashboard_generator_clean.py`**: Interactive visualization

### **Scalability Features**
- **Category-Agnostic Core**: Easily adaptable to new verticals
- **Configurable Subreddits**: Simple addition of new communities
- **Flexible Classification**: Extensible category system
- **API Integration**: Ready for external AI services
- **State Management**: Reliable pipeline recovery and monitoring

## 🤝 Contributing

This project is designed for expansion across multiple Reddit verticals. Contributions welcome for:

- **New Category Implementations**: Travel, Food, Education, Entertainment
- **Enhanced AI Models**: Improved classification and analysis
- **Advanced Visualizations**: Richer dashboard features
- **Performance Optimizations**: Scaling improvements
- **API Development**: External integrations

### **Development Workflow**
1. Fork repository and create feature branch
2. Implement changes with comprehensive testing
3. Update documentation and configuration
4. Submit pull request with detailed description

## 📄 License

MIT License - See LICENSE file for details

## 📞 Support

For questions about implementation, scaling, or new category development:
- Open GitHub Issues for bug reports and feature requests
- Check existing documentation for configuration guidance
- Review code comments for technical implementation details

---

**Built for the future of Reddit intelligence** 🚀  
*Transform community discussions into actionable insights across any vertical*