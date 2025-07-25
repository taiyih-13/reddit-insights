# 🚀 Reddit Insights - Enhanced Analytics Platform

> **Version 2.0** - Multi-domain analytics with performance optimization and base architecture

A high-performance Reddit data analytics platform that extracts, processes, and analyzes content from finance and entertainment subreddits using advanced parallel processing, caching, and AI summarization.

## ✨ Features

### 🏗️ **Base Architecture (v2.0)**
- **Modular Design**: Shared base classes eliminate code duplication
- **Configuration-Driven**: Centralized configuration management
- **Multi-Domain Support**: Finance and entertainment analytics
- **Extensible Framework**: Easy to add new domains

### ⚡ **Performance Optimizations**
- **Parallel Processing**: Up to 8x faster data extraction
- **Intelligent Caching**: 1-2 hour TTL with 50-80% hit rates
- **Circuit Breaker Pattern**: Automatic failure recovery
- **Memory Optimization**: Efficient data types and chunked processing

### 🤖 **AI-Powered Insights**
- **Multi-Domain Summarization**: Domain-specific AI analysis
- **Enhanced Flask API**: RESTful endpoints for real-time summaries
- **Groq Integration**: Fast LLM processing with context optimization

### 📊 **Interactive Dashboard**
- **Real-Time Analytics**: Live data filtering and visualization
- **Multi-Domain Views**: Switch between finance and entertainment
- **Category Filtering**: Drill down into specific content types
- **Responsive Design**: Mobile-friendly interface

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Reddit API credentials
- (Optional) Groq API key for AI summaries

### 1. Installation
```bash
git clone <repository>
cd reddit-insights
pip install -r requirements.txt
```

### 2. Environment Setup
```bash
# Generate environment template
python environment_manager.py template

# Copy and edit the template
cp .env.template .env
# Edit .env with your API credentials

# Validate configuration
python environment_manager.py validate
```

### 3. Quick Test Run
```bash
# Test with small dataset (5 posts per subreddit)
python ultimate_pipeline.py
```

### 4. Generate Dashboard
```bash
# Create interactive dashboard
python dashboard_enhanced.py
```

## 📚 Architecture Overview

### Core Components

#### 🏛️ **Base Classes**
- **`BaseRedditExtractor`**: Common Reddit API operations
- **`BaseClassifier`**: Content classification framework  
- **`BasePipeline`**: Pipeline orchestration and state management
- **`ConfigManager`**: Centralized configuration system

#### 🎯 **Domain-Specific Classes**
- **Finance**: `FinanceRedditExtractor`, `FinanceClassifier`
- **Entertainment**: `EntertainmentRedditExtractor`, `EntertainmentClassifier`

#### ⚡ **Performance Classes**
- **`PerformanceRedditExtractor`**: Parallel processing + caching
- **`ResilientRedditExtractor`**: Circuit breaker + retry logic
- **`UltimatePipeline`**: Complete optimized workflow

### Data Flow
```
Raw Reddit Data → Scoring & Filtering → Classification → Dashboard
      ↓              ↓                    ↓             ↓
   Parallel        Popularity         Category       Interactive
   Extraction      Calculation        Assignment       Visualization
```

## 🔧 Configuration

### Environment Variables

#### Required
```bash
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_secret
REDDIT_USER_AGENT=reddit-insights-bot/1.0
```

#### Optional
```bash
GROQ_API_KEY=your_groq_key                    # For AI summaries
MAX_WORKERS=6                                  # Parallel threads
CACHE_TTL_HOURS=2                             # Cache duration
DEFAULT_BASE_LIMIT=100                        # Posts per subreddit
ENABLE_PARALLEL_EXTRACTION=true              # Use parallel processing
ENABLE_CACHING=true                          # Enable caching system
LOG_LEVEL=INFO                               # Logging level
```

### Domain Configuration
The system uses `reddit_config.json` for domain-specific settings:

```json
{
  "domains": {
    "finance": {
      "subreddits": ["wallstreetbets", "investing", "stocks", ...],
      "min_popularity_threshold": 300,
      "category_minimums": {"stocks": 15, "options": 10, ...},
      "classification_rules": {...}
    },
    "entertainment": {
      "subreddits": ["movies", "netflix", "anime", ...],
      "min_popularity_threshold": 100,
      "category_minimums": {"streaming_platforms": 8, ...},
      "classification_rules": {...}
    }
  }
}
```

## 🎮 Usage Examples

### Basic Pipeline Execution
```python
from ultimate_pipeline import UltimatePipeline

# Initialize with performance optimizations
pipeline = UltimatePipeline(max_workers=6)

# Run single domain
result = pipeline.run_single_domain_ultimate('finance', 'week', 100)

# Run both domains in parallel
results = pipeline.run_both_domains_ultimate('week', 100)

# Complete pipeline (all combinations)
all_results = pipeline.run_full_ultimate_pipeline(100)
```

### Custom Configuration
```python
from config_manager import ConfigManager

config = ConfigManager()

# Update domain settings
config.update_domain_config('finance', min_popularity_threshold=500)

# Add new domain
new_domain = {
    "name": "technology",
    "subreddits": ["technology", "programming"],
    "min_popularity_threshold": 200,
    "category_minimums": {"tech_news": 10},
    "classification_rules": {...}
}
config.add_domain('technology', new_domain)
```

### AI Summarization
```python
from ai_summarizer_enhanced import EnhancedRedditSummarizer

summarizer = EnhancedRedditSummarizer()

# Auto-detect domain and generate summary
result = summarizer.generate_summary('stocks', 'weekly')

# Specify domain explicitly
result = summarizer.generate_summary('movies', 'daily', domain='entertainment')
```

### Dashboard Generation
```python
from dashboard_enhanced import EnhancedRedditDashboard

dashboard = EnhancedRedditDashboard()

# Generate interactive HTML dashboard
dashboard.generate_enhanced_html('my_dashboard.html')
```

## 📊 Performance Benchmarks

### Extraction Speed (100 posts per subreddit)
- **Sequential (old)**: ~180 seconds for 39 subreddits
- **Parallel (new)**: ~25 seconds for 39 subreddits
- **With Caching**: ~8 seconds (80% cache hit rate)

### Memory Usage
- **Standard Processing**: ~150MB peak
- **Optimized Processing**: ~95MB peak (37% reduction)
- **Chunked Processing**: Scales linearly with dataset size

### Cache Performance
- **Hit Rate**: 50-80% for repeated runs within TTL
- **Storage**: ~2MB per 1000 posts cached
- **Speed Improvement**: 5-10x faster for cached data

## 🛠️ Development

### Project Structure
```
reddit-insights/
├── 🏛️ Base Architecture
│   ├── base_extractor.py          # Common extraction logic
│   ├── base_classifier.py         # Classification framework
│   ├── base_pipeline.py           # Pipeline orchestration
│   └── config_manager.py          # Configuration system
├── ⚡ Performance Layer
│   ├── performance_extractor.py   # Parallel + caching
│   ├── resilient_extractor.py     # Circuit breaker + retry
│   └── ultimate_pipeline.py       # Complete optimization
├── 🎯 Domain Implementations
│   ├── finance_extractor.py       # Finance-specific logic
│   ├── finance_classifier.py      # Finance categorization
│   ├── entertainment_extractor_new.py
│   └── entertainment_classifier_new.py
├── 🤖 AI & Visualization
│   ├── ai_summarizer_enhanced.py  # Multi-domain AI summaries
│   ├── dashboard_enhanced.py      # Interactive dashboard
│   └── comprehensive_finance_prompt.py
├── 🔧 Utilities
│   ├── environment_manager.py     # Environment setup
│   ├── popularity_ranker_v2.py    # Scoring algorithm
│   └── phase3_verification.py     # System validation
└── 📊 Generated Files
    ├── week_reddit_posts.csv      # Weekly data
    ├── day_reddit_posts.csv       # Daily data
    ├── reddit_config.json         # Domain configuration
    └── reddit_dashboard_enhanced.html
```

### Adding New Domains

1. **Update Configuration**:
```python
config_manager.add_domain('new_domain', {
    "name": "new_domain",
    "subreddits": ["subreddit1", "subreddit2"],
    "min_popularity_threshold": 150,
    "category_minimums": {"category1": 8},
    "classification_rules": {...}
})
```

2. **Create Domain Classes**:
```python
class NewDomainExtractor(BaseRedditExtractor):
    @property
    def subreddits(self):
        return self.domain_config.subreddits
    # ... implement required methods

class NewDomainClassifier(BaseClassifier):
    @property
    def classification_rules(self):
        return self.domain_config.classification_rules
    # ... implement required methods
```

3. **Update Pipeline**:
```python
# Add to UltimatePipeline or create custom pipeline
```

### Running Tests
```bash
# Validate system architecture
python phase3_verification.py

# Test environment configuration
python environment_manager.py validate

# Run benchmark tests
python ultimate_pipeline.py  # Built-in benchmarking
```

## 🔍 Monitoring & Debugging

### System Health Check
```python
from ultimate_pipeline import UltimatePipeline

pipeline = UltimatePipeline()

# Get resilience statistics
pipeline.finance_extractor.get_resilience_stats()
pipeline.entertainment_extractor.get_resilience_stats()

# Get pipeline status
pipeline.finance_pipeline.get_pipeline_status()
```

### Performance Monitoring
```python
# Cache statistics
extractor.get_cache_stats()

# Circuit breaker status
for name, cb in extractor.circuit_breakers.items():
    print(f"{name}: {cb.state}")

# Memory usage
import psutil
process = psutil.Process()
print(f"Memory: {process.memory_info().rss / 1024 / 1024:.1f}MB")
```

## 🤝 Contributing

1. **Architecture**: Follow base class patterns
2. **Performance**: Profile new features with benchmarks
3. **Configuration**: Use ConfigManager for all settings
4. **Testing**: Ensure phase3_verification.py passes
5. **Documentation**: Update this README for new features

## 📄 License

MIT License - see LICENSE file for details.

## 🆘 Support

### Common Issues

1. **API Rate Limits**: Reduce `MAX_WORKERS` or increase delays
2. **Memory Issues**: Enable chunked processing for large datasets
3. **Cache Problems**: Clear cache directory and restart
4. **Configuration Errors**: Run `python environment_manager.py validate`

### Getting Help

- **Environment Setup**: `python environment_manager.py setup`
- **System Validation**: `python phase3_verification.py`
- **Performance Issues**: Check circuit breaker status and cache stats

---

**Built with ❤️ using Python, Reddit API, and AI**