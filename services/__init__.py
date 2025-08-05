# Services package for AI and pipeline operations
# Import only working modules
try:
    from .ai_summarizer import RedditSummarizer
    AI_SUMMARIZER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: AI summarizer not available: {e}")
    AI_SUMMARIZER_AVAILABLE = False

try:
    from .simple_update_pipeline import refresh_data
    SIMPLE_PIPELINE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Simple pipeline not available: {e}")
    SIMPLE_PIPELINE_AVAILABLE = False

# Don't import unified_update_pipeline or generate_all_data due to dependency issues

__all__ = []
if AI_SUMMARIZER_AVAILABLE:
    __all__.append('RedditSummarizer')
if SIMPLE_PIPELINE_AVAILABLE:
    __all__.append('refresh_data')