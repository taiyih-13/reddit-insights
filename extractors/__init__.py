# Reddit data extraction package
# Import working extractors only
try:
    from .finance_balanced_extractor import FinanceBalancedExtractor
    FINANCE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Finance extractor not available: {e}")
    FINANCE_AVAILABLE = False

try:
    from .entertainment_balanced_extractor import EntertainmentBalancedExtractorV2
    ENTERTAINMENT_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Entertainment extractor not available: {e}")
    ENTERTAINMENT_AVAILABLE = False

try:
    from .travel_balanced_extractor import TravelBalancedExtractor
    TRAVEL_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Travel extractor not available: {e}")
    TRAVEL_AVAILABLE = False

__all__ = []
if FINANCE_AVAILABLE:
    __all__.append('FinanceBalancedExtractor')
if ENTERTAINMENT_AVAILABLE:
    __all__.append('EntertainmentBalancedExtractorV2') 
if TRAVEL_AVAILABLE:
    __all__.append('TravelBalancedExtractor')