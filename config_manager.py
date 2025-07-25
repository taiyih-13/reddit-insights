#!/usr/bin/env python3
"""
Configuration Manager
Centralized configuration management for all Reddit workflows
"""

import json
import os
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class DomainConfig:
    """Configuration for a specific domain (finance, entertainment, etc.)"""
    name: str
    subreddits: List[str]
    min_popularity_threshold: int
    category_minimums: Dict[str, int]
    classification_rules: Dict[str, Any]
    
class ConfigManager:
    """
    Centralized configuration manager for all Reddit data workflows
    """
    
    def __init__(self, config_file='reddit_config.json'):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self):
        """Load configuration from file or create default"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                print(f"✅ Loaded configuration from {self.config_file}")
                return config
            except Exception as e:
                print(f"❌ Error loading config: {e}. Using defaults.")
                return self._create_default_config()
        else:
            print(f"📝 Creating default configuration at {self.config_file}")
            config = self._create_default_config()
            self._save_config(config)
            return config
    
    def _save_config(self, config=None):
        """Save configuration to file"""
        config_to_save = config or self.config
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config_to_save, f, indent=2)
            print(f"💾 Saved configuration to {self.config_file}")
        except Exception as e:
            print(f"❌ Error saving config: {e}")
    
    def _create_default_config(self):
        """Create default configuration with all domains"""
        return {
            "domains": {
                "finance": {
                    "name": "finance",
                    "subreddits": [
                        "wallstreetbets", "investing", "stocks", "SecurityAnalysis",
                        "ValueInvesting", "options", "pennystocks", "daytrading",
                        "SwingTrading", "forex", "cryptocurrency", "Bitcoin",
                        "CryptoMarkets", "thetagang", "SPACs", "financialindependence",
                        "personalfinance"
                    ],
                    "min_popularity_threshold": 300,
                    "category_minimums": {
                        "stocks": 15,
                        "options": 10,
                        "crypto": 10,
                        "trading": 8,
                        "investing": 12,
                        "market_news": 8,
                        "personal_finance": 5
                    },
                    "classification_rules": {
                        "stocks": {
                            "subreddits": ["stocks", "SecurityAnalysis", "ValueInvesting"],
                            "patterns": [
                                r"\\b[A-Z]{1,5}\\b.*\\$",
                                r"stock.*pick",
                                r"equity.*analysis",
                                r"fundamental.*analysis"
                            ],
                            "keywords": ["stock", "equity", "shares", "dividend", "earnings"]
                        },
                        "options": {
                            "subreddits": ["options", "thetagang"],
                            "patterns": [
                                r"call.*option",
                                r"put.*option",
                                r"\\b(calls?|puts?)\\b",
                                r"strike.*price",
                                r"expir.*date"
                            ],
                            "keywords": ["options", "calls", "puts", "strike", "premium", "theta"]
                        },
                        "crypto": {
                            "subreddits": ["cryptocurrency", "Bitcoin", "CryptoMarkets"],
                            "patterns": [
                                r"\\b(bitcoin|btc|ethereum|eth|crypto)\\b",
                                r"blockchain",
                                r"altcoin",
                                r"defi"
                            ],
                            "keywords": ["bitcoin", "crypto", "blockchain", "ethereum", "altcoin"]
                        },
                        "trading": {
                            "subreddits": ["daytrading", "SwingTrading", "forex"],
                            "patterns": [
                                r"day.*trad",
                                r"swing.*trad",
                                r"forex",
                                r"technical.*analysis",
                                r"chart.*pattern"
                            ],
                            "keywords": ["trading", "forex", "technical", "chart", "pattern"]
                        },
                        "investing": {
                            "subreddits": ["investing", "financialindependence"],
                            "patterns": [
                                r"invest.*strateg",
                                r"portfolio",
                                r"asset.*allocation",
                                r"fire.*movement"
                            ],
                            "keywords": ["investing", "portfolio", "asset", "fire", "retirement"]
                        },
                        "market_news": {
                            "patterns": [
                                r"market.*news",
                                r"economic.*data",
                                r"fed.*decision",
                                r"interest.*rate"
                            ],
                            "keywords": ["market", "economy", "fed", "inflation", "gdp"]
                        },
                        "personal_finance": {
                            "subreddits": ["personalfinance"],
                            "patterns": [
                                r"budget",
                                r"debt.*pay",
                                r"credit.*score",
                                r"emergency.*fund"
                            ],
                            "keywords": ["budget", "debt", "credit", "savings", "loan"]
                        }
                    }
                },
                "entertainment": {
                    "name": "entertainment",
                    "subreddits": [
                        "netflix", "hulu", "DisneyPlus", "PrimeVideo", "HBOMax", "AppleTVPlus",
                        "movies", "television", "letterboxd", "anime", "animesuggest",
                        "horror", "horrormovies", "MovieSuggestions", "televisionsuggestions",
                        "NetflixBestOf", "documentaries", "tipofmytongue", "ifyoulikeblank",
                        "criterion", "truefilm", "flicks"
                    ],
                    "min_popularity_threshold": 100,
                    "category_minimums": {
                        "streaming_platforms": 8,
                        "movies": 12,
                        "tv_shows": 10,
                        "genres": 6,
                        "recommendations": 8,
                        "reviews_discussion": 5
                    },
                    "classification_rules": {
                        "streaming_platforms": {
                            "subreddits": ["netflix", "hulu", "DisneyPlus", "PrimeVideo", "HBOMax", "AppleTVPlus"],
                            "patterns": [
                                r"netflix.*show",
                                r"prime.*video",
                                r"disney.*plus",
                                r"hbo.*max"
                            ],
                            "keywords": ["netflix", "hulu", "disney+", "prime video", "hbo max", "apple tv"]
                        },
                        "movies": {
                            "subreddits": ["movies", "letterboxd", "criterion", "truefilm", "flicks"],
                            "patterns": [
                                r"movie.*review",
                                r"film.*discuss",
                                r"cinema",
                                r"director.*cut"
                            ],
                            "keywords": ["movie", "film", "cinema", "director", "actor", "actress"]
                        },
                        "tv_shows": {
                            "subreddits": ["television"],
                            "patterns": [
                                r"tv.*show",
                                r"television.*series",
                                r"episode.*discuss",
                                r"season.*finale"
                            ],
                            "keywords": ["tv show", "television", "series", "episode", "season"]
                        },
                        "genres": {
                            "subreddits": ["horror", "horrormovies", "anime", "animesuggest", "documentaries"],
                            "patterns": [
                                r"horror.*movie",
                                r"anime.*recommend",
                                r"documentary.*about",
                                r"genre.*preference"
                            ],
                            "keywords": ["horror", "anime", "documentary", "thriller", "comedy", "drama"]
                        },
                        "recommendations": {
                            "subreddits": ["MovieSuggestions", "televisionsuggestions", "NetflixBestOf", "ifyoulikeblank"],
                            "patterns": [
                                r"recommend.*movie",
                                r"suggest.*show",
                                r"if.*you.*like",
                                r"similar.*to"
                            ],
                            "keywords": ["recommend", "suggest", "similar", "if you like", "best of"]
                        },
                        "reviews_discussion": {
                            "subreddits": ["tipofmytongue"],
                            "patterns": [
                                r"review.*of",
                                r"what.*did.*think",
                                r"discuss.*episode",
                                r"tip.*of.*tongue"
                            ],
                            "keywords": ["review", "discussion", "opinion", "thoughts", "tip of my tongue"]
                        }
                    }
                }
            },
            "global_settings": {
                "default_base_limit": 100,
                "parallel_extraction": True,
                "filter_media_content": True,
                "filter_low_quality": True,
                "max_selftext_length": 1000
            }
        }
    
    def get_domain_config(self, domain_name: str) -> DomainConfig:
        """Get configuration for a specific domain"""
        if domain_name not in self.config["domains"]:
            raise ValueError(f"Domain '{domain_name}' not found in configuration")
        
        domain_data = self.config["domains"][domain_name]
        return DomainConfig(
            name=domain_data["name"],
            subreddits=domain_data["subreddits"],
            min_popularity_threshold=domain_data["min_popularity_threshold"],
            category_minimums=domain_data["category_minimums"],
            classification_rules=domain_data["classification_rules"]
        )
    
    def get_global_setting(self, setting_name: str, default=None):
        """Get a global setting value"""
        return self.config.get("global_settings", {}).get(setting_name, default)
    
    def update_domain_config(self, domain_name: str, **kwargs):
        """Update configuration for a specific domain"""
        if domain_name not in self.config["domains"]:
            raise ValueError(f"Domain '{domain_name}' not found in configuration")
        
        for key, value in kwargs.items():
            if key in self.config["domains"][domain_name]:
                self.config["domains"][domain_name][key] = value
                print(f"✅ Updated {domain_name}.{key}")
            else:
                print(f"⚠️  Unknown setting: {domain_name}.{key}")
        
        self._save_config()
    
    def add_domain(self, domain_name: str, config_data: Dict[str, Any]):
        """Add a new domain configuration"""
        if domain_name in self.config["domains"]:
            print(f"⚠️  Domain '{domain_name}' already exists. Use update_domain_config to modify.")
            return False
        
        # Validate required fields
        required_fields = ["name", "subreddits", "min_popularity_threshold", "category_minimums", "classification_rules"]
        for field in required_fields:
            if field not in config_data:
                print(f"❌ Missing required field: {field}")
                return False
        
        self.config["domains"][domain_name] = config_data
        self._save_config()
        print(f"✅ Added new domain: {domain_name}")
        return True
    
    def list_domains(self):
        """List all configured domains"""
        print("\n📋 Configured Domains:")
        print("=" * 40)
        
        for domain_name, config in self.config["domains"].items():
            print(f"\n{domain_name.title()}:")
            print(f"  Subreddits: {len(config['subreddits'])}")
            print(f"  Min threshold: {config['min_popularity_threshold']}")
            print(f"  Categories: {len(config['category_minimums'])}")
    
    def validate_config(self):
        """Validate the current configuration"""
        print("🔍 Validating configuration...")
        
        errors = []
        warnings = []
        
        # Check domains
        if not self.config.get("domains"):
            errors.append("No domains configured")
        
        for domain_name, domain_config in self.config.get("domains", {}).items():
            # Check required fields
            required_fields = ["name", "subreddits", "min_popularity_threshold", "category_minimums", "classification_rules"]
            for field in required_fields:
                if field not in domain_config:
                    errors.append(f"Domain '{domain_name}' missing field: {field}")
            
            # Check subreddits list
            if not isinstance(domain_config.get("subreddits"), list):
                errors.append(f"Domain '{domain_name}' subreddits must be a list")
            elif len(domain_config.get("subreddits", [])) == 0:
                warnings.append(f"Domain '{domain_name}' has no subreddits")
            
            # Check threshold
            if not isinstance(domain_config.get("min_popularity_threshold"), int):
                errors.append(f"Domain '{domain_name}' threshold must be an integer")
        
        # Report results
        if errors:
            print("❌ Configuration Errors:")
            for error in errors:
                print(f"   • {error}")
        
        if warnings:
            print("⚠️  Configuration Warnings:")
            for warning in warnings:
                print(f"   • {warning}")
        
        if not errors and not warnings:
            print("✅ Configuration is valid")
        
        return len(errors) == 0