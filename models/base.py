#!/usr/bin/env python3
"""
Base model classes for database entities
Provides common functionality and patterns
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import pandas as pd
from abc import ABC, abstractmethod

@dataclass
class BaseModel:
    """Base class for all database models"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        return asdict(self)
    
    def to_json(self) -> Dict[str, Any]:
        """Convert model to JSON-serializable dictionary"""
        data = self.to_dict()
        
        # Convert datetime objects to ISO strings
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create model instance from dictionary"""
        return cls(**data)

class BaseQuery(ABC):
    """Base class for database queries"""
    
    def __init__(self, db_service):
        self.db = db_service
    
    @abstractmethod
    def execute(self) -> Any:
        """Execute the query"""
        pass
    
    def to_dataframe(self, results: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert query results to DataFrame"""
        if not results:
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        
        # Convert datetime strings back to datetime objects
        datetime_columns = ['created_utc', 'extracted_at', 'updated_at', 'computed_at', 'started_at', 'completed_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df