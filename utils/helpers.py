# utils/helpers.py - Helper functions
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
import hashlib
import logging

logger = logging.getLogger(__name__)

def time_function(func):
    """Decorator to time function execution"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}...")
        
        result = func(*args, **kwargs)
        
        elapsed = time.time() - start_time
        logger.info(f"Finished {func.__name__} in {elapsed:.2f} seconds")
        
        return result
    return wrapper

def validate_event(event: Dict[str, Any]) -> bool:
    """Validate GitHub event has required fields"""
    required_fields = ['id', 'type', 'repo', 'actor', 'created_at']
    
    for field in required_fields:
        if field not in event:
            logger.warning(f"Event missing required field: {field}")
            return False
    
    # Check repo structure
    if not isinstance(event.get('repo'), dict) or 'name' not in event['repo']:
        logger.warning("Event has invalid repo structure")
        return False
    
    return True

def generate_event_hash(event: Dict[str, Any]) -> str:
    """Generate hash for event deduplication"""
    event_str = json.dumps(event, sort_keys=True)
    return hashlib.md5(event_str.encode()).hexdigest()

def create_date_range(days: int = 30) -> List[str]:
    """Create list of dates for data collection"""
    dates = []
    end_date = datetime.now()
    
    for i in range(days):
        date = end_date - timedelta(days=i)
        dates.append(date.strftime('%Y-%m-%d'))
    
    return dates

def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """Safely parse JSON string"""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default

def format_large_number(num: int) -> str:
    """Format large numbers with commas"""
    return f"{num:,}"