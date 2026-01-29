import random
import json
from typing import Dict, Any, List, Optional

random.seed(42)

class ErrorInjector:
    """
    Inject realistic errors into event stream
    Rates: 0.5% malformed JSON, 0.1% missing fields, 1% nulls
    """
    
    def __init__(self) -> None:
        self.error_rates = {
            'malformed_json': 0.005,
            'missing_fields': 0.001,
            'null_values': 0.001
        }
    
    def inject_errors(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]] | str:
        """
        Apply error injection to event
        
        Args:
            event: Clean event dictionary
            
        Returns:
            Modified event or None if event should be malformed
        """
        
        # Missing required fields
        if random.random() < self.error_rates['missing_fields']:
            required_fields: List[str] = ['order_id', 'event_timestamp']
            field_to_remove: str = random.choice(required_fields)
            event.pop(field_to_remove, None)
        
        # Null values in non-nullable fields
        if random.random() < self.error_rates['null_values']:
            nullable_field= random.choice(['rider_id', 'restaurant_id'])
            event[nullable_field] = None
        
        # Malformed JSON 
        if random.random() < self.error_rates['malformed_json']:
            truncated = json.dumps(event)[:-5] + '}' # Truncate JSON
            return truncated 
        
        return event
    