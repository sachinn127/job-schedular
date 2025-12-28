"""
CRON Expression Parser
Parses and validates CRON expressions with seconds support
"""

import re
from typing import Tuple, List


class CronParser:
    """
    Parses CRON expressions in the format:
    second minute hour day month dayofweek
    
    Example: "31 10-15 1 * * MON-FRI"
    Means: At 31st second of every minute between 01:10-01:15 AM, 
           every day of month, every month, Monday to Friday
    """
    
    # Valid ranges for each field
    RANGES = {
        'second': (0, 59),
        'minute': (0, 59),
        'hour': (0, 23),
        'day': (1, 31),
        'month': (1, 12),
        'dayofweek': (0, 6)  # 0=Sunday, 6=Saturday
    }
    
    # Month names
    MONTHS = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    # Day names
    DAYS = {
        'SUN': 0, 'MON': 1, 'TUE': 2, 'WED': 3,
        'THU': 4, 'FRI': 5, 'SAT': 6
    }
    
    def __init__(self):
        pass
    
    def parse(self, cron_expression: str) -> Tuple[bool, str, dict]:
        """
        Parse and validate a CRON expression
        
        Returns:
            (is_valid, error_message, parsed_fields)
        """
        try:
            parts = cron_expression.strip().split()
            
            if len(parts) != 6:
                return False, "CRON expression must have 6 fields: second minute hour day month dayofweek", {}
            
            fields = {
                'second': parts[0],
                'minute': parts[1],
                'hour': parts[2],
                'day': parts[3],
                'month': parts[4],
                'dayofweek': parts[5]
            }
            
            # Validate each field
            for field_name, field_value in fields.items():
                is_valid, error = self._validate_field(field_name, field_value)
                if not is_valid:
                    return False, f"Invalid {field_name} field: {error}", {}
            
            return True, "", fields
            
        except Exception as e:
            return False, f"Error parsing CRON expression: {str(e)}", {}
    
    def _validate_field(self, field_name: str, field_value: str) -> Tuple[bool, str]:
        """Validate a single CRON field"""
        
        if field_value == '*':
            return True, ""
        
        min_val, max_val = self.RANGES[field_name]
        
        # Handle wildcards with steps: */5
        if field_value.startswith('*/'):
            try:
                step = int(field_value[2:])
                if step <= 0:
                    return False, "Step value must be positive"
                return True, ""
            except ValueError:
                return False, "Invalid step value"
        
        # Handle ranges: 1-5 or 1-5/2
        if '-' in field_value and ',' not in field_value:
            return self._validate_range(field_name, field_value)
        
        # Handle lists: 1,2,3 or 1-5,10-15
        if ',' in field_value:
            parts = field_value.split(',')
            for part in parts:
                if '-' in part:
                    is_valid, error = self._validate_range(field_name, part)
                else:
                    is_valid, error = self._validate_single(field_name, part)
                
                if not is_valid:
                    return False, error
            
            return True, ""
        
        # Single value
        return self._validate_single(field_name, field_value)
    
    def _validate_single(self, field_name: str, value: str) -> Tuple[bool, str]:
        """Validate a single value"""
        
        min_val, max_val = self.RANGES[field_name]
        
        try:
            # Handle day/month names
            if field_name == 'month':
                if value.upper() in self.MONTHS:
                    num_val = self.MONTHS[value.upper()]
                else:
                    num_val = int(value)
            elif field_name == 'dayofweek':
                if value.upper() in self.DAYS:
                    num_val = self.DAYS[value.upper()]
                else:
                    num_val = int(value)
            else:
                num_val = int(value)
            
            if not (min_val <= num_val <= max_val):
                return False, f"Value must be between {min_val} and {max_val}"
            
            return True, ""
        
        except ValueError:
            return False, f"Invalid value: {value}"
    
    def _validate_range(self, field_name: str, range_str: str) -> Tuple[bool, str]:
        """Validate a range like 1-5 or 1-5/2"""
        
        if '/' in range_str:
            range_part, step_part = range_str.split('/')
            try:
                step = int(step_part)
                if step <= 0:
                    return False, "Step value must be positive"
            except ValueError:
                return False, "Invalid step value"
        else:
            range_part = range_str
        
        if '-' not in range_part:
            return False, "Invalid range format"
        
        parts = range_part.split('-')
        if len(parts) != 2:
            return False, "Invalid range format"
        
        # Validate start and end
        is_valid_start, error = self._validate_single(field_name, parts[0])
        if not is_valid_start:
            return False, f"Invalid range start: {error}"
        
        is_valid_end, error = self._validate_single(field_name, parts[1])
        if not is_valid_end:
            return False, f"Invalid range end: {error}"
        
        # Check that start <= end
        try:
            min_val = int(parts[0]) if field_name not in ['month', 'dayofweek'] else \
                      (self.MONTHS.get(parts[0].upper()) or int(parts[0]))
            max_val = int(parts[1]) if field_name not in ['month', 'dayofweek'] else \
                      (self.MONTHS.get(parts[1].upper()) or int(parts[1]))
            
            if min_val > max_val:
                return False, "Range start must be less than or equal to end"
        except:
            pass
        
        return True, ""
    
    def get_next_execution(self, cron_expression: str, from_time=None):
        """
        Calculate next execution time for a CRON expression
        This is a simplified version; for production use APScheduler's logic
        """
        is_valid, error, fields = self.parse(cron_expression)
        
        if not is_valid:
            raise ValueError(f"Invalid CRON expression: {error}")
        
        # This would require more complex logic to calculate next execution
        # In production, we rely on APScheduler for this
        return None
    
    @staticmethod
    def examples() -> List[dict]:
        """Get example CRON expressions"""
        return [
            {
                'expression': '0 0 * * * *',
                'description': 'Every hour at minute 0'
            },
            {
                'expression': '0 0 0 * * *',
                'description': 'Every day at midnight'
            },
            {
                'expression': '0 0 0 1 * *',
                'description': 'First day of every month at midnight'
            },
            {
                'expression': '0 0 0 * * MON',
                'description': 'Every Monday at midnight'
            },
            {
                'expression': '31 10-15 1 * * MON-FRI',
                'description': 'At 31st second of every minute between 01:10-01:15 AM, weekdays only'
            },
            {
                'expression': '*/5 * * * * *',
                'description': 'Every 5 seconds'
            },
            {
                'expression': '0 */15 * * * *',
                'description': 'Every 15 minutes'
            }
        ]