import json
from datetime import datetime
import re


def find_all_field_names(filename):
    """
    Read the file and collect ALL field names that appear.
    Like making a set of every column name you see.
    """
    all_fields = set()  # Use set to avoid duplicates
    
    with open(filename, 'r') as file:
        for line in file:
            try:
                record = json.loads(line)
                # Add all the keys (field names) from this record
                all_fields.update(record.keys())
            except:
                continue
    
    return all_fields


def categorize_fields(all_fields):
    """
    Sort field names into categories based on what they contain.
    
    Example:
    - "event_id" contains "id" → goes into id_fields
    - "timestamp" contains "time" → goes into timestamp_fields
    """
    categories = {
        'id_fields': [],
        'timestamp_fields': [],
        'user_fields': [],
        'event_type_fields': []
    }
    
    for field in all_fields:
        field_lower = field.lower()  # Make lowercase for easier matching
        
        # If field name contains "id", it's probably an ID
        if 'id' in field_lower:
            categories['id_fields'].append(field)
        
        # If field name contains "time" or "date", it's probably a timestamp
        if 'time' in field_lower or 'date' in field_lower or 'created' in field_lower or 'occurred' in field_lower:
            categories['timestamp_fields'].append(field)
        
        # If field name contains "user" or "customer", it's probably a user ID
        if 'user' in field_lower or 'customer' in field_lower:
            categories['user_fields'].append(field)
        
        # If field name contains "type" or "event" or "action", it's probably the event type
        if 'type' in field_lower or 'event' in field_lower or 'action' in field_lower:
            categories['event_type_fields'].append(field)
    
    return categories


def get_id(record, id_fields, line_number):
    """
    Try to find an ID in the record.
    Check each field in id_fields until we find one with a value.
    If we can't find any, make up an ID.
    """
    for field in id_fields:
        if field in record and record[field]:
            return str(record[field])
    
    # No ID found, so generate one
    return f"generated_{line_number}"


def get_timestamp(record, timestamp_fields):
    """
    Try to find a timestamp and convert it to standard format (ISO 8601).
    
    Handles different formats:
    - "2025-08-01T00:04:25.122Z" (already good)
    - 1754203335 (unix timestamp in seconds)
    - 1754203335000 (unix timestamp in milliseconds)
    - "2025-08-02 19:40:11" (space-separated)
    """
    for field in timestamp_fields:
        value = record.get(field)
        
        if not value or value == 'invalid-date':
            continue
        
        try:
            # Already in ISO format? Just return it
            if isinstance(value, str) and 'T' in value:
                return value
            
            # Is it a big number? Probably unix timestamp in milliseconds
            if isinstance(value, (int, float)) and value > 1000000000000:
                return datetime.fromtimestamp(value / 1000).isoformat() + 'Z'
            
            # Is it a smaller number? Probably unix timestamp in seconds
            if isinstance(value, (int, float)) and value > 1000000000:
                return datetime.fromtimestamp(value).isoformat() + 'Z'
            
            # Does it have a space? Like "2025-08-02 19:40:11"
            if isinstance(value, str) and ' ' in value and '-' in value:
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                return dt.isoformat() + 'Z'
        except:
            continue
    
    return None


def get_user(record, user_fields):
    """
    Try to find a user ID in the record.
    Skip empty values and "guest".
    """
    for field in user_fields:
        value = record.get(field)
        if value and value != '' and value != 'guest':
            return str(value)
    
    return None


def get_event_type(record, event_type_fields):
    """
    Try to find what type of event this is.
    Like "click", "purchase", "login", etc.
    """
    for field in event_type_fields:
        if field in record and record[field]:
            return str(record[field])
    
    # Can't find a type field? Try to guess from the data
    if 'login_event' in record:
        return 'login'
    if 'error' in record:
        return 'error'
    if 'transaction_type' in record:
        return record['transaction_type']
    
    return None


def get_source(record):
    """
    Figure out where this event came from.
    - vendor: purchase/transaction data
    - device: error logs
    - internal: everything else (clicks, page views, etc.)
    """
    # Has transaction or payment info? → vendor
    if 'transaction_id' in record or 'payment_method' in record or 'order_details' in record:
        return 'vendor'
    
    # Has error with stack trace? → device
    if 'error' in record and 'stack_trace' in record:
        return 'device'
    
    # Everything else → internal
    return 'internal'


def map_to_schema(record, line, line_number, field_categories):
    """
    Take a raw record and convert it to our standard format.
    
    Returns:
    - A clean dictionary if successful
    - None if it fails (with a list of errors)
    """
    errors = []
    
    # Extract each piece of information
    event_id = get_id(record, field_categories['id_fields'], line_number)
    timestamp = get_timestamp(record, field_categories['timestamp_fields'])
    user_id = get_user(record, field_categories['user_fields'])
    event_type = get_event_type(record, field_categories['event_type_fields'])
    source = get_source(record)
    
    # Check if we got the required fields
    if not timestamp:
        errors.append("Missing timestamp")
    
    if not event_type:
        errors.append("Missing event type")
    
    # If something is missing, return None and the errors
    if errors:
        return None, errors
    
    # Build the clean event
    unified_event = {
        'id': event_id,
        'timestamp': timestamp,
        'source': source,
        'eventType': event_type,
        'payload': record
    }
    
    # Add user if we found one
    if user_id:
        unified_event['userId'] = user_id
    
    return unified_event, []


def main():
    filename = 'events.jsonl'
    
    # Step 1: Find all field names
    print("Finding all field names...")
    all_fields = find_all_field_names(filename)
    
    # Step 2: Sort them into categories
    field_categories = categorize_fields(all_fields)
    
    print("\nFound these fields:")
    for category, fields in field_categories.items():
        print(f"  {category}: {fields}")
    
    # Step 3: Process each line
    print("\nProcessing events...")
    valid_events = []
    invalid_events = []
    
    with open(filename, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                unified_event, errors = map_to_schema(record, line, line_number, field_categories)
                
                if unified_event:
                    valid_events.append(unified_event)
                    print(f"✓ Line {line_number}")
                else:
                    invalid_events.append({'line': line_number, 'errors': errors})
                    print(f"✗ Line {line_number}: {errors}")
            except:
                print(f"✗ Line {line_number}: Bad JSON")
    
    # Step 4: Show results
    print(f"\n{'='*50}")
    print(f"Valid: {len(valid_events)}")
    print(f"Invalid: {len(invalid_events)}")
    print(f"Success rate: {len(valid_events)/(len(valid_events)+len(invalid_events))*100:.1f}%")
    
    # Step 5: Save the good ones
    with open('unified_events.json', 'w') as f:
        json.dump(valid_events, f, indent=2)
    
    print(f"\nSaved {len(valid_events)} events to unified_events.json")


if __name__ == "__main__":
    main()