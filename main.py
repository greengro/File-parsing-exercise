import json
from datetime import datetime


def find_all_field_names(filename):
    """Find all unique field names in the file"""
    all_fields = set()
    
    with open(filename, 'r') as file:
        for line in file:
            try:
                record = json.loads(line)
                all_fields.update(record.keys())
            except:
                continue
    
    return all_fields


def get_id(record, line_number):
    """
    Look for ID fields directly in the record.
    """
    # Just check for fields with 'id' in the name
    for key in record.keys():
        if 'id' in key.lower():
            # Prioritize primary IDs
            if key.lower() in ['id', 'event_id', 'eventid', 'transaction_id']:
                if record[key]:
                    return str(record[key])
    
    # Check again for any ID field
    for key in record.keys():
        if 'id' in key.lower() and record[key]:
            return str(record[key])
    
    return f"generated_{line_number}"


def get_timestamp(record):
    """
    Look for timestamp fields directly in the record.
    """
    # Check each field in the record
    for key, value in record.items():
        key_lower = key.lower()
        
        # Is this field name timestamp-like?
        if not any(pattern in key_lower for pattern in ['time', 'date', 'created', 'occurred', 'ts']):
            continue
        
        if not value or value == 'invalid-date':
            continue
        
        try:
            if isinstance(value, str) and 'T' in value:
                return value
            
            if isinstance(value, (int, float)) and value > 1000000000000:
                return datetime.fromtimestamp(value / 1000).isoformat() + 'Z'
            
            if isinstance(value, (int, float)) and value > 1000000000:
                return datetime.fromtimestamp(value).isoformat() + 'Z'
            
            if isinstance(value, str) and ' ' in value and '-' in value:
                dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                return dt.isoformat() + 'Z'
        except:
            continue
    
    return None


def get_user(record):
    """Look for user fields directly in the record"""
    for key, value in record.items():
        key_lower = key.lower()
        
        # Is this a user-like field?
        if 'user' in key_lower or 'customer' in key_lower:
            if value and value != '' and value != 'guest':
                return str(value)
    
    return None


def get_event_type(record):
    """Look for event type fields directly in the record"""
    for key, value in record.items():
        key_lower = key.lower()
        
        # Is this a type-like field?
        if 'type' in key_lower or 'event' in key_lower or 'action' in key_lower:
            if value:
                return str(value)
    
    # Infer from structure
    if 'login_event' in record:
        return 'login'
    if 'error' in record:
        return 'error'
    if 'transaction_type' in record:
        return record['transaction_type']
    
    return None


def get_source(record):
    """Determine source from record structure"""
    if 'transaction_id' in record or 'payment_method' in record or 'order_details' in record:
        return 'vendor'
    
    if 'error' in record and 'stack_trace' in record:
        return 'device'
    
    return 'internal'


def map_to_schema(record, line, line_number):
    """
    Convert record to unified schema.
    No need to pass field_categories anymore!
    """
    errors = []
    
    # Each function discovers fields on its own
    event_id = get_id(record, line_number)
    timestamp = get_timestamp(record)
    user_id = get_user(record)
    event_type = get_event_type(record)
    source = get_source(record)
    
    # Validate
    if not event_id: 
        errors.append("Missing eventid")
    if not timestamp:
        errors.append("Missing timestamp")
    
    if not event_type:
        errors.append("Missing event type")
    
    if errors:
        return None, errors
    
    # Build unified event
    unified_event = {
        'id': event_id,
        'timestamp': timestamp,
        'source': source,
        'eventType': event_type,
        'payload': record
    }
    
    if user_id:
        unified_event['userId'] = user_id
    
    return unified_event, []


def main():
    filename = 'events.jsonl'
    
    # Optional: Still discover fields for debugging/reporting
    print("Discovering fields in data...")
    all_fields = find_all_field_names(filename)
    print(f"Found {len(all_fields)} unique fields: {sorted(all_fields)}\n")
    
    # Process events
    print("Processing events...")
    valid_events = []
    invalid_events = []
    
    with open(filename, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                
                # No need to pass field_categories anymore!
                unified_event, errors = map_to_schema(record, line, line_number)
                
                if unified_event:
                    valid_events.append(unified_event)
                    print(f"✓ Line {line_number}")
                else:
                    invalid_events.append({
                        'line': line_number,
                        'errors': errors,
                        'record': record
                    })
                    print(f"✗ Line {line_number}: {errors}")
            except:
                print(f"✗ Line {line_number}: Bad JSON")
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Valid: {len(valid_events)}")
    print(f"Invalid: {len(invalid_events)}")
    print(f"Success rate: {len(valid_events)/(len(valid_events)+len(invalid_events))*100:.1f}%")
    
    # Save results
    with open('unified_events.json', 'w') as f:
        json.dump(valid_events, f, indent=2)
    
    if invalid_events:
        with open('invalid_events.json', 'w') as f:
            json.dump(invalid_events, f, indent=2)

    print(f"\nSaved {len(valid_events)} events to unified_events.json")
    if invalid_events:
        print(f"Saved {len(invalid_events)} invalid events to invalid_events.json")
        
    print(f"\nSaved {len(valid_events)} events to unified_events.json")


if __name__ == "__main__":
    main()
