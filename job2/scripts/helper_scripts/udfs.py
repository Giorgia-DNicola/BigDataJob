def calculate_percent_change(first_value, last_value):
    if first_value == 0:
        return None  # Prevent division by zero
    return ((last_value - first_value) / first_value) * 100

def extract_year(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, '%Y-%m-%d').year

def calculate_volume(volume_list):
    return sum(volume_list) / len(volume_list) if volume_list else None

# More functions as needed for min, max, etc.
