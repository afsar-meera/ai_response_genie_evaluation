import re
from datetime import datetime, timedelta

def convert_to_date(timestamp):
    # Validate date
    timestamp = timestamp.lower()
    date_pattern = "^[a-z, A-Z]{3}\s[0-9]{1,2}\\,\s[0-9]{4}$"
    if re.match(date_pattern, timestamp):
        date_time_obj = datetime.strptime(timestamp, '%b %d, %Y')
    elif 'yesterday' in timestamp:
        date_time_obj = datetime.today() - timedelta(days=1)
    elif 'days' in timestamp:
        numbers = re.findall(r'^\d{1,2}', timestamp)
        days_ago = int(numbers[0])
        date_time_obj = datetime.today() - timedelta(days=days_ago)
    elif 'one month' in timestamp:
        date_time_obj = datetime.today() - timedelta(days=30)
    elif 'months' in timestamp:
        numbers = re.findall(r'^\d{1,2}', timestamp)
        days_ago = int(numbers[0]) * 30
        date_time_obj = datetime.today() - timedelta(days=days_ago)
    else:
        date_time_obj = datetime.today()

    return date_time_obj.strftime('%m/%d/%Y')

if __name__ == "__main__":
    convert_to_date('yesterday')