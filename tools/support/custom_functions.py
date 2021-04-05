from datetime import datetime as dt, timedelta

def last_business_day():
    """
    Function to figure out last business day on a given day to help with the calculation of prices
    """
    # Figure out Last Business Day
    today = dt.today()
    offset = max(1, (today.weekday() + 6) % 7 - 3)
    delta = timedelta(offset)
    most_recent = today - delta

    # Convert to string
    last_business_day_str = most_recent.strftime('%Y-%m-%d')

    return last_business_day_str

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def str_datetime_operation(string_date, no_of_days, date_format):
    datetime_obj = dt.strptime(string_date, date_format) + timedelta(days=no_of_days)
    datetime_str = datetime_obj.strftime(date_format)
    return datetime_str
