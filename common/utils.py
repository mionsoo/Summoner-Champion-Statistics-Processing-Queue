from datetime import datetime, timedelta


def get_current_datetime():
    return datetime.now() + timedelta(hours=9)
