import calendar
from datetime import datetime


def unix_timestamp(date, default=None, default_ts=0):
    _date = date
    if date is None:
        _date = datetime.fromtimestamp(default_ts) if default is None else default
    return calendar.timegm(_date.timetuple())
