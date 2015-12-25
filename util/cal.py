import calendar


def unix_timestamp(date):
    return calendar.timegm(date.timetuple())
