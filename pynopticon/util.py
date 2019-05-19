import datetime


def timestamp_floor(ts):
    return ts - datetime.timedelta(
        seconds=ts.second,
        microseconds=ts.microsecond)
