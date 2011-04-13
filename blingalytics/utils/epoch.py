from datetime import date, datetime, timedelta
import time

from blingalytics.utils import timezones


# Epoch is Jan 1, 1970
EPOCH = datetime(*time.gmtime(0)[:6])

def datetime_to_hours(dt):
    if type(dt) is date:
        dt = datetime(dt.year, dt.month, dt.day)
    if dt.tzinfo:
        dt = timezones.unlocalize(dt)
    delta = dt - EPOCH
    hours = (delta.days * 24) + (delta.seconds / 3600)
    return hours

def hours_to_datetime(hours):
    return EPOCH + timedelta(hours=hours)

# def datetime_to_months(dt):
#     if type(dt) is date:
#         dt = datetime(dt.year, dt.month, dt.day)
#     if dt.tzinfo:
#         dt = timezones.unlocalize(dt)
#     delta = relativedelta(dt, EPOCH)
#     return delta.years * 12 + delta.months
# 
# def months_to_datetime(months):
#     return EPOCH + relativedelta(months=months)
