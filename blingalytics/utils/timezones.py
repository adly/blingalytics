from datetime import timedelta, tzinfo


ZERO = timedelta(0)
HOUR = timedelta(hours=1)

class UTCTimeZone(tzinfo):
    """Implementation of the UTC timezone."""
    def utcoffset(self, dt):
        return ZERO
    
    def tzname(self, dt):
        return 'UTC'
    
    def dst(self, dt):
        return ZERO

utc_tzinfo = UTCTimeZone()

def unlocalize(aware_dt):
    """Converts a timezone-aware datetime into a naive datetime in UTC."""
    return aware_dt.astimezone(utc_tzinfo).replace(tzinfo=None)
