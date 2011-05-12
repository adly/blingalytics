"""
The key range source provides many of the standard key ranges you'll use
frequently. It also provides a column for outputting the values returned by
any key range.
"""

from datetime import datetime, timedelta

from blingalytics import sources
from blingalytics.utils import epoch


class KeysSource(sources.Source):
    def get_rows(self, key_rows, clean_inputs):
        for key, key_column in key_rows:
            row = {}
            for name in self._columns_dict.keys():
                row[name] = key_column[name]
            yield (key, row)

class KeysColumn(sources.Column):
    source = KeysSource

class Value(KeysColumn):
    """
    Occasionally you may want to simply have a column show the values returned
    by a given key range, such as with an :class:`IterableKeyRange`. This
    column requires no special options, and returns the key value from the key
    whose name matches the name of this column.
    """
    pass

class MonthKeyRange(sources.KeyRange):
    """
    Ensures a key for every month between the start and end dates.
    
    This key range takes two positional arguments, start and end, which are
    used to determine the range of months. These arguments can be datetimes,
    which will be used as-is; or they can be strings, which will be considered
    as references to named widgets, and the user input from the widget will be
    used for the date.
    
    The values of the keys returned by this key range are in the form of an
    integer representing the number of full days since the UNIX epoch
    (Jan. 1, 1970). This is ideal for use with the
    :class:`Epoch <blingalytics.formats.Epoch>` formatter.
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def _resolve_date(self, date, clean_inputs):
        if isinstance(date, basestring):
            # Resolve from the widget with the same name
            return clean_inputs[date]
        if isinstance(date, datetime):
            return date.date()
        return date

    def get_row_keys(self, clean_inputs):
        date = self._resolve_date(self.start, clean_inputs)
        date = date.replace(day=1)
        end = self._resolve_date(self.end, clean_inputs)
        while date <= end:
            yield date
            date = (date + timedelta(days=31)).replace(day=1)

class EpochKeyRange(sources.KeyRange):
    """
    Ensures a key for every day between the start and end dates.
    
    This key range takes two positional arguments, start and end, which are
    used to determine the range of days. These arguments can be datetimes,
    which will be used as-is; or they can be strings, which will be considered
    as references to named widgets, and the user input from the widget will be
    used for the date.
    
    The values of the keys returned by this key range are in the form of an
    integer representing the number of full days since the UNIX epoch
    (Jan. 1, 1970). This is ideal for use with the
    :class:`Epoch <blingalytics.formats.Epoch>` formatter.
    """
    def __init__(self, start, end):
        self.start = start
        self.end = end

    def _resolve_date(self, date, clean_inputs):
        if isinstance(date, basestring):
            # Resolve from the widget with the same name
            try:
                return clean_inputs[date]
            except KeyError:
                raise ValueError('Start or end date name not found in widgets.')
        if isinstance(date, datetime):
            return date.date()
        return date

    def get_row_keys(self, clean_inputs):
        date = self._resolve_date(self.start, clean_inputs)
        end = self._resolve_date(self.end, clean_inputs)
        if date > end:
            raise ValueError('Start date must be earlier than end date.')
        while date <= end:
            yield epoch.datetime_to_hours(date) / 24
            date += timedelta(days=1)

class IterableKeyRange(sources.KeyRange):
    """
    Ensures every value returned by the iterable is in the key range. It takes
    one argument, which is the iterable to use.
    
    Note that this iterable must be returned in sorted order. By default, this
    key range will sort the iterable for you before it is returned. However,
    if your iterable is already in sorted order and you want to avoid the
    overhead of resorting the list, can pass in ``sort_results=False``.
    """
    def __init__(self, iterable, sort_results=True):
        self.iterable = iterable
        self.sort_results = sort_results

    def get_row_keys(self, clean_inputs):
        if self.sort_results:
            return sorted(self.iterable)
        else:
            return self.iterable

class SourceKeyRange(sources.KeyRange):
    """
    This key range doesn't actually ensure any keys. It simply allows each key
    value returned by the sources to be a new row. If it isn't returned by a
    source column, it won't be a row.
    """
    def get_row_keys(self, clean_inputs):
        return []
