"""
The Redis-based cache engine provides flexible, powerful storage for computed
reports. It is currently the recommended caching option for anything beyond
a simple dev environment.

.. note::

    The Redis cache requires Redis and its Python bindings to be installed.
    See :doc:`/install` for details.

"""

from builtins import str
from datetime import datetime
from decimal import Decimal
import itertools
import hashlib

import redis

from blingalytics import caches
from blingalytics.utils.serialize import encode, encode_dict, decode, \
    decode_dict


class RedisCache(caches.Cache):
    """
    Caches computed reports in Redis. This takes the same init options as the
    redis-py_ client. The most commonly used are:

    .. _redis-py: https://github.com/andymccurdy/redis-py

    * ``host``: The host to connect to the redis server on. Defaults to
      ``'localhost'``.
    * ``port``: The port to use when connecting. Defaults to ``6379``.
    * ``db``: Which Redis database to connect to, as an integer. Defaults to
      ``0``.
    """
    def __init__(self, **kwargs):
        """
        Accepts the same arguments as redis-py client.
        
        Defaults to localhost:6379 and database 0.
        """
        self.conn = redis.Redis(**kwargs)

    def create_instance(self, report_id, instance_id, rows, footer, expire):
        keys = set()
        table_name = '%s:%s' % (report_id, instance_id)

        # Really simple lock on writing to the table
        # NOTE: Cannot use expire on the table lock because until Redis 2.1.3
        # any write operation (even setnx on an existing key) causes the key
        # to be deleted so setnx returns true, even on existing keys, if they
        # have an expire set.
        if not self.conn.setnx('%s:_lock:' % table_name, 'lock'):
            raise caches.InstanceLockError('Instance already locked')

        # Check if table already created, or set the timestamp
        if self.conn.exists('%s:' % table_name):
            # Release lock and raise an error
            self.conn.delete('%s:_lock:' % table_name)
            raise caches.InstanceExistsError('Instance already cached')
        self.conn['%s:' % table_name] = encode(datetime.utcnow())
        keys.add('%s:' % table_name)

        # Pipeline the insert operations for speed
        try:
            p = self.conn.pipeline(False)

            for row_id, row in enumerate(rows):
                p.hmset('%s:%s' % (table_name, row_id), encode_dict(row))
                keys.add('%s:%s' % (table_name, row_id))
                p.sadd('%s:ids:' % table_name, row_id)
                keys.add('%s:ids:' % table_name)

                # Index the row
                key = '%s:index:%s:' % (table_name, row_id)
                data = {}
                for name, value in row.items():
                    t = type(value)
                    if t is str:
                        data[name] = value.encode('utf-8')
                    elif t is Decimal:
                        data[name] = float(value)
                    elif t in (int, float, int, str):
                        data[name] = value
                    else:
                        data[name] = str(value)
                p.hmset(key, data)
                keys.add(key)

            # Table footer
            if footer:
                footer_row = encode_dict(footer() or {})
                p.hmset('%s:footer:' % table_name, footer_row)
                keys.add('%s:footer:' % table_name)

            # mark that the table is done.
            p.set('%s:_done:' % table_name, 'done')
            keys.add('%s:_done:' % table_name)

            # Table expiration.
            if expire:
                for key in keys:
                    p.expire(key, expire)

            # Release the table lock
            p.delete('%s:_lock:' % table_name)
            p.execute()
        except:
            try:
                # failed to actually run the report, so cleanup initial cache data.
                self.conn.delete('%s:_lock:' % table_name)
                self.conn.delete('%s:' % table_name)
            except:
                # don't set a redis error mask the original exception
                pass
            raise

    def kill_instance_cache(self, report_id, instance_id):
        # Get a simple lock (see create_instantce method for details)
        table_name = '%s:%s' % (report_id, instance_id)
        if not self.conn.setnx('%s:_lock:' % table_name, 'lock'):
            raise caches.InstanceLockError('Instance already locked')

        p = self.conn.pipeline(False)
        for key in self.conn.keys('%s:*' % table_name):
            p.delete(key)

        # Release the table lock
        p.delete('%s:_lock:' % table_name)
        p.execute()

    def kill_report_cache(self, report_id):
        p = self.conn.pipeline(False)
        for key in self.conn.keys('%s:*' % report_id):
            p.delete(key)
        p.execute()

    def is_instance_started(self, report_id, instance_id):
        table_name = '%s:%s' % (report_id, instance_id)
        return self.conn.exists('%s:' % table_name)

    def is_instance_finished(self, report_id, instance_id):
        table_name = '%s:%s' % (report_id, instance_id)
        return self.conn.exists('%s:_done:' % table_name)

    def instance_row_count(self, report_id, instance_id):
        table_name = '%s:%s' % (report_id, instance_id)
        if not self.conn.exists('%s:_done:' % table_name):
            raise caches.InstanceIncompleteError
        rows = self.conn.scard('%s:ids:' % table_name)
        return int(rows)

    def instance_timestamp(self, report_id, instance_id):
        table_name = '%s:%s' % (report_id, instance_id)
        timestamp = self.conn['%s:' % table_name]
        if not timestamp:
            raise caches.InstanceIncompleteError
        return decode(timestamp)

    def instance_rows(self, report_id, instance_id, selected=None, sort=None, limit=None, offset=None, alpha=False):
        table_name = '%s:%s' % (report_id, instance_id)
        if not self.conn.exists('%s:_done:' % table_name):
            raise caches.InstanceIncompleteError

        ids_key = '%s:ids:' % table_name
        temp_key = None

        # If querying only on selected rows, create an intermediate selected set
        if selected:
            selected = set(selected)

            # Create a unique key and store the selected row set
            temp_key = '%s:ids_%s:' % (
                table_name,
                hashlib.sha1(str(selected)).hexdigest()[::4]
            )
            ids_key = temp_key
            p = self.conn.pipeline(False)
            for row_id in selected:
                p.sadd(temp_key, int(row_id))
            p.execute()

        # Parse the sorting criteria
        by = '%s:index:*:->%s' % (table_name, sort[0]) if sort else None
        desc = (sort[1] == 'desc')
        limit = -1 if limit is None else limit

        # Get a list of row ids, sorted by the criteria
        # TODO: Either store alpha t/f per row in redis, or encode numeric values as sortable strings
        ids = self.conn.sort(ids_key, by=by, desc=desc, start=offset, num=limit, alpha=alpha)
        if temp_key:
            self.conn.delete(temp_key)

        # Pipeline getting all the requested rows by id
        p = self.conn.pipeline(False)
        for id in ids:
            p.hgetall('%s:%s' % (table_name, id))

        # Add the row ids to the rows and return them
        rows = map(decode_dict, p.execute())
        return map(
            (lambda id_row: id_row[1].__setitem__('_bling_id', id_row[0]) or id_row[1]),
            zip(ids, rows)
        )

    def instance_footer(self, report_id, instance_id):
        table_name = '%s:%s' % (report_id, instance_id)
        if not self.conn.exists('%s:_done:' % table_name):
            raise caches.InstanceIncompleteError
        return decode_dict(self.conn.hgetall('%s:footer:' % table_name))
