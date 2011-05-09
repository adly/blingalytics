from datetime import datetime, timedelta
import itertools
import sqlite3

from blingalytics import caches
from blingalytics.utils.serialize import encode, decode


def connection(func):
    def inner(self, *args, **kwargs):
        result = None
        self_conn_original = getattr(self, 'conn', None)
        self.conn = sqlite3.connect(self.database,
            detect_types=sqlite3.PARSE_DECLTYPES)
        try:
            result = func(self, *args, **kwargs)
        # except sqlite3.OperationalError:
        #     raise caches.InstanceLockError('Database is locked.')
        finally:
            try:
                self.conn.execute('commit')
                self.conn.close()
                self.conn = self_conn_original
            except sqlite3.OperationalError:
                pass
        return result
    return inner

class LocalCache(caches.Cache):
    """A local filesystem cache using SQLite."""
    METADATA_TABLE = 'metadata'

    def __init__(self, database=None):
        """Specify the database file, or will use default."""
        if not database:
            database = '/tmp/blingalytics_cache'
        self.database = database
        self._create_metadata_table()

    @connection
    def _create_metadata_table(self):
        self.conn.execute('''
            create table if not exists %s (
                report_id text,
                instance_id text,
                created_ts timestamp,
                expires_ts timestamp,
                footer,
                unique (report_id, instance_id)
            )
        ''' % self.METADATA_TABLE)

    @connection
    def create_instance(self, report_id, instance_id, rows, footer, expire):
        now = datetime.utcnow()
        expire = now + timedelta(seconds=expire)

        # Check if the instance already exists
        metas = self.conn.execute('''
            select expires_ts from %s
            where report_id = ? and instance_id = ?
        ''' % self.METADATA_TABLE, (report_id, instance_id))
        for meta in metas:
            if meta[0] > now:
                raise caches.InstanceExistsError('Instance already cached.')

        # Build the table for this instance (will not exist for zero rows)
        table_name = '%s_%s' % (report_id, instance_id)
        self.conn.execute('drop table if exists %s' % table_name)
        try:
            first_row = rows.next()
        except StopIteration:
            pass
        else:
            columns = ', '.join(sorted(first_row.keys()))
            self.conn.execute('create table %s (%s)' % (table_name, columns))
            for column in first_row.keys():
                self.conn.execute('''
                    create index ix_%s_%s on %s (%s)
                ''' % (table_name, column, table_name, column))

            # Insert the rows into the table
            for row in itertools.chain([first_row], rows):
                columns, values = zip(*row.items())
                columns = ','.join(columns)
                inserts = ','.join(['?' for value in values])
                self.conn.execute('''
                    insert into %s (%s) values (%s)
                ''' % (table_name, columns, inserts), map(encode, values))

        # Create the metadata row for the instance
        self.conn.execute('''
            insert or replace into %s
            (report_id, instance_id, created_ts, expires_ts, footer)
            values (?, ?, ?, ?, ?)
        ''' % self.METADATA_TABLE, (report_id, instance_id, now, expire, encode(footer() or {})))

    @connection
    def kill_instance_cache(self, report_id, instance_id):
        # Delete the metadata row, if it exists
        self.conn.execute('''
            delete from %s
            where report_id = ? and instance_id = ?
        ''' % self.METADATA_TABLE, (report_id, instance_id))

        # Delete the instance table, if it exists
        table_name = '%s_%s' % (report_id, instance_id)
        self.conn.execute('drop table if exists %s' % table_name)

    @connection
    def kill_report_cache(self, report_id):
        # Find all the cached instances of the report
        instances = self.conn.execute('''
            select report_id, instance_id from %s
            where report_id = ?
        ''' % self.METADATA_TABLE, (report_id,))

        # Delete the instance table and metadata table
        for instance in instances:
            table_name = '%s_%s' % instance
            self.conn.execute('drop table if exists %s' % table_name)
            self.conn.execute('''
                delete from %s
                where report_id = ? and instance_id = ?
            ''' % self.METADATA_TABLE, instance)

    def is_instance_started(self, report_id, instance_id):
        raise NotImplementedError('The SQLite cache is not intended for '
            'concurrent connections. Please simply run report.run_report '
            'synchronously and wait for it to finish. Or use Redis.')

    @connection
    def is_instance_finished(self, report_id, instance_id):
        now = datetime.utcnow()
        rows = self.conn.execute('''
            select expires_ts from %s
            where report_id = ? and instance_id = ?
        ''' % self.METADATA_TABLE, (report_id, instance_id))
        for row in rows:
            if row[0] > now:
                return True
        return False

    def instance_row_count(self, report_id, instance_id):
        if not self.is_instance_finished(report_id, instance_id):
            raise caches.InstanceIncompleteError
        table_name = '%s_%s' % (report_id, instance_id)
        try:
            count = self.conn.execute('select count(*) from %s' % table_name)
        except sqlite3.OperationalError:
            # If we have a metadata record but no table, there were no rows
            # to cache
            return 0
        return count.next()[0]

    @connection
    def instance_timestamp(self, report_id, instance_id):
        if not self.is_instance_finished(report_id, instance_id):
            raise caches.InstanceIncompleteError
        timestamp = self.conn.execute('''
            select created_ts from %s
            where report_id = ? and instance_id = ?
        ''' % self.METADATA_TABLE, (report_id, instance_id))
        return timestamp.next()[0]

    @connection
    def instance_rows(self, report_id, instance_id, selected=None, sort=None, limit=None, offset=None, alpha=False):
        if not self.is_instance_finished(report_id, instance_id):
            raise caches.InstanceIncompleteError
        self.conn.row_factory = sqlite3.Row

        # Construct the query for the rows
        table_name = '%s_%s' % (report_id, instance_id)
        query = 'select rowid as _bling_id, * from %s ' % table_name
        if selected:
            selected_ids = ','.join([str(id) for id in selected])
            query += 'where rowid in (%s) '
        cast = 'text' if alpha else 'real'
        query += 'order by cast(%s as %s) %s ' % (sort[0], cast, sort[1])
        if limit:
            query += 'limit %d ' % limit
        if offset:
            query += 'offset %d ' % offset

        # Decode and return the rows
        return itertools.imap(
            lambda row: dict(zip(row.keys(), [row[0]] + map(decode, list(row)[1:]))),
            self.conn.execute(query)
        )

    @connection
    def instance_footer(self, report_id, instance_id):
        if not self.is_instance_finished(report_id, instance_id):
            raise caches.InstanceIncompleteError
        footer = self.conn.execute('''
            select footer from %s
            where report_id = ? and instance_id = ?
        ''' % self.METADATA_TABLE, (report_id, instance_id))
        return decode(footer.next()[0])
