class InstanceLockError(Exception):
    """Cannot secure a lock on writing the instance to cache."""

class InstanceExistsError(Exception):
    """Cannot record the instance because it already exists in cache."""

class InstanceIncompleteError(Exception):
    """The instance has not yet finished being created in cache."""

class Cache(object):
    def create_instance(self, report_id, instance_id, rows, footer, expire):
        raise NotImplementedError

    def kill_instance_cache(self, report_id, instance_id):
        raise NotImplementedError

    def kill_report_cache(self, report_id):
        raise NotImplementedError

    def is_instance_started(self, report_id, instance_id):
        raise NotImplementedError

    def is_instance_finished(self, report_id, instance_id):
        raise NotImplementedError

    def instance_row_count(self, report_id, instance_id):
        raise NotImplementedError

    def instance_timestamp(self, report_id, instance_id):
        raise NotImplementedError

    def instance_rows(self, report_id, instance_id, selected=None, sort=None, limit=None, offset=None):
        raise NotImplementedError

    def instance_footer(self, report_id, instance_id):
        raise NotImplementedError
