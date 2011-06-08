#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
The ``blingalytics`` module provides a few utility methods that are useful for
retrieving your reports.

.. note::

    The report registration method is done using a metaclass set on the
    :class:`base.Report` class. This means that the modules where you've
    defined your report classes have to be imported before the methods below
    will know they exist. You can import them when your code initializes, or
    right before you call the utility functions, or whatever — just be sure to
    do it.
"""

from collections import defaultdict

from blingalytics.base import ReportMeta


def get_report_by_code_name(code_name):
    """
    Returns the report class with the given ``code_name``, or ``None`` if not
    found.
    """
    if code_name is None:
        return None
    for report in ReportMeta.report_catalog:
        if getattr(report, 'code_name', None) == code_name:
            return report
    return None

def get_reports_by_category():
    """
    Returns all known reports, organized by category. The result is returned
    as a dict of category strings to lists of report classes.
    """
    categories = defaultdict(list)
    for report in ReportMeta.report_catalog:
        if hasattr(report, 'category'):
            categories[report.category].append(report)
    return categories
