"""
Blingalytics: Reporting infrastructure.

This module provides a few basic report categorization helper functions. The
main Blingalytics docs (and a decent intro and map to the rest of the docs)
can be found in the base module.
"""

from collections import defaultdict

from blingalytics.base import ReportMeta


def get_report_by_code_name(code_name):
    """
    Returns the report class with the given name, or None if none found.
    
    Note that the reports you've defined must be imported in order for the
    metaclass to catalog them. You can do this when your code starts up or
    just before you call this function; just make sure you do it.
    """
    if code_name is None:
        return None
    for report in ReportMeta.report_catalog:
        if getattr(report, 'code_name', None) == code_name:
            return report
    return None

def get_reports_by_category():
    """
    Returns a dict of categories to lists of reports.
    
    Note that the reports you've defined must be imported in order for the
    metaclass to catalog them. You can do this when your code starts up or
    just before you call this function; just make sure you do it.
    """
    categories = defaultdict(list)
    for report in ReportMeta.report_catalog:
        if hasattr(report, 'category'):
            categories[report.category].append(report)
    return categories
