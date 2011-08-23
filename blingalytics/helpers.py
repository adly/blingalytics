import json

from blingalytics import get_report_by_code_name
from blingalytics.caches import local_cache


DEFAULT_CACHE = local_cache.LocalCache()

def report_response(params, runner=None, cache=DEFAULT_CACHE):
    # Find and instantitate the report class
    params = dict((k, v) for k, v in params.items())
    report_code_name = params.pop('report', None)
    if not report_code_name:
        return json.dumps({'errors': ['Report code name not specified.']})
    report_cls = get_report_by_code_name(report_code_name)
    if not report_cls:
        return json.dumps({'errors': ['Specified report not found.']})
    report = report_cls(cache)

    # Return immediately for metadata request
    if params.pop('metadata', False):
        return json.dumps({
            'errors': [],
            'widgets': report.render_widgets(),
            'header': report.report_header(),
            'default_sort': report.default_sort,
        })

    # Process user inputs
    errors = report.clean_user_inputs(**params)
    if errors:
        return json.dumps({
            'errors': [str(error) for error in errors],
        })

    # Run the report, either synchronously or not, if needed
    if not report.is_report_finished():
        if runner:
            if not report.is_report_started():
                runner(report_code_name, params)
            return json.dumps({
                'errors': [],
                'poll': True,
            })
        else:
            report.run_report()

    # Return report data
    offset = int(params.get('iDisplayStart'))
    limit = int(params.get('iDisplayLength'))
    sort_col = params.get('iSortCol_0')
    if sort_col:
        sort_col = report.columns[int(sort_col) - 1][0]
    else:
        sort_col = report.default_sort[0]
    sort_dir = str(params.get('sSortDir_0', report.default_sort[1]))
    sort = (sort_col, sort_dir)
    echo = int(params.get('sEcho'))
    return json.dumps({
        'errors': [],
        'poll': False,
        'iTotalRecords': report.report_row_count(),
        'iTotalDisplayRecords': report.report_row_count(),
        'sEcho': str(echo),
        'aaData': report.report_rows(sort=sort, limit=limit, offset=offset),
        'footer': report.report_footer(),
    })
