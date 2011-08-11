import json


def process_report_response(report, params):
    """
    Pass in the report instance and the request parameters. This will return
    the appropriate JSON to return for the AJAX response.
    """
    # If request is for headers, return them
    if params.get('metadata'):
        return json.dumps({
            'header': report.report_header(),
            'default_sort': report.default_sort,
        })

    # Otherwise, request is for rows
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
        'iTotalRecords': report.report_row_count(),
        'iTotalDisplayRecords': report.report_row_count(),
        'sEcho': str(echo),
        'aaData': report.report_rows(sort=sort, limit=limit, offset=offset),
    })
