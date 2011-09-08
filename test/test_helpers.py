import json
import unittest

from blingalytics import base, caches, helpers
from mock import Mock

from test.reports import BasicDatabaseReport, SuperBasicReport


class TestFrontendHelpers(unittest.TestCase):
    def setUp(self):
        report = BasicDatabaseReport(helpers.DEFAULT_CACHE)
        report.kill_cache(full=True)
        report = SuperBasicReport(helpers.DEFAULT_CACHE)
        report.kill_cache(full=True)
        self.mock_runner = Mock()
        self.mock_cache = Mock(spec=caches.Cache)
        self.mock_cache.instance_rows.return_value = []
        self.mock_cache.instance_footer.return_value = {'id': None}
        self.mock_cache.instance_row_count.return_value = 0

    def test_report_response_basic(self):
        # Test report codename errors
        self.assertEqual(helpers.report_response({}),
            '{"errors": ["Report code name not specified."]}')
        self.assertEqual(helpers.report_response({'report': 'nonexistent'}),
            '{"errors": ["Specified report not found."]}')

        # Test metadata request
        metadata = json.loads(helpers.report_response({
            'report': 'super_basic_report',
            'metadata': '1',
        }))
        self.assertEqual(set(metadata.keys()),
            set(['errors', 'widgets', 'header', 'default_sort']))
        self.assertEqual(metadata['errors'], [])

        # Test user input errors
        response = json.loads(helpers.report_response({
            'report': 'basic_database_report',
        }))
        self.assertEqual(len(response['errors']), 1)

        # Test correct request
        response = json.loads(helpers.report_response({
            'report': 'super_basic_report',
            'iDisplayStart': '0',
            'iDisplayLength': '10',
            'sEcho': '1',
        }))
        self.assertEqual(len(response['errors']), 0)
        self.assertEqual(response['iTotalRecords'], 3)
        self.assertEqual(response['iTotalDisplayRecords'], 3)
        self.assertEqual(response['sEcho'], '1')
        self.assertEqual(response['poll'], False)
        self.assertEqual(len(response['aaData']), 3)
        self.assertEqual(len(response['aaData'][0]), 2)
        self.assertEqual(len(response['footer']), 2)

    def test_report_response_runner(self):
        # Runner gets run
        self.mock_cache.is_instance_started.return_value = False
        self.mock_cache.is_instance_finished.return_value = False
        response = json.loads(helpers.report_response({
            'report': 'super_basic_report',
            'iDisplayStart': '0',
            'iDisplayLength': '10',
            'sEcho': '1',
        }, runner=self.mock_runner, cache=self.mock_cache))
        self.mock_runner.assert_called_once_with('super_basic_report',
            {'sEcho': '1', 'iDisplayStart': '0', 'iDisplayLength': '10'})
        self.assertEqual(response['errors'], [])
        self.assertEqual(response['poll'], True)

        # Runner already running does not get run
        self.mock_cache.reset_mock()
        self.mock_cache.is_instance_started.return_value = True
        self.mock_cache.is_instance_finished.return_value = False
        response = json.loads(helpers.report_response({
            'report': 'super_basic_report',
            'iDisplayStart': '0',
            'iDisplayLength': '10',
            'sEcho': '1',
        }, runner=self.mock_runner, cache=self.mock_cache))
        self.assertFalse(self.mock_cache.called)
        self.assertEqual(response['errors'], [])
        self.assertEqual(response['poll'], True)

    def test_report_response_runner_local_cache(self):
        # Cannot use local cache with async runner
        self.assertRaises(NotImplementedError, helpers.report_response, {
            'report': 'super_basic_report',
            'iDisplayStart': '0',
            'iDisplayLength': '10',
            'sEcho': '1',
        }, runner=self.mock_runner)

    def test_report_response_cache(self):
        # Validate that custom cache is used correctly
        self.mock_cache.is_instance_started.return_value = False
        self.mock_cache.is_instance_finished.return_value = False
        response = json.loads(helpers.report_response({
            'report': 'super_basic_report',
            'iDisplayStart': '0',
            'iDisplayLength': '10',
            'sEcho': '1',
        }, cache=self.mock_cache))
        self.assertTrue(self.mock_cache.instance_rows.called)
        self.assertTrue(self.mock_cache.instance_footer.called)
        self.assertEqual(response['errors'], [])
        self.assertEqual(response['poll'], False)
        self.assertEqual(response['aaData'], [])
