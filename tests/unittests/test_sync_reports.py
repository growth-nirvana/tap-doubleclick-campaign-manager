from datetime import date
from types import SimpleNamespace

from tap_doubleclick_campaign_manager.sync_reports import (
    transform_field,
    parse_config_date,
    resolve_sync_date_range,
    get_date_chunks,
    is_transient_api_error,
    date_ranges_match,
    find_in_flight_report_file,
    run_or_reuse_report_file,
)

import unittest

class TestSyncReports(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def test_transform_field_handles_single_type_string(self):
        actual = transform_field("string", "some_field")
        expected = "some_field"
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_double(self):
        actual = transform_field("double", "1.23")
        expected = 1.23
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_long(self):
        actual = transform_field("long", "123")
        expected = 123
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_boolean(self):
        actual = transform_field("boolean", "true")
        expected = True
        self.assertEqual(expected, actual)

    def test_transform_field_handles_single_type_empty(self):
        actual = transform_field("double", "")
        expected = None
        self.assertEqual(expected, actual)


    def test_transform_field_handles_multiple_type_uses_first_type(self):
        actual = transform_field(["double", "string"], "123")
        expected = 123.0
        self.assertEqual(expected, actual)

        actual = transform_field(["long", "string"], "123")
        expected = 123
        self.assertEqual(expected, actual)

        actual = transform_field(["long", "string"], "some_field")
        expected = None
        self.assertEqual(expected, actual)

    def test_transform_field_handles_multiple_type_uses_second_type(self):
        actual = transform_field(["double", "string"], "some_field")
        expected = "some_field"
        self.assertEqual(expected, actual)

    def test_parse_config_date_accepts_date_only(self):
        self.assertEqual(parse_config_date("2026-03-01"), date(2026, 3, 1))

    def test_parse_config_date_accepts_iso_datetime_z(self):
        self.assertEqual(
            parse_config_date("2026-04-12T00:00:00Z"),
            date(2026, 4, 12),
        )

    def test_resolve_sync_date_range_returns_none_when_unset(self):
        self.assertIsNone(resolve_sync_date_range({}))

    def test_resolve_sync_date_range_returns_pair_when_both_set(self):
        r = resolve_sync_date_range(
            {"start_date": "2026-03-01", "end_date": "2026-04-12"}
        )
        self.assertEqual(r, (date(2026, 3, 1), date(2026, 4, 12)))

    def test_get_date_chunks_single_day(self):
        d = date(2026, 3, 1)
        self.assertEqual(get_date_chunks(d, d, chunk_size_days=60), [(d, d)])

    def test_get_date_chunks_inclusive_end(self):
        a, b = date(2026, 3, 1), date(2026, 4, 12)
        chunks = get_date_chunks(a, b, chunk_size_days=60)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], (a, b))

    def test_is_transient_api_error_detects_503_backend_error(self):
        err = Exception(
            '<HttpError 503 when requesting ... returned "The service is currently unavailable.". '
            'Details: "[{{\'message\': \'The service is currently unavailable.\', '
            '\'domain\': \'global\', \'reason\': \'backendError\'}}]">'
        )
        self.assertTrue(is_transient_api_error(err))

    def test_is_transient_api_error_detects_http_status(self):
        err = Exception('request failed')
        err.resp = SimpleNamespace(status=503)
        self.assertTrue(is_transient_api_error(err))

    def test_is_transient_api_error_detects_rate_limit(self):
        self.assertTrue(is_transient_api_error(Exception('rateLimitExceeded')))

    def test_is_transient_api_error_rejects_permanent_errors(self):
        self.assertFalse(is_transient_api_error(Exception('404 Not Found')))
        self.assertFalse(is_transient_api_error(Exception('File status is FAILED')))

    def test_date_ranges_match_compares_absolute_dates(self):
        expected = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}
        matching = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}
        different = {'startDate': '2026-07-06', 'endDate': '2026-09-04'}

        self.assertTrue(date_ranges_match(matching, expected))
        self.assertFalse(date_ranges_match(different, expected))

    def test_date_ranges_match_compares_relative_ranges(self):
        expected = {'relativeDateRange': 'LAST_30_DAYS'}
        matching = {'relativeDateRange': 'LAST_30_DAYS'}
        different = {'relativeDateRange': 'LAST_7_DAYS'}

        self.assertTrue(date_ranges_match(matching, expected))
        self.assertFalse(date_ranges_match(different, expected))


class FakeFilesListRequest:
    def __init__(self, resource, kwargs):
        self.resource = resource
        self.kwargs = kwargs

    def execute(self):
        return self.resource._execute_list(self.kwargs)


class FakeReportFilesResource:
    def __init__(self, pages):
        self.pages = pages
        self.list_calls = []
        self.run_calls = 0

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeFilesListRequest(self, kwargs)

    def _execute_list(self, kwargs):
        page_token = kwargs.get('pageToken')
        page_index = 0 if page_token is None else int(page_token)
        page = self.pages[page_index]
        response = {'items': page.get('items', [])}
        if 'nextPageToken' in page:
            response['nextPageToken'] = page['nextPageToken']
        return response


class FakeReportsResource:
    def __init__(self, files_resource):
        self._files = files_resource
        self.run_calls = 0

    def files(self):
        return self._files

    def run(self, **kwargs):
        self.run_calls += 1
        return FakeRunRequest(self)


class FakeRunRequest:
    def __init__(self, reports_resource):
        self.reports_resource = reports_resource

    def execute(self):
        return {
            'id': '999',
            'status': 'QUEUED',
            'dateRange': {
                'startDate': '2026-08-05',
                'endDate': '2026-09-04',
            },
            'lastModifiedTime': '1000',
        }


class FakeService:
    def __init__(self, pages):
        self._files = FakeReportFilesResource(pages)
        self._reports = FakeReportsResource(self._files)

    def reports(self):
        return self._reports


class TestInFlightReportReuse(unittest.TestCase):

    def test_find_in_flight_report_file_returns_oldest_matching_file(self):
        pages = [{
            'items': [
                {
                    'id': '200',
                    'status': 'QUEUED',
                    'lastModifiedTime': '2000',
                    'dateRange': {'startDate': '2026-08-05', 'endDate': '2026-09-04'},
                },
                {
                    'id': '100',
                    'status': 'QUEUED',
                    'lastModifiedTime': '1000',
                    'dateRange': {'startDate': '2026-08-05', 'endDate': '2026-09-04'},
                },
                {
                    'id': '300',
                    'status': 'REPORT_AVAILABLE',
                    'lastModifiedTime': '3000',
                    'dateRange': {'startDate': '2026-08-05', 'endDate': '2026-09-04'},
                },
            ],
        }]
        service = FakeService(pages)
        expected = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}

        actual = find_in_flight_report_file(service, 'profile-1', 'report-1', expected)

        self.assertEqual(actual['id'], '100')

    def test_find_in_flight_report_file_ignores_different_date_range(self):
        pages = [{
            'items': [{
                'id': '100',
                'status': 'PROCESSING',
                'lastModifiedTime': '1000',
                'dateRange': {'startDate': '2026-07-06', 'endDate': '2026-09-04'},
            }],
        }]
        service = FakeService(pages)
        expected = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}

        actual = find_in_flight_report_file(service, 'profile-1', 'report-1', expected)

        self.assertIsNone(actual)

    def test_run_or_reuse_report_file_reuses_existing_in_flight_file(self):
        pages = [{
            'items': [{
                'id': '100',
                'status': 'QUEUED',
                'lastModifiedTime': '1000',
                'dateRange': {'startDate': '2026-08-05', 'endDate': '2026-09-04'},
            }],
        }]
        service = FakeService(pages)
        expected = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}

        report_file, reused = run_or_reuse_report_file(
            service, 'profile-1', 'report-1', expected, 'stream'
        )

        self.assertTrue(reused)
        self.assertEqual(report_file['id'], '100')
        self.assertEqual(service.reports().run_calls, 0)

    def test_run_or_reuse_report_file_submits_new_run_when_none_in_flight(self):
        service = FakeService([{'items': []}])
        expected = {'startDate': '2026-08-05', 'endDate': '2026-09-04'}

        report_file, reused = run_or_reuse_report_file(
            service, 'profile-1', 'report-1', expected, 'stream'
        )

        self.assertFalse(reused)
        self.assertEqual(report_file['id'], '999')
        self.assertEqual(service.reports().run_calls, 1)
