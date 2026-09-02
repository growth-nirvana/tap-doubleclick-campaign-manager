from datetime import date
from types import SimpleNamespace

from tap_doubleclick_campaign_manager.sync_reports import (
    transform_field,
    parse_config_date,
    resolve_sync_date_range,
    get_date_chunks,
    is_transient_api_error,
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
