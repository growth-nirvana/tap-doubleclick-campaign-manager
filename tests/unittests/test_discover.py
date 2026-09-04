import unittest

from tap_doubleclick_campaign_manager.discover import discover_streams, list_all_reports


def _make_report(report_id, name):
    return {
        'id': report_id,
        'name': name,
        'type': 'STANDARD',
        'criteria': {
            'dimensions': ['cookieReachTotalReach'],
            'metricNames': ['clickCount'],
        },
    }


class FakeListRequest:
    def __init__(self, service, kwargs):
        self.service = service
        self.kwargs = kwargs

    def execute(self):
        return self.service._execute_list(self.kwargs)


class FakeReportsResource:
    def __init__(self, pages):
        self.pages = pages
        self.list_calls = []

    def list(self, **kwargs):
        self.list_calls.append(kwargs)
        return FakeListRequest(self, kwargs)

    def _execute_list(self, kwargs):
        page_token = kwargs.get('pageToken')
        if page_token is None:
            page_index = 0
        else:
            page_index = int(page_token)

        page = self.pages[page_index]
        response = {'items': page.get('items', [])}
        if 'nextPageToken' in page:
            response['nextPageToken'] = page['nextPageToken']
        return response


class FakeService:
    def __init__(self, pages):
        self._reports = FakeReportsResource(pages)

    def reports(self):
        return self._reports


class TestListAllReports(unittest.TestCase):

    def test_list_all_reports_paginates_through_all_pages(self):
        pages = [
            {
                'items': [_make_report('1', 'Report One'), _make_report('2', 'Report Two')],
                'nextPageToken': '1',
            },
            {
                'items': [_make_report('3', 'Report Three')],
            },
        ]
        service = FakeService(pages)

        actual = list_all_reports(service, 'profile-123')

        self.assertEqual(
            actual,
            [
                _make_report('1', 'Report One'),
                _make_report('2', 'Report Two'),
                _make_report('3', 'Report Three'),
            ],
        )
        self.assertEqual(len(service.reports().list_calls), 2)
        self.assertEqual(
            service.reports().list_calls[0],
            {'profileId': 'profile-123', 'maxResults': 10},
        )
        self.assertEqual(
            service.reports().list_calls[1],
            {'profileId': 'profile-123', 'maxResults': 10, 'pageToken': '1'},
        )

    def test_list_all_reports_returns_empty_list_when_no_reports(self):
        service = FakeService([{'items': []}])

        actual = list_all_reports(service, 'profile-123')

        self.assertEqual(actual, [])
        self.assertEqual(len(service.reports().list_calls), 1)

    def test_list_all_reports_handles_missing_items_key(self):
        service = FakeService([{}])

        actual = list_all_reports(service, 'profile-123')

        self.assertEqual(actual, [])


class TestDiscoverStreams(unittest.TestCase):

    def test_discover_streams_includes_reports_from_all_pages(self):
        pages = [
            {
                'items': [_make_report('100', 'First Report')],
                'nextPageToken': '1',
            },
            {
                'items': [_make_report('200', 'Second Report')],
            },
        ]
        service = FakeService(pages)
        config = {'profile_id': 'profile-123'}

        catalog = discover_streams(service, config)

        stream_ids = {stream['tap_stream_id'] for stream in catalog['streams']}
        self.assertEqual(
            stream_ids,
            {'first_report_100', 'second_report_200'},
        )

    def test_discover_streams_skips_reports_with_invalid_schema(self):
        valid_report = _make_report('100', 'Valid Report')
        invalid_report = {
            'id': '200',
            'name': 'Invalid Report',
            'type': 'STANDARD',
            'criteria': {
                'dimensions': ['cookieReachTotalReach'],
            },
        }
        service = FakeService([{'items': [valid_report, invalid_report]}])
        config = {'profile_id': 'profile-123'}

        catalog = discover_streams(service, config)

        stream_ids = {stream['tap_stream_id'] for stream in catalog['streams']}
        self.assertEqual(stream_ids, {'valid_report_100'})


if __name__ == '__main__':
    unittest.main()
