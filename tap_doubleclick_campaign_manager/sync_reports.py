import io
import re
import csv
import time
import random
from datetime import datetime, timedelta

import singer
from googleapiclient import http

from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    REPORT_ID_FIELD,
    PROFILE_ID_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2  # 10 seconds
MAX_RETRY_INTERVAL = 300  # 5 minutes
MAX_RETRY_ELAPSED_TIME = 18000  # 5 hours
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB
FLOODLIGHT_MAX_DAYS = 60  # Maximum days for floodlight reports
IN_FLIGHT_FILE_STATUSES = ('QUEUED', 'PROCESSING')
IN_FLIGHT_FILE_LIST_MAX_PAGES = 5

class StreamFunc(object):
    def __init__(self, func):
        self.func = func
        self.last_bytes = None

    def write(self, _bytes):
        with io.BytesIO() as stream:
            if self.last_bytes:
                stream.write(self.last_bytes)

            stream.write(_bytes)
            stream.seek(0)

            lines = stream.readlines()

            if lines and lines[-1][-1:] != b'\n':
                self.last_bytes = lines.pop()
            else:
                self.last_bytes = None

        if lines:
            for line in lines:
                self.func(line.decode('utf-8')[:-1])

def next_sleep_interval(previous_sleep_interval):
    min_interval = previous_sleep_interval or MIN_RETRY_INTERVAL
    max_interval = previous_sleep_interval * 2 or MIN_RETRY_INTERVAL
    return min(MAX_RETRY_INTERVAL, random.randint(min_interval, max_interval))

def is_transient_api_error(e):
    """
    Return True for API errors that are usually safe to retry with backoff.

    Covers quota/rate limits and transient Google backend outages (e.g. HTTP 503
    backendError seen while polling report file status).
    """
    message = str(e).lower()
    transient_tokens = (
        'quota',
        'ratelimitexceeded',
        'userratelimitexceeded',
        'backenderror',
        'backend error',
        'the service is currently unavailable',
        'internalerror',
        'internal error',
        'unavailable',
        'temporarily unavailable',
        'deadline exceeded',
        'connection reset',
        'connection aborted',
        'broken pipe',
        'timed out',
        'timeout',
    )
    if any(token in message for token in transient_tokens):
        return True

    # googleapiclient.errors.HttpError exposes resp.status
    resp = getattr(e, 'resp', None)
    status = getattr(resp, 'status', None)
    if status in (408, 429, 500, 502, 503, 504):
        return True
    return False

def handle_transient_api_error(e, sleep_interval, stream_name, report_id, report_file_id):
    """
    Sleep and retry on transient API errors.

    Returns (handled, next_sleep_interval). When handled is False, callers should
    re-raise. Sleep interval is always advanced so the first retry never sleeps 0s.
    """
    if not is_transient_api_error(e):
        return False, sleep_interval

    sleep_for = sleep_interval or next_sleep_interval(0)
    LOGGER.warning(
        '%s: Transient API error for report_id %s / file_id %s (%s). Sleeping for %s seconds',
        stream_name, report_id, report_file_id, e, sleep_for
    )
    time.sleep(sleep_for)
    return True, next_sleep_interval(sleep_for)

# Backwards-compatible alias
def handle_rate_limit_error(e, sleep_interval, stream_name, report_id, report_file_id):
    handled, _ = handle_transient_api_error(
        e, sleep_interval, stream_name, report_id, report_file_id
    )
    return handled

def parse_line(line):
    if not line or line.isspace():
        return None
    try:
        with io.StringIO(line) as stream:
            reader = csv.reader(stream)
            return next(reader)
    except StopIteration:
        return None
    except Exception as e:
        LOGGER.warning(f"Failed to parse line: {line[:100]}... Error: {str(e)}")
        return None

def transform_field(dfa_type, value):
    if value == '':
        return None
    if dfa_type == 'double':
        return float(value)
    if dfa_type == 'long':
        try:
            return int(value)
        except:
            return None
    if dfa_type == 'boolean':
        value = value.lower().strip()
        return (
            value == 'true' or
            value == 't' or
            value == 'yes' or
            value == 'y' or
            value == '1'
        )

    if isinstance(dfa_type, list):
        for t in dfa_type:
            try:
                return transform_field(t, value)
            except Exception:
                continue
        return value

    try:
        if dfa_type == 'double':
            return float(value)
        elif dfa_type == 'long':
            return int(value)
        elif dfa_type == 'boolean':
            value = value.lower().strip()
            return value in ('true', 't', 'yes', 'y')
        elif dfa_type == 'string':
            return value
    except Exception:
        return value

    return value


def normalize_types(obj, fieldmap):
    for field in fieldmap:
        name = field["name"]
        value = obj.get(name)

        if value is None:
            continue

        types = field.get("type")
        if not types:
            continue
        if isinstance(types, str):
            types = [types]

        if "string" in types and all(t in ["string", "null"] for t in types):
            obj[name] = str(value)
        elif "number" in types and not isinstance(value, float):
            try:
                obj[name] = float(value)
            except Exception:
                pass  # Let it fail if it's not castable
        elif "integer" in types and not isinstance(value, int):
            try:
                obj[name] = int(float(value))
            except Exception:
                pass  # Let it fail gracefully

    return obj

def process_file(service, fieldmap, report_config, file_id, report_time, processed_records=None):
    """Process a report file."""
    report_id = report_config['report_id']
    stream_name = report_config['stream_name']
    stream_alias = report_config['stream_alias']
    profile_id = report_config['profile_id']
    
    request = service.files().get_media(reportId=report_id, fileId=file_id)
    
    line_state = {
        'headers_line': False,
        'past_headers': False,
        'count': 0,
        'errors': 0,
        'batch': []
    }
    
    report_id_int = int(report_id)
    BATCH_SIZE = 5000
    
    def process_batch(batch):
        """Process a batch of records."""
        if not batch:
            return
        try:
            for record in batch:
                singer.write_record(stream_name, record, stream_alias=stream_alias)
            line_state['count'] += len(batch)
        except Exception as e:
            LOGGER.error(f"Error writing batch of {len(batch)} records: {str(e)}")
            for record in batch:
                try:
                    singer.write_record(stream_name, record, stream_alias=stream_alias)
                    line_state['count'] += 1
                except Exception as sub_e:
                    LOGGER.warning(f"Failed to write record: {str(sub_e)}")
                    line_state['errors'] += 1
        line_state['batch'] = []
    
    def line_transform(line):
        """Transform and process each line of the report."""
        if not line_state['past_headers'] and not line_state['headers_line'] and line == 'Report Fields':
            line_state['headers_line'] = True
            return
        if line_state['headers_line']:
            line_state['headers_line'] = False
            line_state['past_headers'] = True
            return
        
        if line_state['past_headers']:
            row = parse_line(line)
            if not row or row[0] == 'Grand Total:':
                return
            
            try:
                # Create record object
                obj = {}
                for i in range(len(fieldmap)):
                    field = fieldmap[i]
                    val = transform_field(field['type'], row[i] if i < len(row) else '')
                    obj[field['name']] = val
                
                obj[SINGER_REPORT_FIELD] = report_time
                obj[REPORT_ID_FIELD] = report_id_int
                obj[PROFILE_ID_FIELD] = int(profile_id)
                
                obj = normalize_types(obj, fieldmap)
                
                # Add to batch for processing
                line_state['batch'].append(obj)
                
                if len(line_state['batch']) >= BATCH_SIZE:
                    process_batch(line_state['batch'])
                
            except Exception as e:
                LOGGER.warning(f"Error processing line: {str(e)}")
                line_state['errors'] += 1
    
    try:
        stream = StreamFunc(line_transform)
        downloader = http.MediaIoBaseDownload(stream, request, chunksize=CHUNK_SIZE)
        download_finished = False
        
        download_sleep = 0
        download_retries = 0
        MAX_DOWNLOAD_RETRIES = 20

        while not download_finished:
            try:
                _, download_finished = downloader.next_chunk()
                download_retries = 0
            except Exception as e:
                if is_transient_api_error(e) and download_retries < MAX_DOWNLOAD_RETRIES:
                    download_retries += 1
                    download_sleep = next_sleep_interval(download_sleep)
                    LOGGER.warning(
                        '%s: Transient error downloading report_id %s / file_id %s '
                        '(attempt %s/%s): %s. Sleeping for %s seconds',
                        stream_name, report_id, file_id, download_retries,
                        MAX_DOWNLOAD_RETRIES, e, download_sleep
                    )
                    time.sleep(download_sleep)
                    continue
                LOGGER.error(f"Error downloading chunk: {str(e)}")
                raise
        
        # Process any remaining records
        process_batch(line_state['batch'])
        
    except Exception as e:
        LOGGER.error(f"Error in process_file: {str(e)}")
        raise
    
    finally:
        with singer.metrics.record_counter(stream_name) as counter:
            counter.increment(line_state['count'])
        
        LOGGER.info(f"Processed {line_state['count']} records")
        if line_state['errors'] > 0:
            LOGGER.warning(f"Completed with {line_state['errors']} errors out of {line_state['count']} records")

def parse_config_date(value):
    """Parse an ISO date or datetime string from config to a date."""
    if value is None or value == '':
        return None
    s = str(value).strip()
    if 'T' in s:
        norm = s.replace('Z', '+00:00') if s.endswith('Z') else s
        return datetime.fromisoformat(norm).date()
    return datetime.strptime(s[:10], '%Y-%m-%d').date()


def resolve_sync_date_range(config):
    """
    Optional CM360 report window from config: ``start_date`` + ``end_date`` (inclusive).

    When either key is missing, the tap uses the default rolling last-30-days window.

    Returns:
        (start_date, end_date) if both keys are set and valid
        None for the default rolling window
    """
    start_raw = config.get('start_date')
    end_raw = config.get('end_date')
    if start_raw is None and end_raw is None:
        return None
    if start_raw is None or end_raw is None:
        LOGGER.warning(
            'start_date and end_date must both be set to use a custom report range; '
            'using default last-30-days window instead.'
        )
        return None
    start_d = parse_config_date(start_raw)
    end_d = parse_config_date(end_raw)
    if start_d > end_d:
        LOGGER.warning(
            'start_date (%s) is after end_date (%s); using default last-30-days window instead.',
            start_d, end_d,
        )
        return None
    return (start_d, end_d)


def get_date_chunks(start_date, end_date, chunk_size_days=FLOODLIGHT_MAX_DAYS):
    """Generate date chunks for floodlight reports. start_date and end_date are inclusive."""
    chunks = []
    current_start = start_date
    while current_start <= end_date:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_date)
        chunks.append((current_start, current_end))
        if current_end >= end_date:
            break
        current_start = current_end + timedelta(days=1)
    return chunks

def update_report_date_range(
    service, profile_id, report_id, start_date, end_date, custom_range=False
):
    """Update the report's date range."""
    report = service.reports().get(profileId=profile_id, reportId=report_id).execute()

    date_range = {}
    today = datetime.now().date()

    if report.get("type") == "FLOODLIGHT":
        # For floodlight reports, use the provided date range
        if start_date and end_date:
            date_range = {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d")
            }
        else:
            LOGGER.warning("No valid start/end date found for floodlight report. Falling back to relativeDateRange=LAST_30_DAYS.")
            date_range = {
                "relativeDateRange": "LAST_30_DAYS"
            }
        report["floodlightCriteria"]["dateRange"] = date_range
    else:
        if custom_range and start_date and end_date:
            date_range = {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d")
            }
        else:
            # For standard reports, use one month of data (rolling window)
            if start_date:
                one_month_ago = today - timedelta(days=30)
                adjusted_start_date = max(start_date, one_month_ago)
            else:
                adjusted_start_date = today - timedelta(days=30)

            date_range = {
                "startDate": adjusted_start_date.strftime("%Y-%m-%d"),
                "endDate": today.strftime("%Y-%m-%d")
            }
        report["criteria"]["dateRange"] = date_range

    LOGGER.info(f"Updated date range for report {report_id}: {date_range}")
    return service.reports().update(profileId=profile_id, reportId=report_id, body=report).execute()


def report_date_range(report):
    """Return the date range dict from a report resource."""
    if report.get('type') == 'FLOODLIGHT':
        return (report.get('floodlightCriteria') or {}).get('dateRange')
    return (report.get('criteria') or {}).get('dateRange')


def date_ranges_match(file_date_range, expected_date_range):
    """Return True when a report file's date range matches the expected run window."""
    if not file_date_range or not expected_date_range:
        return False

    file_relative = file_date_range.get('relativeDateRange')
    expected_relative = expected_date_range.get('relativeDateRange')
    if file_relative or expected_relative:
        return file_relative == expected_relative

    return (
        file_date_range.get('startDate') == expected_date_range.get('startDate') and
        file_date_range.get('endDate') == expected_date_range.get('endDate')
    )


def find_in_flight_report_file(service, profile_id, report_id, expected_date_range):
    """
    Find an existing QUEUED or PROCESSING file for a report and date range.

    Returns the oldest matching in-flight file so job retries resume polling
    instead of submitting duplicate report runs.
    """
    matches = []
    page_token = None
    pages = 0

    while pages < IN_FLIGHT_FILE_LIST_MAX_PAGES:
        request_kwargs = {
            'profileId': profile_id,
            'reportId': report_id,
            'maxResults': 10,
            'sortField': 'LAST_MODIFIED_TIME',
            'sortOrder': 'ASCENDING',
        }
        if page_token:
            request_kwargs['pageToken'] = page_token

        response = service.reports().files().list(**request_kwargs).execute()
        pages += 1

        for report_file in response.get('items', []):
            status = report_file.get('status')
            if status not in IN_FLIGHT_FILE_STATUSES:
                continue
            if date_ranges_match(report_file.get('dateRange'), expected_date_range):
                matches.append(report_file)

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    if not matches:
        return None

    return min(matches, key=lambda report_file: int(report_file.get('lastModifiedTime', 0)))


def run_or_reuse_report_file(service, profile_id, report_id, expected_date_range, stream_name):
    """Submit a report run or reuse an existing in-flight file for the same window."""
    existing_file = find_in_flight_report_file(
        service, profile_id, report_id, expected_date_range
    )
    if existing_file is not None:
        LOGGER.info(
            '%s: Reusing in-flight report file %s (status=%s) for report_id %s instead of submitting a new run',
            stream_name,
            existing_file['id'],
            existing_file.get('status'),
            report_id,
        )
        return existing_file, True

    report_file = (
        service
        .reports()
        .run(profileId=profile_id, reportId=report_id)
        .execute()
    )
    LOGGER.info(
        '%s: Submitted new report run for report_id %s (file_id %s, status=%s)',
        stream_name,
        report_id,
        report_file['id'],
        report_file.get('status'),
    )
    return report_file, False


def sync_report(service, field_type_lookup, profile_id, report_config, sync_date_range=None):
    """Sync a report and handle deduplication across chunks."""
    report_name = report_config.get("name")
    report_id = report_config['report_id']
    stream_name = report_config['stream_name']
    stream_alias = report_config['stream_alias']

    LOGGER.info("%s: Starting sync", stream_name)

    report = (
        service
        .reports()
        .get(profileId=profile_id, reportId=report_id)
        .execute()
    )

    fieldmap = get_fields(field_type_lookup, report)
    schema = get_schema(stream_name, fieldmap)
    singer.write_schema(stream_name, schema, [], stream_alias=stream_alias)

    # Initialize processed_records as a dictionary
    processed_records = {}
    LOGGER.debug("Initialized processed_records dictionary for sync_report")

    custom_range = sync_date_range is not None
    today = datetime.now().date()
    one_month_ago = today - timedelta(days=30)

    if sync_date_range:
        range_start, range_end = sync_date_range
        if report.get("type") == "FLOODLIGHT":
            date_chunks = get_date_chunks(range_start, range_end)
        else:
            date_chunks = [(range_start, range_end)]
        LOGGER.info(
            "%s: Custom report window %s to %s (%d chunk(s))",
            stream_name, range_start, range_end, len(date_chunks),
        )
    else:
        date_chunks = [(one_month_ago, today)]
        if report.get("type") == "FLOODLIGHT":
            LOGGER.info("%s: Processing floodlight report for date range %s to %s",
                       stream_name, one_month_ago, today)
        else:
            LOGGER.info("%s: Processing standard report for date range %s to %s",
                       stream_name, one_month_ago, today)

    for chunk_start, chunk_end in date_chunks:
        LOGGER.info("%s: Processing date range %s to %s", stream_name, chunk_start, chunk_end)
        
        # Update report date range for this chunk
        updated_report = update_report_date_range(
            service, profile_id, report_id, chunk_start, chunk_end, custom_range=custom_range
        )
        
        with singer.metrics.job_timer('run_report'):
            report_time = datetime.utcnow().isoformat() + 'Z'
            expected_date_range = report_date_range(updated_report)
            report_file, reused_file = run_or_reuse_report_file(
                service,
                profile_id,
                report_id,
                expected_date_range,
                stream_name,
            )

            report_file_id = report_file['id']

            sleep = 0
            if reused_file and report_file.get('lastModifiedTime'):
                start_time = int(report_file['lastModifiedTime']) / 1000
            else:
                start_time = time.time()
            retry_count = 0
            MAX_RETRIES = 25  # Transient API errors while polling file status
            
            while True:
                try:
                    report_file = (
                        service
                        .files()
                        .get(reportId=report_id, fileId=report_file_id)
                        .execute()
                    )

                    status = report_file['status']

                    if status == 'QUEUED':
                        sleep = next_sleep_interval(sleep)
                        LOGGER.info('%s: report_id %s / file_id %s - File status is %s, sleeping for %s seconds',
                                    stream_name, report_id, report_file_id, status, sleep)
                        time.sleep(sleep)

                    elif status == 'PROCESSING':
                        # Avoid hammering the API while CM360 is still working
                        sleep = next_sleep_interval(sleep)
                        LOGGER.info('%s: report_id %s / file_id %s - File status is %s, sleeping for %s seconds',
                                    stream_name, report_id, report_file_id, status, sleep)
                        time.sleep(sleep)

                    elif status == 'REPORT_AVAILABLE':
                        LOGGER.info('Report file %s had status of %s; beginning file processing.', report_file_id, status)
                        # Pass the processed_records dictionary to process_file
                        process_file(service, fieldmap, report_config, report_file_id, report_time, processed_records)
                        break

                    else:
                        message = ('%s: report_id %s / file_id %s - File status is %s, processing failed'
                                   % (stream_name, report_id, report_file_id, status))
                        LOGGER.error(message)
                        raise Exception(message)

                except Exception as e:
                    handled, sleep = handle_transient_api_error(
                        e, sleep, stream_name, report_id, report_file_id
                    )
                    if handled:
                        retry_count += 1
                        if retry_count >= MAX_RETRIES:
                            message = ('%s: report_id %s / file_id %s - Max retries for transient API errors exceeded'
                                       % (stream_name, report_id, report_file_id))
                            LOGGER.error(message)
                            raise Exception(message)
                        continue
                    raise

                if time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                    message = ('%s: report_id %s / file_id %s - Max retry time exceeded'
                               % (stream_name, report_id, report_file_id))
                    LOGGER.error(message)
                    raise Exception(message)

def sync_reports(service, config, catalog, state):
    profile_id = config.get('profile_id')
    sync_date_range = resolve_sync_date_range(config)
    reports = []
    for stream in catalog.streams:
        mdata = singer.metadata.to_map(stream.metadata)
        root_metadata = mdata[()]
        if root_metadata.get('selected') is True:
            reports.append({
                'report_id': root_metadata['tap-doubleclick-campaign-manager.report-id'],
                'stream_name': stream.tap_stream_id,
                'stream_alias': stream.stream_alias,
                'profile_id': profile_id
            })

    reports = sorted(reports, key=lambda x: x['report_id'])

    if state.get('reports') != reports:
        state['current_report'] = None
        state['reports'] = reports

    field_type_lookup = get_field_type_lookup()

    current_report = state.get('current_report')
    past_current_report = False
    for report_config in reports:
        report_id = report_config['report_id']

        if current_report is not None and not past_current_report and current_report != report_id:
            continue

        past_current_report = True
        state['current_report'] = report_id
        singer.write_state(state)

        sync_report(service, field_type_lookup, profile_id, report_config, sync_date_range)

    state['reports'] = None
    state['current_report'] = None
    singer.write_state(state)
