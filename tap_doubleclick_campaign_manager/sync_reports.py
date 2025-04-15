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
    get_fields,
    get_schema,
    get_field_type_lookup
)

LOGGER = singer.get_logger()

MIN_RETRY_INTERVAL = 2  # 10 seconds
MAX_RETRY_INTERVAL = 300  # 5 minutes
MAX_RETRY_ELAPSED_TIME = 3600  # 1 hour
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB
FLOODLIGHT_MAX_DAYS = 60  # Maximum days for floodlight reports

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


def process_file(service, fieldmap, report_config, file_id, report_time):
    report_id = report_config['report_id']
    stream_name = report_config['stream_name']
    stream_alias = report_config['stream_alias']
    
    request = service.files().get_media(reportId=report_id, fileId=file_id)
    
    line_state = {
        'headers_line': False,
        'past_headers': False,
        'count': 0,
        'errors': 0,
        'batch': []
    }
    
    report_id_int = int(report_id)
    BATCH_SIZE = 5000  # Keep the larger batch size for performance
    
    def process_batch(batch):
        if not batch:
            return
        try:
            # Process records individually but in a batch loop for better performance
            for record in batch:
                singer.write_record(stream_name, record, stream_alias=stream_alias)
            line_state['count'] += len(batch)
        except Exception as e:
            LOGGER.error(f"Error writing batch of {len(batch)} records: {str(e)}")
            # If batch fails, try individual records
            for record in batch:
                try:
                    singer.write_record(stream_name, record, stream_alias=stream_alias)
                    line_state['count'] += 1
                except Exception as sub_e:
                    LOGGER.warning(f"Failed to write record: {str(sub_e)}")
                    line_state['errors'] += 1
        line_state['batch'] = []
    
    def line_transform(line):
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
                obj = {}
                for i in range(len(fieldmap)):
                    field = fieldmap[i]
                    val = transform_field(field['type'], row[i] if i < len(row) else '')
                    obj[field['name']] = val
                
                obj[SINGER_REPORT_FIELD] = report_time
                obj[REPORT_ID_FIELD] = report_id_int
                
                obj = normalize_types(obj, fieldmap)
                line_state['batch'].append(obj)
                line_state['count'] += 1
                
                if len(line_state['batch']) >= BATCH_SIZE:
                    process_batch(line_state['batch'])
                
            except Exception as e:
                LOGGER.warning(f"Error processing line: {str(e)}")
                line_state['errors'] += 1
    
    try:
        stream = StreamFunc(line_transform)
        downloader = http.MediaIoBaseDownload(stream, request, chunksize=CHUNK_SIZE)
        download_finished = False
        
        while not download_finished:
            try:
                _, download_finished = downloader.next_chunk()
            except Exception as e:
                LOGGER.error(f"Error downloading chunk: {str(e)}")
                if 'quota' in str(e).lower():
                    time.sleep(60)  # Wait a minute if we hit quota limits
                continue
        
        # Process any remaining records in the batch
        process_batch(line_state['batch'])
        
    except Exception as e:
        LOGGER.error(f"Error in process_file: {str(e)}")
        raise
    
    finally:
        with singer.metrics.record_counter(stream_name) as counter:
            counter.increment(line_state['count'])
        
        if line_state['errors'] > 0:
            LOGGER.warning(f"Completed with {line_state['errors']} errors out of {line_state['count']} records")

def get_date_chunks(start_date, end_date, chunk_size_days=FLOODLIGHT_MAX_DAYS):
    """Generate date chunks for floodlight reports."""
    chunks = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return chunks

def update_report_date_range(service, profile_id, report_id, start_date, end_date):
    """Update the report's date range."""
    report = service.reports().get(profileId=profile_id, reportId=report_id).execute()
    
    if report.get("type") == "FLOODLIGHT":
        report["floodlightCriteria"]["dateRange"] = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d")
        }
    else:
        report["criteria"]["dateRange"] = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d")
        }
    
    return service.reports().update(profileId=profile_id, reportId=report_id, body=report).execute()

def sync_report(service, field_type_lookup, profile_id, report_config):
    report_name = report_config.get("name")
    report_start_date = report_config.get("start_date")
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

    # Get the report's date range
    if report.get("type") == "FLOODLIGHT":
        date_range = report.get("floodlightCriteria", {}).get("dateRange", {})
    else:
        date_range = report.get("criteria", {}).get("dateRange", {})

    start_date = datetime.strptime(date_range.get("startDate", ""), "%Y-%m-%d").date()
    end_date = datetime.strptime(date_range.get("endDate", ""), "%Y-%m-%d").date()

    # For floodlight reports, split into chunks of 60 days
    if report.get("type") == "FLOODLIGHT":
        date_chunks = get_date_chunks(start_date, end_date)
        LOGGER.info("%s: Splitting into %d date chunks for floodlight report", stream_name, len(date_chunks))
    else:
        date_chunks = [(start_date, end_date)]

    for chunk_start, chunk_end in date_chunks:
        LOGGER.info("%s: Processing date range %s to %s", stream_name, chunk_start, chunk_end)
        
        # Update report date range for this chunk
        updated_report = update_report_date_range(service, profile_id, report_id, chunk_start, chunk_end)
        
        with singer.metrics.job_timer('run_report'):
            report_time = datetime.utcnow().isoformat() + 'Z'
            report_file = (
                service
                .reports()
                .run(profileId=profile_id, reportId=report_id)
                .execute()
            )

            report_file_id = report_file['id']

            sleep = 0
            start_time = time.time()
            while True:
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

                elif status == 'REPORT_AVAILABLE':
                    LOGGER.info('Report file %s had status of %s; beginning file processing.', report_file_id, status)
                    process_file(service, fieldmap, report_config, report_file_id, report_time)
                    break

                elif status != 'PROCESSING':
                    message = ('%s: report_id %s / file_id %s - File status is %s, processing failed'
                               % (stream_name, report_id, report_file_id, status))
                    LOGGER.error(message)
                    raise Exception(message)

                if time.time() - start_time > MAX_RETRY_ELAPSED_TIME:
                    message = ('%s: report_id %s / file_id %s - Max retry time exceeded'
                               % (stream_name, report_id, report_file_id))
                    LOGGER.error(message)
                    raise Exception(message)

def sync_reports(service, config, catalog, state):
    profile_id = config.get('profile_id')
    reports = []
    for stream in catalog.streams:
        mdata = singer.metadata.to_map(stream.metadata)
        root_metadata = mdata[()]
        if root_metadata.get('selected') is True:
            reports.append({
                'report_id': root_metadata['tap-doubleclick-campaign-manager.report-id'],
                'stream_name': stream.tap_stream_id,
                'stream_alias': stream.stream_alias
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

        sync_report(service, field_type_lookup, profile_id, report_config)

    state['reports'] = None
    state['current_report'] = None
    singer.write_state(state)
