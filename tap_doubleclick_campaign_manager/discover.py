import re

import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

LOGGER = singer.get_logger()


def sanitize_name(report_name):
    report_name = re.sub(r'[\s\-\/]', '_', report_name.lower())
    return re.sub(r'[^a-z0-9_]', '', report_name)


def list_all_reports(service, profile_id):
    """Fetch all reports for a profile, paging through reports.list results."""
    reports = []
    page_token = None
    page_count = 0

    while True:
        request_kwargs = {
            'profileId': profile_id,
            'maxResults': 10,
        }
        if page_token:
            request_kwargs['pageToken'] = page_token

        response = service.reports().list(**request_kwargs).execute()
        reports.extend(response.get('items', []))
        page_count += 1

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    LOGGER.info(
        "Discovered %d report(s) across %d page(s) for profile %s",
        len(reports),
        page_count,
        profile_id,
    )
    return reports


def discover_streams(service, config):
    profile_id = config.get('profile_id')

    reports = list_all_reports(service, profile_id)

    reports = sorted(reports, key=lambda x: x['id'])
    report_configs = {}
    for report in reports:
        stream_base_name = sanitize_name(report['name'])
        tap_stream_id = f"{stream_base_name}_{report['id']}"  # unique identifier
        report_configs[tap_stream_id] = {
            "stream_name": stream_base_name,
            "tap_stream_id": tap_stream_id,
            "report": report
        }

    field_type_lookup = get_field_type_lookup()
    catalog = Catalog([])
    skipped_count = 0

    for tap_stream_id, cfg in report_configs.items():
        report = cfg["report"]

        try:
            fieldmap = get_fields(field_type_lookup, report)
            schema_dict = get_schema(tap_stream_id, fieldmap)
            schema = Schema.from_dict(schema_dict)

            metadata = [{
                'metadata': {
                    'tap-doubleclick-campaign-manager.report-id': report['id']
                },
                'breadcrumb': []
            }]

            for prop in schema_dict['properties'].keys():
                metadata.append({
                    'metadata': {
                        'inclusion': 'automatic'
                    },
                    'breadcrumb': ['properties', prop]
                })

            catalog.streams.append(CatalogEntry(
                stream=tap_stream_id,
                stream_alias=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=[],
                schema=schema,
                metadata=metadata
            ))
        except Exception as e:
            skipped_count += 1
            LOGGER.warning(
                "Skipping report %r (type=%s, id=%s): %s",
                report.get('name'),
                report.get('type'),
                report.get('id'),
                e,
            )

    if skipped_count:
        LOGGER.warning(
            "Skipped %d report(s) that could not be converted to catalog streams",
            skipped_count,
        )

    LOGGER.info(
        "Built catalog with %d stream(s) from %d report(s)",
        len(catalog.streams),
        len(reports),
    )

    return catalog.to_dict()
