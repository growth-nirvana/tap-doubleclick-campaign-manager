import re

from singer.catalog import Catalog, CatalogEntry, Schema
from tap_doubleclick_campaign_manager.schema import (
    SINGER_REPORT_FIELD,
    get_fields,
    get_schema,
    get_field_type_lookup
)

def sanitize_name(report_name):
    report_name = re.sub(r'[\s\-\/]', '_', report_name.lower())
    return re.sub(r'[^a-z0-9_]', '', report_name)

def discover_streams(service, config):
    profile_id = config.get('profile_id')

    reports = (
        service
        .reports()
        .list(profileId=profile_id)
        .execute()
        .get('items')
    )

    reports = sorted(reports, key=lambda x: x['id'])
    field_type_lookup = get_field_type_lookup()
    catalog = Catalog([])

    for report in reports:
        raw_name = report['name']
        report_id = report['id']
        base_stream_name = sanitize_name(raw_name)
        stream_name = f"{base_stream_name}_{report_id}"  # ensures uniqueness
        tap_stream_id = stream_name  # same as stream name for consistency

        fieldmap = get_fields(field_type_lookup, report)
        schema_dict = get_schema(stream_name, fieldmap)
        schema = Schema.from_dict(schema_dict)

        metadata = [
            {
                'metadata': {
                    'tap-doubleclick-campaign-manager.report-id': report_id
                },
                'breadcrumb': []
            }
        ]
        for prop in schema_dict['properties'].keys():
            metadata.append({
                'metadata': {'inclusion': 'automatic'},
                'breadcrumb': ['properties', prop]
            })

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=tap_stream_id,
            stream_alias=stream_name,
            key_properties=[],
            schema=schema,
            metadata=metadata
        ))

    return catalog.to_dict()
