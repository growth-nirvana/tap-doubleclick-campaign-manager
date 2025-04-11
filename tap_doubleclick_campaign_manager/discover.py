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
        .get('items', [])
    )

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

    for tap_stream_id, cfg in report_configs.items():
        stream_name = cfg["stream_name"]
        report = cfg["report"]

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

    return catalog.to_dict()
