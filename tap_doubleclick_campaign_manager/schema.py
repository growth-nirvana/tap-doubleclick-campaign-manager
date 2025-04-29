import os
import json
import re

DIMENSION_MAPPING = {
    # Floodlight variable dimensions
    "floodlightVariableDimension1": "hotel_name_string_",  # U1: Hotel Name
    "floodlightVariableDimension2": "currency_string_",  # U2: Currency
    "floodlightVariableDimension11": "offer_code_used_string_",  # U11: Offer Code Used
    "floodlightVariableDimension16": "event_string_",  # U16: Event
    "floodlightVariableDimension19": "artist_name_string_",  # U19: Artist Name
    "floodlightVariableDimension20": "venue_name_string_",  # U20: Venue Name
    
    # Standard DCM dimensions that need snake_case conversion
    "paidSearchCampaignId": "paid_search_campaign_id",
    "paidSearchAdGroupId": "paid_search_ad_group_id",
    "paidSearchKeywordId": "paid_search_keyword_id",
    "paidSearchAdvertiserId": "paid_search_advertiser_id",
    "paidSearchEngineAccount": "paid_search_engine_account",
    "paidSearchAdGroup": "paid_search_ad_group",
    "paidSearchAdvertiser": "paid_search_advertiser",
    "paidSearchCampaign": "paid_search_campaign",
    "paidSearchKeyword": "paid_search_keyword",
    "paidSearchMatchType": "paid_search_match_type",
    "packageRoadblock": "package_roadblock",
    "packageRoadblockId": "package_roadblock_id",
    "packageRoadblockStrategy": "package_roadblock_strategy",
    "dmaRegion": "designated_market_area_dma_",
    "site": "site_cm_360_",
    "placementId": "placement_id",
    "creativeId": "creative_id",
    "activityId": "activity_id",
    "campaignId": "campaign_id",
    "advertiserId": "advertiser_id",
    "adId": "ad_id",
    "floodlightConfigId": "floodlight_configuration",
    "totalRevenue": "total_revenue"
}

METRIC_MAPPING = {
    # Floodlight variable metrics from configuration
    "floodlightVariableMetric3": "revenue_number_",  # U3: Revenue
    "floodlightVariableMetric4": "number_of_nights_number_",  # U4: Number of Nights
    "floodlightVariableMetric9": "number_of_children_number_",  # U9: Number of Children
    "floodlightVariableMetric10": "number_of_adults_number_",  # U10: Number of Adults
    "floodlightVariableMetric11": "offer_code_used_number_",  # U11: Offer Code Used
    "floodlightVariableMetric12": "purchase_price_number_",  # U12: Purchase Price
    "floodlightVariableMetric16": "event_number_",  # U16: Event
    "floodlightVariableMetric17": "tickets_purchased_number_",  # U17: Tickets Purchased
    "floodlightVariableMetric18": "revenue_star_number",  # U18: Revenue*
    "floodlightVariableMetric19": "artist_name_number_",  # U19: Artist Name
    "floodlightVariableMetric20": "venue_name_number_",  # U20: Venue Name
    "floodlightVariableMetric24": "number_of_guests_number_",  # U24: Number of Guests
    "floodlightVariableMetric25": "transaction_id_number_",  # U25: Transaction ID
    
    # Activity metrics
    "activityViewThroughRevenue": "view_through_revenue",
    "activityClickThroughRevenue": "click_through_revenue",
    "activityClickThroughConversions": "click_through_conversions",
    "activityViewThroughConversions": "view_through_conversions",
    
    # Conversion metrics
    "totalConversions": "total_conversions",
    "totalConversionsRevenue": "total_conversions_revenue",

    # Standard metrics
    "activeViewEligibleImpressions": "active_view_eligible_impressions",
    "activeViewMeasurableImpressions": "active_view_measurable_impressions",
    "activeViewViewableImpressions": "active_view_viewable_impressions",
    "clickRate": "click_rate",
    "clicks": "clicks",
    "impressions": "impressions",
    "mediaCost": "media_cost",
    "richMediaVideoCompletions": "video_completions",
    "richMediaVideoPlays": "video_plays",
    "richMediaVideoViews": "video_views",
    "richMediaEngagements": "rich_media_engagements"
}

SINGER_REPORT_FIELD = '_sdc_report_time'
REPORT_ID_FIELD = '_sdc_report_id'
PROFILE_ID_FIELD = 'profile_id'

def get_field_type_lookup():
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'report_field_type_lookup.json')
    with open(path) as file:
        return json.load(file)

def report_dimension_fn(dimension):
    if isinstance(dimension, str):
        return dimension
    elif isinstance(dimension, dict):
        return dimension['name']
    raise Exception('Could not determine report dimensions')

def get_fields(field_type_lookup, report):
    report_type = report['type']
    if report_type == 'STANDARD':
        criteria_obj = report['criteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames']
    elif report_type == 'FLOODLIGHT':
        criteria_obj = report['floodlightCriteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames']
    elif report_type == 'CROSS_DIMENSION_REACH':
        criteria_obj = report['crossDimensionReachCriteria']
        dimensions = criteria_obj['breakdown']
        metric_names = criteria_obj['metricNames'] + criteria_obj['overlapMetricNames']
    elif report_type == 'PATH_TO_CONVERSION':
        criteria_obj = report['pathToConversionCriteria']
        dimensions = (
            criteria_obj['conversionDimensions'] +
            criteria_obj['perInteractionDimensions'] +
            criteria_obj['customFloodlightVariables']
        )
        metric_names = criteria_obj['metricNames']
    elif report_type == 'REACH':
        criteria_obj = report['reachCriteria']
        dimensions = criteria_obj['dimensions']
        metric_names = criteria_obj['metricNames'] + criteria_obj['reachByFrequencyMetricNames']
    else:
        raise Exception(f"Unknown report type: {report_type}")

    dimensions = list(map(report_dimension_fn, dimensions))
    metric_names = list(map(report_dimension_fn, metric_names))
    
    # Map all dimensions and metrics to Fivetran names
    mapped_dimensions = []
    for d in dimensions:
        if d in DIMENSION_MAPPING:
            mapped_dimensions.append(DIMENSION_MAPPING[d])
        else:
            mapped_dimensions.append(d.lower().replace(' ', '_'))
    
    mapped_metrics = []
    for m in metric_names:
        if m in METRIC_MAPPING:
            mapped_metrics.append(METRIC_MAPPING[m])
        else:
            mapped_metrics.append(m.lower().replace(' ', '_'))
    
    # Convert any remaining camelCase to snake_case and handle compound words
    def camel_to_snake(name):
        # Skip if already mapped
        if name in mapped_dimensions or name in mapped_metrics:
            return name
            
        # First convert camelCase to snake_case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        
        # Then handle compound words (e.g., totalconversions -> total_conversions)
        s3 = re.sub(r'([a-z])([A-Z])', r'\1_\2', s2)
        
        # Also handle cases where a word ends with a number
        s4 = re.sub(r'([a-z])(\d)', r'\1_\2', s3)
        
        return s4.lower()

    final_dimensions = [camel_to_snake(d) for d in mapped_dimensions]
    final_metrics = [camel_to_snake(m) for m in mapped_metrics]
    
    columns = final_dimensions + final_metrics

    fieldmap = []
    for column in columns:
        fieldmap.append({
            'name': column,
            'type': field_type_lookup.get(column, 'string')
        })

    return fieldmap

def convert_to_json_schema_type(non_json_type):
    if non_json_type == 'long':
        return 'integer'
    if non_json_type == 'double':
        return 'number'
    return non_json_type

def convert_to_json_schema_types(non_json_types):
    if isinstance(non_json_types, str):
        return [convert_to_json_schema_type(non_json_types)]
    return [convert_to_json_schema_type(t) for t in non_json_types]

def get_schema(stream_name, fieldmap):
    properties = {}

    properties[SINGER_REPORT_FIELD] = {
        'type': 'string',
        'format': 'date-time'
    }

    properties[REPORT_ID_FIELD] = {
        'type': 'integer'
    }

    properties[PROFILE_ID_FIELD] = {
        'type': 'integer'
    }

    for field in fieldmap:
        raw_type = field['type']
        json_types = convert_to_json_schema_types(raw_type)

        # Select preferred type (if multiple), log a warning
        preferred_order = ['integer', 'number', 'boolean', 'string']
        type_set = set(json_types)
        type_set.discard('null')

        selected_type = next((t for t in preferred_order if t in type_set), 'string')

        properties[field['name']] = {
            'type': ['null', selected_type]
        }

    schema = {
        'type': 'object',
        'properties': properties,
        'additionalProperties': False
    }

    return schema
