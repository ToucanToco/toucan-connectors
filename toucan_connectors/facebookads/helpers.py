from enum import Enum


class FacebookadsDataKind(str, Enum):
    campaigns = 'Campaigns'
    ads_under_campaign = 'Ads'


# Lists built from:
# * https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads/
ALLOWED_PARAMETERS_MAP = {
    FacebookadsDataKind.campaigns: [
        'date_preset',
        'time_range',
    ],
    FacebookadsDataKind.ads_under_campaign: [
        'date_preset',
        'time_range',
        'updated_since',
        'effective_status',
    ],
}
