from enum import Enum
from typing import Dict, List


class FacebookadsDataKind(str, Enum):
    campaigns = 'Campaigns'
    ads_under_campaign = 'AdsUnderCampaign'
    all_ads = 'AllAds'


def has_next_page(data: dict) -> List[Dict]:
    if "paging" not in data:
        return False

    return "next" in data["paging"]


# Lists built from:
# * https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads/
ALLOWED_PARAMETERS_MAP = {
    FacebookadsDataKind.campaigns: [
        'date_preset',
        'time_range',
        'fields',
    ],
    FacebookadsDataKind.ads_under_campaign: [
        'date_preset',
        'time_range',
        'updated_since',
        'effective_status',
        'fields',
    ],
    FacebookadsDataKind.all_ads: ['fields'],
}
