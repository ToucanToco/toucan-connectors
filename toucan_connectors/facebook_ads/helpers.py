from enum import Enum
from typing import Dict, List


class FacebookadsDataKind(str, Enum):
    campaigns = 'Campaigns'
    ads_under_campaign = 'AdsUnderCampaign'
    all_ads = 'AllAds'


def has_next_page(data: dict) -> List[Dict]:
    if 'paging' not in data:
        return False

    return 'next' in data['paging']
