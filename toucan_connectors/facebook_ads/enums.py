from enum import Enum


class FacebookAdsDataKind(str, Enum):
    campaigns = 'Campaigns'
    ads_under_campaign = 'AdsUnderCampaign'
    all_ads = 'AllAds'
    insights = 'Insights'
