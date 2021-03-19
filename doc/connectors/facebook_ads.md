# Facebook Ads connector

Import data from facebook ads API.

## Data provider configuration

* `type`: `"FacebookAds"`
* `name`: str, required
* `auth_flow_id`: str

The `auth_flow_id` will be used to identify tokens relative to this connector in the secrets database.

```javascript
DATA_PROVIDERS: [
  type:         'FacebookAds'
  name:         '<name>'
  auth_flow_id:    '<auth_flow_id>'
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `data_kind`: str, required, the kind of data that will be fetched,
    * Possible values:
        * Campaigns
        * AdsUnderCampaign
        * AllAds
* `parameters`: dict, optional, a dict of parameters that will be applied against the retrieved data
  * `campaign_id` and `account_id` are optional parameters that are not used on every route, but must be set in the parameters
* `data_fields`: str, optional, a comma-separated list of fields that should be retrieved from an API request. The fields are depicted in the following documentation pages:
  * [Campaigns](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group)
  * [Ads under a campaign](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads)
  * [Ads for a specific account](https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/ads)


```javascript
DATA_SOURCES: [
  domain:    '<domain>',
  name:      '<name>',
  data_kind:    '<data_kind>',
  parameters:   '{"date_preset": "last_year"}',
]
```
