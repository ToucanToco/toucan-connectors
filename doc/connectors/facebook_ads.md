# FacebookInsights connector

Import data from facebook ads API.

## Data provider configuration

* `type`: `"Facebookads"`
* `name`: str, required
* `token`: str, required
* `account_id`: str, required

```javascript
DATA_PROVIDERS: [
  type:         'Facebookads',
  name:         '<name>',
  token:        '<token>',
  account_id:   '<account_id>'
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `data_kind`: str, required, the kind of data that will be fetched,
    * Possible values:
        * Campaigns
        * Ads
* `parameters`: dict, optional, a dict of parameters that will be applied against the retrieved data,
* `campaign_id`: str, optional, a campaign id that is needed when `data_kind` equals to `Ads`.


```javascript
DATA_SOURCES: [
  domain:    '<domain>',
  name:      '<name>',
  data_kind:    '<data_kind>',
  parameters:   '{"date_preset": "last_year"}',
  campaign_id:  '<campaign_id>';
]
```
