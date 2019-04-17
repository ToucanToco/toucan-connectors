# FacebookInsights connector

Import data from facebook insights API.

## Data provider configuration

* `type`: `"MySQL"`
* `name`: str, required

```javascript
DATA_PROVIDERS: [
  type:    'facebook_insights',
  name:    '<name>'
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `metrics`: list of str, required, the list of metrics that need to be fetched like *page_total_actions*,
* `period`: str, optional, the aggregation period, defaults to `week`,
* `date_preset`: str, optional, a date range preset like *last_week* or *yesterday*, defaults to `last_30d`.


```javascript
DATA_SOURCES: [
  domain:    '<domain>',
  name:      '<name>',
  pages:  {
    // page_id: page_token
    '292074147300': 'EAAFXzXZAi46xkkOxuJtfOciUQbC8u8A1aU0M2CvRGJACGkSzPRoyQCtEB5Yo9dwsdASOfDzGhonJl49oG1SZct1LJsjfJIhmdnT9dH3x8QyYy4nJnmLd7LH8yOytdn8nbeN3F29yXDxKnOkkwZzGJLYyxuEbbrQAewhqxtz1ki1dxowDCRFcqMTq32CLrY2IFQoYj',
  },
  metrics: ['page_total_actions', 'page_impressions'],
  period: 'week',
  date_preset: 'last_year',
]
```

For more details on expected values and semantics, check https://developers.facebook.com/docs/graph-api/reference/v2.8/insights