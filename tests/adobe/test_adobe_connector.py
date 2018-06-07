from toucan_connectors.adobe.adobe_connector import AdobeAnalyticsDataSource, AdobeAnalyticsConnector
from adobe_analytics import ReportDefinition
import responses

adobe_datasource = AdobeAnalyticsDataSource(
    name='name',
    domain='domain',
    suite_id='suite_id',
    dimensions=['dimension_1', 'dimension_2'],
    metrics=['metrics_1', 'metrics_2'],
    date_from='2018-06-07',
    date_to='2018-06-07'
)

adobe_connector = AdobeAnalyticsConnector(
    name='name',
    username='username',
    password='password'
)

js_suites = {'report_suites': [{'rsid': 'suite_id',
                                'site_title': 'site_title'}]}

js_report = {'report': {'data': [{'counts': ['0'],
    'day': 7,
    'month': 6,
    'name': 'Thu.  7 Jun. 2018',
    'year': 2018}],
  'elements': [{'id': 'datetime', 'name': 'Date'}],
  'metrics': [{'current': False,
    'decimals': 0,
    'id': 'pageviews',
    'latency': 626,
    'name': 'Page Views',
    'type': 'number'}],
  'period': 'Thu.  7 Jun. 2018',
  'reportSuite': {'id': 'suite_id', 'name': 'name'},
  'totals': ['0'],
  'type': 'overtime',
  'version': '1.4.17.2'},
 'runSeconds': 0,
 'waitSeconds': 0}


def test_get_report_definition():
    assert type(adobe_datasource.report_definition) == ReportDefinition

@responses.activate
def test_get_df():
    url_suite = adobe_connector.endpoint + '?method=Company.GetReportSuites'
    url_report_queue = adobe_connector.endpoint + '?method=Report.Queue'
    url_report_result = adobe_connector.endpoint + '?method=Report.Get'
    responses.add(responses.POST, url_suite, json=js_suites, match_querystring=True)
    responses.add(responses.POST, url_report_queue, json=js_report_result, match_querystring=True)
    responses.add(responses.POST, url_report_result, json=js_report_result, match_querystring=True)
    adobe_connector.get_df(adobe_datasource)
