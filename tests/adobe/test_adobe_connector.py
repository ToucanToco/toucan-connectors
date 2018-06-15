from toucan_connectors.adobe.adobe_connector import AdobeAnalyticsDataSource, \
    AdobeAnalyticsConnector
from adobe_analytics import ReportDefinition
import responses


adobe_datasource = AdobeAnalyticsDataSource(
    name='name',
    domain='domain',
    suite_id='suite_id',
    dimensions=['dimension_1', 'dimension_2'],
    metrics='metric',
    date_from='2018-06-07',
    date_to='2018-06-07',
    granularity="day"
)

adobe_connector = AdobeAnalyticsConnector(
    name='name',
    username='username',
    password='password'
)

js_suites = {'report_suites': [{'rsid': 'suite_id', 'site_title': 'site_title'}]}

js_queue = {'reportID': 1}

js_report = {
    'report': {'data': [
        {'counts': ['0'], 'day': 7, 'month': 6, 'name': 'Thu.  7 Jun. 2018', 'year': 2018}],
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


def test_dimenssions_dict():
    AdobeAnalyticsDataSource(
        name='name',
        domain='domain',
        suite_id='suite_id',
        dimensions=[{"id": "page", "top": 5000}],
        metrics='metric',
        date_from='2018-06-07',
        date_to='2018-06-07'
    )
    assert type(adobe_datasource.report_definition) == ReportDefinition


@responses.activate
def test_get_df():
    responses.add(responses.POST,
                  adobe_connector.endpoint + '?method=Company.GetReportSuites',
                  json=js_suites, match_querystring=True)

    responses.add(responses.POST,
                  adobe_connector.endpoint + '?method=Report.Queue',
                  json=js_queue, match_querystring=True)

    responses.add(responses.POST,
                  adobe_connector.endpoint + '?method=Report.Get',
                  json=js_report, match_querystring=True)

    df = adobe_connector.get_df(adobe_datasource)
    assert list(df.columns) == ['Date', 'Page Views']
    assert df.loc[0]['Page Views'] == '0'
