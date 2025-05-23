# ruff: noqa: N815,N806
from logging import getLogger

try:
    import pandas as pd
    from apiclient.discovery import build
    from oauth2client.service_account import ServiceAccountCredentials

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from pydantic import BaseModel, ConfigDict, Field

from toucan_connectors.common import nosql_apply_parameters_to_query
from toucan_connectors.google_credentials import GoogleCredentials
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

API = "analyticsreporting"
SCOPE = "https://www.googleapis.com/auth/analytics.readonly"
VERSION = "v4"


class Dimension(BaseModel):
    name: str
    histogramBuckets: list[str] | None = None


class DimensionFilter(BaseModel):
    dimensionName: str
    operator: str
    expressions: list[str] | None = None
    caseSensitive: bool = False
    model_config = ConfigDict(extra="allow")


class DimensionFilterClause(BaseModel):
    operator: str
    filters: list[DimensionFilter]


class DateRange(BaseModel):
    startDate: str
    endDate: str


class Metric(BaseModel):
    expression: str
    alias: str | None = None
    model_config = ConfigDict(extra="allow")


class MetricFilter(BaseModel):
    metricName: str
    operator: str
    comparisonValue: str
    model_config = ConfigDict(extra="allow")


class MetricFilterClause(BaseModel):
    operator: str
    filters: list[MetricFilter]


class OrderBy(BaseModel):
    fieldName: str
    orderType: str | None = None
    sortOrder: str | None = None


class Pivot(BaseModel):
    dimensions: list[Dimension] | None = None
    dimensionFilterClauses: list[DimensionFilterClause] | None = None
    metrics: list[Metric] | None = None
    startGroup: int | None = None
    maxGroupCount: int | None = None


class Cohort(BaseModel):
    name: str
    type: str
    dateRange: DateRange | None = None


class CohortGroup(BaseModel):
    cohorts: list[Cohort]
    lifetimeValue: bool = False


class Segment(BaseModel):
    segmentId: str | None = None
    # TODO dynamicSegment: DynamicSegment


class ReportRequest(BaseModel):
    viewId: str
    dateRanges: list[DateRange] | None = None
    samplingLevel: str | None = None
    dimensions: list[Dimension] | None = None
    dimensionFilterClauses: list[DimensionFilterClause] | None = None
    metrics: list[Metric] | None = None
    metricFilterClauses: list[MetricFilterClause] | None = None
    filtersExpression: str = ""
    orderBys: list[OrderBy] = []
    segments: list[Segment] = []
    pivots: list[Pivot] | None = None
    cohortGroup: CohortGroup | None = None
    pageToken: str = ""
    pageSize: int = 10000
    includeEmptyRows: bool = False
    hideTotals: bool = False
    hideValueRanges: bool = False


def get_dict_from_response(report, request_date_ranges):
    columnHeader = report.get("columnHeader", {})
    dimensionHeaders = columnHeader.get("dimensions", [])
    metricHeaders = columnHeader.get("metricHeader", {}).get("metricHeaderEntries", [])
    rows = report.get("data", {}).get("rows", [])

    all_rows = []
    for row_index, row in enumerate(rows):
        dimensions = row.get("dimensions", [])
        dateRangeValues = row.get("metrics", [])

        for i, values in enumerate(dateRangeValues):
            for metricHeader, value in zip(metricHeaders, values.get("values"), strict=False):
                row_dict = {
                    "row_index": row_index,
                    "date_range_id": i,
                    "metric_name": metricHeader.get("name"),
                }

                if request_date_ranges and (len(request_date_ranges) >= i):
                    row_dict["start_date"] = request_date_ranges[i].startDate
                    row_dict["end_date"] = request_date_ranges[i].endDate

                if metricHeader.get("type") == "INTEGER":
                    row_dict["metric_value"] = int(value)
                elif metricHeader.get("type") == "FLOAT":
                    row_dict["metric_value"] = float(value)
                else:
                    row_dict["metric_value"] = value

                for dimension_name, dimension_value in zip(dimensionHeaders, dimensions, strict=False):
                    row_dict[dimension_name] = dimension_value

                all_rows.append(row_dict)

    return all_rows


def get_query_results(service, report_request):
    response = service.reports().batchGet(body={"reportRequests": report_request.dict()}).execute()
    return response.get("reports", [])[0]


class GoogleAnalyticsDataSource(ToucanDataSource):
    report_request: ReportRequest = Field(
        ...,
        title="Report request",
        description="See the complete "
        '<a href="https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#reportrequest" target="_blank">Google documentation</a>',  # noqa: E501
    )


class GoogleAnalyticsConnector(ToucanConnector, data_source_model=GoogleAnalyticsDataSource):
    credentials: GoogleCredentials = Field(
        ...,
        title="Google Credentials",
        description="For authentication, download an authentication file from your "
        '<a href="https://console.developers.google.com/apis/credentials" target="_blank">Google Console</a> '
        "and use the values here. This is an oauth2 credential file. For more information see this "
        '<a href="https://gspread.readthedocs.io/en/latest/oauth2.html" target="_blank">documentation</a>. '
        'You should use "service_account" credentials, which is the preferred type of credentials '
        "to use when authenticating on behalf of a service or application",
    )
    scope: list[str] = Field(
        [SCOPE],
        description="OAuth 2.0 scopes define the level of access you need to "
        "request the Google APIs. For more information, see this "
        '<a href="https://developers.google.com/identity/protocols/googlescopes" target="_blank">documentation</a>',
    )

    def _retrieve_data(self, data_source: GoogleAnalyticsDataSource) -> "pd.DataFrame":
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials.dict(), self.scope)
        service = build(API, VERSION, credentials=credentials)
        report_request = ReportRequest(
            **nosql_apply_parameters_to_query(data_source.report_request.dict(), data_source.parameters)
        )
        report = get_query_results(service, report_request)
        reports_data = [pd.DataFrame(get_dict_from_response(report, report_request.dateRanges))]

        while "nextPageToken" in report:
            report_request.pageToken = report["nextPageToken"]

            report = get_query_results(service, report_request)
            reports_data.append(pd.DataFrame(get_dict_from_response(report, report_request.dateRanges)))

        return pd.concat(reports_data)
