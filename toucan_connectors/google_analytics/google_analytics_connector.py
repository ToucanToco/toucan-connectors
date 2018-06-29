# https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet

# https://github.com/ToucanToco/keel-billed-v2/blob/api-renault.toucantoco.com/powerstore-analytics/preprocess/augment.py

from enum import Enum
from typing import List

import pandas as pd
from pydantic import BaseModel

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.common import GoogleCredentials

VERSION = 'v4'


class Sampling(str, Enum):
    SAMPLING_UNSPECIFIED = 'SAMPLING_UNSPECIFIED'
    DEFAULT = 'DEFAULT'
    SMALL = 'SMALL'
    LARGE = 'LARGE'


class MetricType(str, Enum):
    METRIC_TYPE_UNSPECIFIED = 'METRIC_TYPE_UNSPECIFIED'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    CURRENCY = 'CURRENCY'
    PERCENT = 'PERCENT'
    TIME = 'TIME'


class OrderType(str, Enum):
    ORDER_TYPE_UNSPECIFIED = 'ORDER_TYPE_UNSPECIFIED'
    VALUE = 'VALUE'
    DELTA = 'DELTA'
    SMART = 'SMART'
    HISTOGRAM_BUCKET = 'HISTOGRAM_BUCKET'
    DIMENSION_AS_INTEGER = 'DIMENSION_AS_INTEGER'


class SortOrder(str, Enum):
    SORT_ORDER_UNSPECIFIED = 'SORT_ORDER_UNSPECIFIED'
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'


class FilterLogicalOperator(str, Enum):
    OPERATOR_UNSPECIFIED = 'OPERATOR_UNSPECIFIED'
    OR = 'OR'
    AND = 'AND'


class Operator(str, Enum):
    OPERATOR_UNSPECIFIED = 'OPERATOR_UNSPECIFIED'
    REGEXP = 'REGEXP'
    BEGINS_WITH = 'BEGINS_WITH'
    ENDS_WITH = 'ENDS_WITH'
    PARTIAL = 'PARTIAL'
    EXACT = 'EXACT'
    NUMERIC_EQUAL = 'NUMERIC_EQUAL'
    NUMERIC_GREATER_THAN = 'NUMERIC_GREATER_THAN'
    NUMERIC_LESS_THAN = 'NUMERIC_LESS_THAN'
    IN_LIST = 'IN_LIST'


class Type(str, Enum):
    UNSPECIFIED_COHORT_TYPE = "UNSPECIFIED_COHORT_TYPE"
    FIRST_VISIT_DATE = "FIRST_VISIT_DATE"


class Dimension(BaseModel):
    name: str
    histogramBuckets: List[str]
    pageToken: str = ''


class DimensionFilter(BaseModel):
    dimensionName: str
    operator: Operator
    expressions: List[str]
    caseSensitive: bool

    class Config:
        # TODO `not` param is not implemented
        allow_extra = True


class DimensionFilterClause(BaseModel):
    operator: FilterLogicalOperator
    filters: List[DimensionFilter]


class DateRange(BaseModel):
    startDate: str
    endDate: str


class Metric(BaseModel):
    expression: str
    alias: str
    formattingType: MetricType


class MetricFilter(BaseModel):
    metricName: str
    operator: Operator
    comparisonValue: str

    class Config:
        # TODO `not` param is not implemented
        allow_extra = True


class MetricFilterClause(BaseModel):
    operator: FilterLogicalOperator
    filters: List[MetricFilter]


class OrderBy(BaseModel):
    fieldName: str
    orderType: OrderType
    sortOrder: SortOrder


class Pivot(BaseModel):
    dimensions: List[Dimension]
    dimensionFilterClauses: List[DimensionFilterClause]
    metrics: List[Metric]
    startGroup: int
    maxGroupCount: int


class Cohort(BaseModel):
    name: str
    type: Type
    dateRage: DateRange


class CohortGroup(BaseModel):
    cohorts: List[Cohort]
    lifetimeValue: bool


class ReportRequests(BaseModel):
    viewId: str
    dateRanges: List[DateRange]
    samplingLevel: Sampling
    dimensions: List[Dimension]
    dimensionFilterClauses: List[DimensionFilterClause]
    metrics: List[Metric]
    metricsFilterClauses: List[MetricFilterClause]
    filtersExpression: str = ''
    orderBys: List[OrderBy] = []
    # TODO    segment: List[Segment]
    pivot: List[Pivot]
    cohortGroup: CohortGroup
    pageToken: str = ''
    pageSize: int = 10000
    includeEmptyRows: bool
    hideTotals: bool
    hideValueRanges: bool


class GoogleAnalyticsDataSource(ToucanDataSource):
    report_requests = List


class GoogleAnalyticsConnector(ToucanConnector):
    type = "GoogleAnalytics"
    data_source_model: GoogleAnalyticsDataSource

    credentials: GoogleCredentials
    scope: List[str] = ['https://www.googleapis.com/auth/analytics.readonly']

    def get_df(self, data_source: GoogleAnalyticsDataSource) -> pd.DataFrame:
        pass
