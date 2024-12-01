from dataclasses import dataclass
from enum import Enum
from typing import List


class Range(Enum):
  Before_28_days = "Before_28_days"
  Before_56_days = "Before_56_days"
  Before_current_row = "Before_current_row"

class Aggregation(Enum):
  Mean = "avg"
  Sum = "sum"
  Min = "min"
  Max = "max"
  Mode = "mode"
  Lag = "lag"

@dataclass
class FeatureView:
  name: str

@dataclass
class Metric:
  name: str

@dataclass
class SimpleMetric(Metric):
  column: str

@dataclass
class RatioMetric(Metric):
  nominator_column: str
  denominator_column: str

@dataclass
class PartitionKeys:
  short_name: str
  columns: List[str]

@dataclass
class DateKey:
  column: str

@dataclass
class MetricAggregationConfig:
  metric: Metric
  aggregations: List[Aggregation]

@dataclass
class AggregateConfig:
  source: FeatureView
  partitions: PartitionKeys
  date_column: DateKey
  metrics_aggregations: List[MetricAggregationConfig]
  ranges: List[Range]

@dataclass
class AggregationsConfig:
  config: List[AggregateConfig]
