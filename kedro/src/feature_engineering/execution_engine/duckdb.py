from typing import List
import logging

import duckdb
import pandas as pd

from feature_engineering.models import (
    PartitionKeys,
    Metric, SimpleMetric, RatioMetric, Range, Aggregation, DateKey, AggregateConfig
)
from feature_engineering.execution_engine.base import ExecutionEngine

logger = logging.getLogger(__name__)


class SQLQueryBuilder:
  @staticmethod
  def build_partition_clause(partition: PartitionKeys) -> str:
    return ", ".join(partition.columns)

  @staticmethod
  def build_rolling_window_aggregation(
      # TODO - support for RatioMetric
    metric: SimpleMetric, agg: Aggregation, range_: Range, partition_clause: str, date_column: DateKey
  ) -> str:
    range_clause = {
      Range.Before_28_days: "ROWS BETWEEN 28 PRECEDING AND CURRENT ROW",
      Range.Before_56_days: "ROWS BETWEEN 56 PRECEDING AND CURRENT ROW",
      Range.Before_current_row: "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    }[range_]

    return f"""
    {agg.value}({metric.column}) OVER (
      PARTITION BY {partition_clause}
      ORDER BY {date_column.column} {range_clause}
    ) AS {metric.column}_{agg.value.lower()}_{range_.value.lower()}
    """

  @staticmethod
  def build_lag_aggregation(metric: str, partition_clause: str, date_column: str) -> str:
    return f"LAG({metric}) OVER (PARTITION BY {partition_clause} ORDER BY {date_column}) AS {metric}_lag"


class DuckDBExecutionEngine(ExecutionEngine):

  @property
  def target_date_column_name(self) -> str:
    return "event_timestamp"

  def aggregate(self, df: pd.DataFrame, config: AggregateConfig) -> pd.DataFrame:
    temp_table_name = f"agg_{config.source.name}_{'_'.join(config.partitions.columns)}_{config.date_column.column}"
    partition_clause = SQLQueryBuilder.build_partition_clause(config.partitions)
    source_table = "df"

    query = f"CREATE OR REPLACE TEMP TABLE {temp_table_name} AS SELECT {partition_clause}, {config.date_column.column} as {self.target_date_column_name},"
    for metric_aggregations in config.metrics_aggregations:
      metric = metric_aggregations.metric
      for agg in metric_aggregations.aggregations:
        if agg == Aggregation.Lag:
          query += SQLQueryBuilder.build_lag_aggregation(metric, partition_clause, config.date_column) + ","
        else:
          for range_ in config.ranges:
            query += SQLQueryBuilder.build_rolling_window_aggregation(
              metric, agg, range_, partition_clause, config.date_column
            ) + ","

      query = query.rstrip(",") + f" FROM {source_table}"

      logger.info(f"Executing query: {query}")
      duckdb.sql(query)

      return temp_table_name

  def _create_unique_partitions(self, agg_tables: List[str], partition: PartitionKeys) -> str:
    partition_clause = SQLQueryBuilder.build_partition_clause(partition)

    union_query = " UNION ".join([
      f"SELECT DISTINCT {partition_clause}, {self.target_date_column_name} FROM {table}"
      for table in agg_tables
    ])
    query = f"CREATE OR REPLACE TEMP TABLE unique_partitions_dates AS {union_query}"

    logger.info(f"Executing query: {query}")
    duckdb.sql(query)

    return "unique_partitions_dates"

  def merge(self, partition: PartitionKeys) -> pd.DataFrame:
    partition_clause = SQLQueryBuilder.build_partition_clause(partition)

    # TODO - split into smaller methods
    # get aggregated tables with same partition
    agg_tables = [
      table[0] for table in duckdb.sql("SHOW TABLES").fetchall()
      if table[0].startswith("agg_")
    ]

    # Create unique partitions
    unique_partitions_table = self._create_unique_partitions(agg_tables, partition)

    # Perform the merge
    merged_table_name = f"merged_table_{partition_clause}"
    query = f"SELECT * FROM {unique_partitions_table}"
    for table in agg_tables:
      query += f" LEFT JOIN {table} USING ({partition_clause}, {self.target_date_column_name})"

    logger.info(f"Executing query: {query}")
    return duckdb.query(query).to_df()
