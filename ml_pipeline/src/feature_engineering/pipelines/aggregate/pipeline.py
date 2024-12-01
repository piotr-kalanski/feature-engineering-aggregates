from typing import List, Dict

from kedro.pipeline import Pipeline, node
from kedro.pipeline.node import Node

from feature_engineering.models import AggregationsConfig, PartitionKeys, AggregateConfig, FeatureView
from feature_engineering.feature_store.fake import FakeFeatureStore
from feature_engineering.execution_engine.duckdb import DuckDBExecutionEngine
from feature_engineering.configuration import config

feature_store = FakeFeatureStore()
execution_engine = DuckDBExecutionEngine()


class ConfigParser:

  # TODO - move to AggregationsConfig
  @staticmethod
  def get_required_columns(config: AggregationsConfig, feature_view_name: str) -> List[str]:
    required_columns = set()
    for agg_config in config.config:
      if agg_config.source.name == feature_view_name:
        required_columns.update(agg_config.partitions.columns)
        required_columns.add(agg_config.date_column.column)
        # TODO - support for ratio metrics
        required_columns.update([m.metric.column for m in agg_config.metrics_aggregations])
    return list(required_columns)

  @staticmethod
  def get_unique_partitions(config: AggregationsConfig) -> List[PartitionKeys]:
    unique_keys = set()
    result = []
    for agg_config in config.config:
      key = agg_config.partitions.short_name
      if key not in unique_keys:
        unique_keys.add(key)
        result.append(agg_config.partitions)
    return result

  @staticmethod
  def group_by_feature_view(config: AggregationsConfig) -> Dict[str, List[AggregateConfig]]:
    feature_view_groups = {}
    for agg_config in config.config:
      if agg_config.source.name not in feature_view_groups:
        feature_view_groups[agg_config.source.name] = []
        feature_view_groups[agg_config.source.name].append(agg_config)
    return feature_view_groups


def create_read_node(fv: FeatureView, config: AggregationsConfig) -> Node:
  return node(
    func=lambda: feature_store.read_from_feature_store(fv, config),
    inputs=None,
    outputs=f"temp_{fv.name}",
    name=f"read_{fv.name}"
  )

def create_aggregate_node(agg_config: AggregateConfig, temp_table_name: str) -> Node:
  return node(
    func=lambda df: execution_engine.aggregate(agg_config, df),
    inputs=temp_table_name,
    outputs=f"agg_{agg_config.source.name}_{agg_config.partitions.short_name}_{agg_config.date_column.column}",
    name=f"aggregate_{agg_config.source.name}_{agg_config.partitions.short_name}"
  )

def create_merge_node(partition: PartitionKeys) -> Node:
  return node(
    func=lambda: execution_engine.merge(partition),
    inputs=None, # TODO - add inputs from each aggregate node
    outputs=f"merged_table_{partition.short_name}",
    name=f"merge_{partition.short_name}"
  )

def create_write_node(partition: PartitionKeys) -> Node:
    return node(
      func=lambda table_name: feature_store.write_to_offline_store(table_name),
      inputs=f"merged_table_{partition.short_name}",
      outputs=None,
      name=f"write_{partition.short_name}"
    )

def generate_pipeline(config: AggregationsConfig) -> Pipeline:
  nodes = []

  # Read nodes
  feature_view_groups = ConfigParser.group_by_feature_view(config)
  temp_table_map = {}
  for fv_name, agg_configs in feature_view_groups.items():
    fv = agg_configs[0].source
    nodes.append(create_read_node(fv, config))
    temp_table_map[fv.name] = f"temp_{fv.name}"

  # Aggregate nodes
  for agg_config in config.config:
    temp_table_name = temp_table_map[agg_config.source.name]
    nodes.append(create_aggregate_node(agg_config, temp_table_name))

  # Merge nodes
  for partition in ConfigParser.get_unique_partitions(config):
    nodes.append(create_merge_node(partition))
    nodes.append(create_write_node(partition))

  return Pipeline(nodes)


def create_pipeline(**kwargs):
  return generate_pipeline(config)
