from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from feature_engineering.models import AggregateConfig, PartitionKeys


class ExecutionEngine(ABC):
  @abstractmethod
  def aggregate(self, df: pd.DataFrame, config: AggregateConfig) -> Any:
    pass

  @abstractmethod
  def merge(self, partition: PartitionKeys) -> pd.DataFrame:
    pass
  