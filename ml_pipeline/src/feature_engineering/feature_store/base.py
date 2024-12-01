from abc import ABC, abstractmethod
from typing import List

import pandas as pd

from feature_engineering.models import FeatureView


class FeatureStore(ABC):
  @abstractmethod
  def read_feature_view(self, fv: FeatureView, selected_columns: List[str]) -> pd.DataFrame:
    pass

  @abstractmethod
  def write_to_offline_store(self, df: pd.DataFrame, fv: FeatureView) -> None:
    pass
