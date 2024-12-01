from typing import List

import numpy as np
import pandas as pd

from feature_engineering.models import FeatureView
from feature_engineering.feature_store.base import FeatureStore


dates = pd.date_range('2020-01-01', periods=100, freq='1D')
count = 1000

class FakeFeatureStore(FeatureStore):

  def read_feature_view(self, fv: FeatureView, selected_columns: List[str]) -> pd.DataFrame:
    if fv.name == "orders":
      df = pd.DataFrame(data={
        "created_date": np.random.choice(dates, count),
        "job_order_key": np.random.choice([f"jo{i}"for i in range(500)], count),
        "customer_key": np.random.choice([f"jo{i}"for i in range(100)], count),
        "job_duty": np.random.choice([f"jd{i}"for i in range(100)], count),
        "zone_key": np.random.choice([f"z{i}"for i in range(100)], count),
        "order_pay_rate": np.random.normal(15, 2, count),
        "order_bill_rate": np.random.normal(30, 3, count)
    })
    elif fv.name == "dispatch":
      df = pd.DataFrame(data={
        "shift_date": np.random.choice(dates, count),
        "associate_account_prism_key": np.random.choice([f"a{i}"for i in range(1000)], count),
        "job_order_key": np.random.choice([f"jo{i}"for i in range(500)], count),
        "customer_key": np.random.choice([f"jo{i}"for i in range(100)], count),
        "job_duty": np.random.choice([f"jd{i}"for i in range(100)], count),
        "zone_key": np.random.choice([f"z{i}"for i in range(100)], count),
        "shift_start_hour": np.random.choice([8,8.5,6,6.5], count),
        "dispatch_pay_rate": np.random.normal(15, 2, count),
        "dispatch_bill_rate": np.random.normal(30, 3, count),
        "rating": np.random.choice(list(range(6)), count),
        "total_worked_hours": np.random.choice([8,9,10], count),
    })

    return df[selected_columns]

  def write_to_offline_store(self, df: pd.DataFrame, fv: FeatureView) -> None:
    pass
