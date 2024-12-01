from feature_engineering.models import (
    FeatureView,
    SimpleMetric,
    PartitionKeys,
    DateKey,
    MetricAggregationConfig,
    Aggregation,
    AggregationsConfig,
    Range,
    AggregateConfig,
)

orders_feature_view = FeatureView("orders")
dispatch_feature_view = FeatureView("dispatch")

order_pay_rate_metric = SimpleMetric("orderpayrate", "order_pay_rate")
order_bill_rate_metric = SimpleMetric("orderbillrate", "order_bill_rate")
dispatch_pay_rate_metric = SimpleMetric("dispatchpayrate", "dispatch_pay_rate")
dispatch_bill_rate_metric = SimpleMetric("dispatchbillrate", "dispatch_bill_rate")
rating_metric = SimpleMetric("rating", "rating")
total_worked_hours_metric = SimpleMetric("workedhours", "total_worked_hours")

zone_partition_keys = PartitionKeys("zone", ["zone_key"])
customer_partition_keys = PartitionKeys("customer", ["customer_key"])
job_duty_partition_keys = PartitionKeys("jobduty", ["job_duty"])
associate_account_partition_keys = PartitionKeys("associate", ["associate_account_prism_key"])
customer_job_duty_partition_keys = PartitionKeys("customer_jobduty", ["customer_key", "job_duty"])

created_date_key = DateKey("created_date")
shift_date_key = DateKey("shift_date")

order_pay_rate_aggregation_config = MetricAggregationConfig(order_pay_rate_metric, [Aggregation.Mean, Aggregation.Min, Aggregation.Max])
order_bill_rate_aggregation_config = MetricAggregationConfig(order_bill_rate_metric, [Aggregation.Mean, Aggregation.Min, Aggregation.Max])
dispatch_pay_rate_aggregation_config = MetricAggregationConfig(dispatch_pay_rate_metric, [Aggregation.Mean, Aggregation.Min, Aggregation.Max])
dispatch_bill_rate_aggregation_config = MetricAggregationConfig(dispatch_bill_rate_metric, [Aggregation.Mean, Aggregation.Min, Aggregation.Max])
rating_aggregation_config = MetricAggregationConfig(rating_metric, [Aggregation.Mean, Aggregation.Min, Aggregation.Max])
total_worked_hours_aggregation_config = MetricAggregationConfig(total_worked_hours_metric, [Aggregation.Mean, Aggregation.Sum])

orders_zone_agg_config = AggregateConfig(
  source=orders_feature_view,
  partitions=zone_partition_keys,
  date_column=created_date_key,
  metrics_aggregations=[
      order_pay_rate_aggregation_config,
      order_bill_rate_aggregation_config,
  ],
  ranges=[Range.Before_28_days, Range.Before_56_days]
)
orders_job_duty_agg_config = AggregateConfig(
  source=orders_feature_view,
  partitions=job_duty_partition_keys,
  date_column=created_date_key,
  metrics_aggregations=[
      order_pay_rate_aggregation_config,
      order_bill_rate_aggregation_config,
  ],
  ranges=[Range.Before_28_days, Range.Before_56_days]
)
dispatch_zone_agg_config = AggregateConfig(
  source=dispatch_feature_view,
  partitions=zone_partition_keys,
  date_column=shift_date_key,
  metrics_aggregations=[
      dispatch_pay_rate_aggregation_config,
      dispatch_bill_rate_aggregation_config,
      rating_aggregation_config,
      total_worked_hours_aggregation_config
  ],
  ranges=[Range.Before_28_days, Range.Before_56_days]
)
dispatch_job_duty_agg_config = AggregateConfig(
  source=dispatch_feature_view,
  partitions=job_duty_partition_keys,
  date_column=shift_date_key,
  metrics_aggregations=[
      dispatch_pay_rate_aggregation_config,
      dispatch_bill_rate_aggregation_config,
      rating_aggregation_config,
      total_worked_hours_aggregation_config
  ],
  ranges=[Range.Before_28_days, Range.Before_56_days]
)

config = AggregationsConfig([
  orders_zone_agg_config,
  orders_job_duty_agg_config,
  dispatch_zone_agg_config,
  dispatch_job_duty_agg_config
])
