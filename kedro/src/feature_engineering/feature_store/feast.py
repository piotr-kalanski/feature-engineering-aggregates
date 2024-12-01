# TODO
# class FeastFeatureStore
# https://rtd.feast.dev/en/master/index.html#feast.feature_store.FeatureStore.write_to_offline_store

# def read_from_feature_store(fv: FeatureView, config: AggregationsConfig) -> str:
# required_columns = ConfigParser.get_required_columns(config, fv.name)

# # Fetch and filter data
# store = FeatureStore(repo_path="path/to/your/feast/repo")
# features = store.get_feature_view(fv.name)
# dataframe = store.get_historical_features(features=features).to_df()
# dataframe = dataframe[list(required_columns)]

# return dataframe