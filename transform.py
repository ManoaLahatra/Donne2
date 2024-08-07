import pandas as pd

def transform_data(demographic_df, geographic_df, how, on):
    location_df = pd.merge(demographic_df, geographic_df, how="inner", on='Location')
    return location_df
