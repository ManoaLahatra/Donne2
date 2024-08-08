import pandas as pd
def transform_data(demographic_df, geographic_df, how, on):
    location_df = pd.merge(demographic_df, geographic_df, how=how, on=on)
    return location_df
def transform_to_dataframe(path_of_the_file):
    data_frame = pd.read_csv(path_of_the_file)
    return data_frame