import pandas as pd

def transform_data(demographic_df, geographic_df, pollution_df):
    location_df = pd.merge(demographic_df, geographic_df, how="inner", on="Location")
    
    final_df = pd.merge(location_df, pollution_df, how="inner", on="Location")
    
    return final_df

def transform_to_dataframe(path_of_the_file):
    data_frame = pd.read_csv(path_of_the_file)
    return data_frame

pollution_file = '/home/riantsoa/airflow/air_pollution_data.csv'
demographic_file = '/home/riantsoa/airflow/Demographic_Data.csv'
geographic_file = '/home/riantsoa/airflow/Geographic_Data.csv'

pollution_df = transform_to_dataframe(pollution_file)
demographic_df = transform_to_dataframe(demographic_file)
geographic_df = transform_to_dataframe(geographic_file)

final_df = transform_data(demographic_df, geographic_df, pollution_df)

print(final_df)

final_df.to_csv('/home/riantsoa/airflow/data/final_merged_data.csv', index=False)
