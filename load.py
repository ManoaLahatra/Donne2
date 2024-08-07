import pandas as pd

def load_data(dataframe):
    data_loaded = pd.DataFrame(dataframe)
    result_data = data_loaded.to_csv('/home/haja/PycharmProjects/airPollution/air_pollution_data.csv', index=False)
    return result_data