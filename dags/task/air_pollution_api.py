import requests as rq
import pandas as pd
import os
import sys

sys.path.append('/home/riantsoa/airflow/dags/task')

from transform import transform_data, transform_to_dataframe
from load import load_data

def extract_data():
    API_KEY = '9adf7bbe7303db96a2a02a507440f534'
    url = 'https://api.openweathermap.org/data/2.5/air_pollution'

    cities = {
        'Los Angeles': {'lat': 34.0522, 'lon': 118.2437},
        'Paris': {'lat': 48.8566, 'lon': 2.3522},
        'Antananarivo': {'lat': -18.8792, 'lon': 47.5079},
        'Tokyo': {'lat': 35.6895, 'lon': 139.6917},
        'Lima': {'lat': -12.0464, 'lon': -77.0428},
        'Nairobi': {'lat': -1.2864, 'lon': 36.8172}
    }

    data_list = []

    demographic_df = transform_to_dataframe('/home/riantsoa/airflow/Demographic_Data.csv')
    geographic_df = transform_to_dataframe('/home/riantsoa/airflow/Geographic_Data.csv')
    location_df = transform_data(demographic_df, geographic_df, "inner", on='Location')

    for city, coords in cities.items():
        if city in location_df['Location'].values:
            lat = coords['lat']
            lon = coords['lon']
            try:
                response = rq.get(f'{url}?lat={lat}&lon={lon}&appid={API_KEY}')
                response.raise_for_status()

                data = response.json()
                coord = data.get('coord', {})
                aqi = data['list'][0]['main'].get('aqi', None)
                components = data['list'][0]['components']
                dt = data['list'][0].get('dt', None)

                if aqi is not None and components:
                    data_list.append({
                        'Location': city,
                        'Latitude': coord.get('lat', None),
                        'Longitude': coord.get('lon', None),
                        'AQI': aqi,
                        'CO': components.get('co', None),
                        'NO': components.get('no', None),
                        'NO2': components.get('no2', None),
                        'O3': components.get('o3', None),
                        'SO2': components.get('so2', None),
                        'PM2_5': components.get('pm2_5', None),
                        'PM10': components.get('pm10', None),
                        'NH3': components.get('nh3', None),
                        'Timestamp': dt
                    })
                    print(f"Data successfully retrieved for {city}")
                else:
                    print(f"Incomplete data for {city}. Skipping entry.")
            except rq.exceptions.RequestException as e:
                print(f"Request failed for {city}: {e}")

    result_df = pd.DataFrame(data_list)

    load_data(result_df)

if __name__ == '__main__':
    extract_data()
