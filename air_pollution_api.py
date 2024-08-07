import requests as rq
import pandas as pd

demographic_df = pd.read_csv('/home/haja/PycharmProjects/airPollution/Demographic_Data.csv')
geographic_df = pd.read_csv('/home/haja/PycharmProjects/airPollution/Geographic_Data.csv')
location_df = pd.merge(demographic_df, geographic_df, "inner", on='Location')

print(location_df)

API_KEY = '9adf7bbe7303db96a2a02a507440f534'
url = f'https://api.openweathermap.org/data/2.5/air_pollution'

cities = {
    'Los Angeles': {'lat': 34.0522, 'lon': 118.2437},
    'Paris': {'lat': 48.8566, 'lon': 2.3522},
    'Antananarivo': {'lat': -18.8792, 'lon': 47.5079},
    'Tokyo': {'lat': 35.6895, 'lon': 139.6917},
    'Lima': {'lat': -12.0464, 'lon': -77.0428},
    'Nairobi': {'lat': -1.2864, 'lon': 36.8172}
}

data_list = []

for city, coords in cities.items():
    if city in location_df['Location'].values :
        lat = coords['lat']
        lon = coords['lon']
        response = rq.get(f'{url}?lat={lat}&lon={lon}&appid={API_KEY}')
    if response.status_code == 200:
        data = response.json()
        data_list.append({
            'Location': city,
            'Data': data
        })
    else:
        print(f"error for {city}: {response.status_code}")

result_df = pd.DataFrame(data_list)

result_df.to_csv('/home/haja/PycharmProjects/airPollution/air_pollution_data.csv', index=False)

