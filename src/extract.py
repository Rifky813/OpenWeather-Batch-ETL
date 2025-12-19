import requests
import json
import os
from datetime import datetime

API_KEY = os.getenv('OPENWEATHER_API_KEY')
CITY = 'Bekasi'
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

def fetch_weather_data():
    """
    Fetch the data from OpenWeatherMap API
    """
    if not API_KEY:
        raise ValueError("API Key not found.")
    
    params = {
        'q': CITY,
        'appid': API_KEY,
        'units': 'metric'
    }

    print('Fetching weather data in {CITY}..')
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        print('Successfully fetched data.')
        return response.json()
    else:
        print(f'Failed to fetch data. Status code: {response.status_code}')
        return None
    
def save_to_raw(data):
    """
    Save JSON data to data/raw folder.
    """
    os.makedirs('data/raw', exist_ok=True)
    filename = f'data/raw/weather_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'

    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

    print(f'Stored raw data in: {filename}')

if __name__ == '__main__':
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_raw(weather_data)
