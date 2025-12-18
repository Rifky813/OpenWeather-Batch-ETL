import json
import pandas as pd
from datetime import datetime
import os

def transform_weather_data(raw_data):
    """
    Tranforming nested JSON data to structured dictionary.
    """
    transformed = {
        'country': raw_data['sys'].get('country'),
        'city': raw_data.get('name'),
        'weather_main': raw_data['weather'][0].get('main'),
        'weather_description': raw_data['weather'][0].get('description'),
        'temp': raw_data['main'].get('temp'),
        'feels_like': raw_data['main'].get('feels_like'),
        'pressure': raw_data['main'].get('pressure'),
        'humidity': raw_data['main'].get('humidity'),
        'wind_speed': raw_data['wind'].get('speed'),
        'timezone': datetime.fromtimestamp(raw_data.get('timezone')).strftime('%d-%m-%Y %H:%M:%S')
    }

    return transformed

def save_processed_data(data):
    os.makedirs('data/processed', exist_ok=True)

    df = pd.DataFrame([data])

    # Use a filename safe for Windows (no colons)
    filename = f"data/processed/weather_clean_{datetime.now().strftime('%d-%m-%Y_%H-%M-%S')}.csv"
    df.to_csv(filename, index=False)

    print(f'Data stored to: {filename}')

    return data

if __name__ == '__main__':
    try:    
        raw_files = [f for f in os.listdir('data/raw') if f.endswith('.json')]
        if raw_files:
            latest_file = max([os.path.join('data/raw', f) for f in raw_files], key=os.path.getctime)
            
            with open(latest_file, 'r') as f:
                raw_data = json.load(f)

            clean_data = transform_weather_data(raw_data)
            print('Tranformation result:')
            print(clean_data)

            save_processed_data(clean_data)
        else:
            print('File not found in data/raw.')
    except Exception as e:
        print(f'Error: {e}')

