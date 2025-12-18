CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50),
    country VARCHAR(10),
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    temp FLOAT,
    feels_like FLOAT,
    pressure INTEGER,
    humidity INTEGER,
    wind_speed FLOAT,
    timezone TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
