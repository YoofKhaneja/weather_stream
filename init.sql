-- Current weather table
CREATE TABLE IF NOT EXISTS current_weather (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,
    temperature_f DECIMAL(5,2) NOT NULL,
    humidity INTEGER NOT NULL,
    pressure_hpa DECIMAL(7,2) NOT NULL,
    wind_speed_mph DECIMAL(6,2) NOT NULL,
    precipitation_inch DECIMAL(6,3) NOT NULL,
    weather_code INTEGER NOT NULL,
    summary VARCHAR(100) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_current_humidity CHECK (humidity BETWEEN 0 AND 100)
);

CREATE INDEX idx_current_city_state ON current_weather(city, state);
CREATE INDEX idx_current_recorded_at ON current_weather(recorded_at DESC);

-- Hourly forecast table (next 5 hours)
CREATE TABLE IF NOT EXISTS hourly_forecast (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,
    forecast_hour TIMESTAMP NOT NULL,
    temperature_f DECIMAL(5,2) NOT NULL,
    humidity INTEGER NOT NULL,
    pressure_hpa DECIMAL(7,2) NOT NULL,
    wind_speed_mph DECIMAL(6,2) NOT NULL,
    precipitation_inch DECIMAL(6,3) NOT NULL,
    weather_code INTEGER NOT NULL,
    summary VARCHAR(100) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_hourly_humidity CHECK (humidity BETWEEN 0 AND 100),
    UNIQUE(city, state, forecast_hour, recorded_at)
);

CREATE INDEX idx_hourly_city_state ON hourly_forecast(city, state);
CREATE INDEX idx_hourly_forecast_hour ON hourly_forecast(forecast_hour ASC);
CREATE INDEX idx_hourly_city_state_hour ON hourly_forecast(city, state, forecast_hour);

-- Latest current weather view
CREATE OR REPLACE VIEW latest_current_weather AS
SELECT DISTINCT ON (city, state)
    city, state, temperature_f, humidity, pressure_hpa,
    wind_speed_mph, precipitation_inch, weather_code, summary, recorded_at
FROM current_weather
ORDER BY city, state, recorded_at DESC;