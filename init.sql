-- Current weather snapshots
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

CREATE INDEX IF NOT EXISTS idx_current_city_state
    ON current_weather(city, state);
CREATE INDEX IF NOT EXISTS idx_current_recorded_at
    ON current_weather(recorded_at DESC);

-- Comparison between current and nearest forecast
CREATE TABLE IF NOT EXISTS forecast_comparison (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,

    "current_time" TIMESTAMP NOT NULL,
    "current_weather_code" INTEGER NOT NULL,
    current_summary VARCHAR(100) NOT NULL,

    "forecast_hour" TIMESTAMP NOT NULL,
    "forecast_weather_code" INTEGER NOT NULL,
    forecast_summary VARCHAR(100) NOT NULL,

    temperature_f DECIMAL(5,2) NOT NULL,
    humidity INTEGER NOT NULL,
    pressure_hpa DECIMAL(7,2) NOT NULL,
    wind_speed_mph DECIMAL(6,2) NOT NULL,
    precipitation_inch DECIMAL(6,3) NOT NULL,

    message VARCHAR(255) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_forecast_humidity CHECK (humidity BETWEEN 0 AND 100),
    UNIQUE (city, state, "current_time", "forecast_hour")
);

CREATE INDEX IF NOT EXISTS idx_fc_city_state
    ON forecast_comparison(city, state);
CREATE INDEX IF NOT EXISTS idx_fc_times
    ON forecast_comparison(city, state, "current_time", "forecast_hour");