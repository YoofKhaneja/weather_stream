-- ============================================================================
-- WEATHER DATA ANALYTICS QUERIES
-- Purpose: Comprehensive analysis of temperature trends, patterns, and insights
-- ============================================================================

-- ----------------------------------------------------------------------------
-- SECTION 1: TEMPERATURE TRENDS OVER TIME
-- ----------------------------------------------------------------------------

-- 1.1: Hourly Temperature Trends by City (Last 24 Hours)
SELECT 
    city,
    state,
    DATE_TRUNC('hour', recorded_at) as hour,
    AVG(temperature_f) as avg_temp,
    MAX(temperature_f) as max_temp,
    MIN(temperature_f) as min_temp,
    STDDEV(temperature_f) as temp_variance,
    COUNT(*) as sample_count
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
GROUP BY city, state, DATE_TRUNC('hour', recorded_at)
ORDER BY city, state, hour DESC;

-- 1.2: Daily Temperature Trends (Last 30 Days)
SELECT 
    city,
    state,
    DATE_TRUNC('day', recorded_at) as day,
    AVG(temperature_f) as avg_temp,
    MAX(temperature_f) as max_temp,
    MIN(temperature_f) as min_temp,
    MAX(temperature_f) - MIN(temperature_f) as daily_range,
    COUNT(*) as readings
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY city, state, DATE_TRUNC('day', recorded_at)
ORDER BY city, state, day DESC;

-- 1.3: Weekly Temperature Comparison (Current Week vs Last Week)
WITH current_week AS (
    SELECT 
        city,
        state,
        AVG(temperature_f) as avg_temp_current,
        MAX(temperature_f) as max_temp_current,
        MIN(temperature_f) as min_temp_current
    FROM current_weather
    WHERE recorded_at >= DATE_TRUNC('week', NOW())
    GROUP BY city, state
),
last_week AS (
    SELECT 
        city,
        state,
        AVG(temperature_f) as avg_temp_last,
        MAX(temperature_f) as max_temp_last,
        MIN(temperature_f) as min_temp_last
    FROM current_weather
    WHERE recorded_at >= DATE_TRUNC('week', NOW() - INTERVAL '1 week')
      AND recorded_at < DATE_TRUNC('week', NOW())
    GROUP BY city, state
)
SELECT 
    cw.city,
    cw.state,
    ROUND(cw.avg_temp_current::numeric, 2) as current_week_avg,
    ROUND(lw.avg_temp_last::numeric, 2) as last_week_avg,
    ROUND((cw.avg_temp_current - lw.avg_temp_last)::numeric, 2) as temp_change,
    ROUND(((cw.avg_temp_current - lw.avg_temp_last) / NULLIF(lw.avg_temp_last, 0) * 100)::numeric, 2) as percent_change,
    CASE 
        WHEN cw.avg_temp_current > lw.avg_temp_last THEN '🔥 Warming'
        WHEN cw.avg_temp_current < lw.avg_temp_last THEN '❄️ Cooling'
        ELSE '→ Stable'
    END as trend
FROM current_week cw
JOIN last_week lw ON cw.city = lw.city AND cw.state = lw.state
ORDER BY ABS(cw.avg_temp_current - lw.avg_temp_last) DESC;

-- 1.4: Month-over-Month Temperature Comparison
WITH monthly_stats AS (
    SELECT 
        city,
        state,
        DATE_TRUNC('month', recorded_at) as month,
        AVG(temperature_f) as avg_temp,
        MAX(temperature_f) as max_temp,
        MIN(temperature_f) as min_temp,
        STDDEV(temperature_f) as temp_stddev
    FROM current_weather
    WHERE recorded_at >= NOW() - INTERVAL '6 months'
    GROUP BY city, state, DATE_TRUNC('month', recorded_at)
)
SELECT 
    city,
    state,
    month,
    ROUND(avg_temp::numeric, 2) as avg_temp,
    ROUND(max_temp::numeric, 2) as max_temp,
    ROUND(min_temp::numeric, 2) as min_temp,
    ROUND(temp_stddev::numeric, 2) as variability,
    ROUND((avg_temp - LAG(avg_temp) OVER (PARTITION BY city, state ORDER BY month))::numeric, 2) as change_from_prev_month
FROM monthly_stats
ORDER BY city, state, month DESC;

-- ----------------------------------------------------------------------------
-- SECTION 2: WEATHER PATTERN ANALYSIS
-- ----------------------------------------------------------------------------

-- 2.1: Weather Condition Distribution by City
SELECT 
    city,
    state,
    summary,
    COUNT(*) as occurrence_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city, state))::numeric, 2) as percentage,
    AVG(temperature_f) as avg_temp_during,
    AVG(humidity) as avg_humidity
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY city, state, summary
ORDER BY city, state, occurrence_count DESC;

-- 2.2: Weather Transition Patterns (What follows what?)
WITH weather_sequence AS (
    SELECT 
        city,
        state,
        summary as current_weather,
        LEAD(summary) OVER (PARTITION BY city, state ORDER BY recorded_at) as next_weather,
        recorded_at,
        temperature_f
    FROM current_weather
    WHERE recorded_at >= NOW() - INTERVAL '30 days'
)
SELECT 
    city,
    state,
    current_weather,
    next_weather,
    COUNT(*) as transition_count,
    ROUND(AVG(temperature_f)::numeric, 2) as avg_temp_at_transition
FROM weather_sequence
WHERE next_weather IS NOT NULL
GROUP BY city, state, current_weather, next_weather
HAVING COUNT(*) > 1
ORDER BY city, state, transition_count DESC;

-- 2.3: Precipitation Patterns
SELECT 
    city,
    state,
    DATE_TRUNC('day', recorded_at) as day,
    SUM(precipitation_inch) as total_precip,
    AVG(humidity) as avg_humidity,
    COUNT(CASE WHEN precipitation_inch > 0 THEN 1 END) as rainy_readings,
    COUNT(*) as total_readings,
    ROUND((COUNT(CASE WHEN precipitation_inch > 0 THEN 1 END) * 100.0 / COUNT(*))::numeric, 2) as rain_percentage
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY city, state, DATE_TRUNC('day', recorded_at)
HAVING SUM(precipitation_inch) > 0
ORDER BY total_precip DESC;

-- 2.4: Wind Speed Analysis
SELECT 
    city,
    state,
    ROUND(AVG(wind_speed_mph)::numeric, 2) as avg_wind_speed,
    ROUND(MAX(wind_speed_mph)::numeric, 2) as max_wind_speed,
    ROUND(MIN(wind_speed_mph)::numeric, 2) as min_wind_speed,
    CASE 
        WHEN AVG(wind_speed_mph) < 5 THEN 'Calm'
        WHEN AVG(wind_speed_mph) < 15 THEN 'Moderate'
        WHEN AVG(wind_speed_mph) < 25 THEN 'Breezy'
        ELSE 'Windy'
    END as wind_classification,
    COUNT(*) as readings
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '7 days'
GROUP BY city, state
ORDER BY avg_wind_speed DESC;

-- 2.5: Extreme Weather Events Detection
SELECT 
    city,
    state,
    recorded_at,
    temperature_f,
    humidity,
    wind_speed_mph,
    precipitation_inch,
    summary,
    CASE 
        WHEN temperature_f > 100 THEN 'Extreme Heat'
        WHEN temperature_f < 0 THEN 'Extreme Cold'
        WHEN wind_speed_mph > 40 THEN 'High Winds'
        WHEN precipitation_inch > 1.0 THEN 'Heavy Rain'
        ELSE 'Severe Weather'
    END as event_type
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
  AND (
    temperature_f > 100 OR 
    temperature_f < 0 OR 
    wind_speed_mph > 40 OR 
    precipitation_inch > 1.0
  )
ORDER BY recorded_at DESC;

-- ----------------------------------------------------------------------------
-- SECTION 3: FORECAST ACCURACY ANALYSIS
-- ----------------------------------------------------------------------------

-- 3.1: Forecast vs Actual Temperature Comparison
WITH forecast_actual AS (
    SELECT 
        fc.city,
        fc.state,
        fc.forecast_hour,
        fc.temperature_f as forecast_temp,
        cw.temperature_f as actual_temp,
        ABS(fc.temperature_f - cw.temperature_f) as temp_error,
        fc.forecast_summary,
        cw.summary as actual_summary,
        CASE 
            WHEN fc.forecast_summary = cw.summary THEN 'Correct'
            ELSE 'Incorrect'
        END as condition_accuracy
    FROM forecast_comparison fc
    LEFT JOIN current_weather cw 
        ON fc.city = cw.city 
        AND fc.state = cw.state
        AND DATE_TRUNC('hour', fc.forecast_hour) = DATE_TRUNC('hour', cw.recorded_at)
    WHERE fc.recorded_at >= NOW() - INTERVAL '7 days'
)
SELECT 
    city,
    state,
    COUNT(*) as total_forecasts,
    ROUND(AVG(temp_error)::numeric, 2) as avg_temp_error_f,
    ROUND(MAX(temp_error)::numeric, 2) as max_temp_error_f,
    ROUND((COUNT(CASE WHEN condition_accuracy = 'Correct' THEN 1 END) * 100.0 / COUNT(*))::numeric, 2) as condition_accuracy_pct
FROM forecast_actual
WHERE actual_temp IS NOT NULL
GROUP BY city, state
ORDER BY avg_temp_error_f ASC;

-- 3.2: Forecast Accuracy by Time of Day
WITH hourly_accuracy AS (
    SELECT 
        fc.city,
        fc.state,
        EXTRACT(HOUR FROM fc.forecast_hour) as hour_of_day,
        ABS(fc.temperature_f - cw.temperature_f) as temp_error
    FROM forecast_comparison fc
    LEFT JOIN current_weather cw 
        ON fc.city = cw.city 
        AND fc.state = cw.state
        AND DATE_TRUNC('hour', fc.forecast_hour) = DATE_TRUNC('hour', cw.recorded_at)
    WHERE fc.recorded_at >= NOW() - INTERVAL '14 days'
      AND cw.temperature_f IS NOT NULL
)
SELECT 
    hour_of_day,
    COUNT(*) as forecast_count,
    ROUND(AVG(temp_error)::numeric, 2) as avg_error_f,
    ROUND(STDDEV(temp_error)::numeric, 2) as error_stddev
FROM hourly_accuracy
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 3.3: Best and Worst Forecast Performance by City
WITH city_accuracy AS (
    SELECT 
        fc.city,
        fc.state,
        COUNT(*) as forecast_count,
        AVG(ABS(fc.temperature_f - cw.temperature_f)) as avg_error
    FROM forecast_comparison fc
    LEFT JOIN current_weather cw 
        ON fc.city = cw.city 
        AND fc.state = cw.state
        AND DATE_TRUNC('hour', fc.forecast_hour) = DATE_TRUNC('hour', cw.recorded_at)
    WHERE fc.recorded_at >= NOW() - INTERVAL '7 days'
      AND cw.temperature_f IS NOT NULL
    GROUP BY fc.city, fc.state
    HAVING COUNT(*) >= 5
)
(
    SELECT 
        city,
        state,
        ROUND(avg_error::numeric, 2) as avg_error_f,
        forecast_count,
        '🎯 Best' as performance
    FROM city_accuracy
    ORDER BY avg_error ASC
    LIMIT 10
)
UNION ALL
(
    SELECT 
        city,
        state,
        ROUND(avg_error::numeric, 2) as avg_error_f,
        forecast_count,
        '❌ Worst' as performance
    FROM city_accuracy
    ORDER BY avg_error DESC
    LIMIT 10
)
ORDER BY performance, avg_error_f;

-- ----------------------------------------------------------------------------
-- SECTION 4: GEOGRAPHIC COMPARISONS
-- ----------------------------------------------------------------------------

-- 4.1: State-Level Temperature Aggregation
SELECT 
    state,
    COUNT(DISTINCT city) as cities_tracked,
    ROUND(AVG(temperature_f)::numeric, 2) as avg_temp,
    ROUND(MAX(temperature_f)::numeric, 2) as max_temp,
    ROUND(MIN(temperature_f)::numeric, 2) as min_temp,
    ROUND(MAX(temperature_f) - MIN(temperature_f)::numeric, 2) as temp_range,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(wind_speed_mph)::numeric, 2) as avg_wind_speed
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
GROUP BY state
ORDER BY avg_temp DESC;

-- 4.2: Temperature Extremes Across US
SELECT 
    'Hottest' as category,
    city,
    state,
    ROUND(temperature_f::numeric, 2) as temperature_f,
    humidity,
    recorded_at
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
ORDER BY temperature_f DESC
LIMIT 5

UNION ALL

SELECT 
    'Coldest' as category,
    city,
    state,
    ROUND(temperature_f::numeric, 2) as temperature_f,
    humidity,
    recorded_at
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
ORDER BY temperature_f ASC
LIMIT 5;

-- 4.3: Regional Weather Patterns
WITH regional_mapping AS (
    SELECT 
        city,
        state,
        temperature_f,
        humidity,
        wind_speed_mph,
        summary,
        recorded_at,
        CASE 
            WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
            WHEN state IN ('OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
            WHEN state IN ('DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA') THEN 'South'
            WHEN state IN ('MT', 'ID', 'WY', 'NV', 'UT', 'CO', 'AZ', 'NM', 'OK', 'TX') THEN 'Southwest'
            WHEN state IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'West'
        END as region
    FROM current_weather
    WHERE recorded_at >= NOW() - INTERVAL '24 hours'
)
SELECT 
    region,
    COUNT(DISTINCT city || ', ' || state) as locations,
    ROUND(AVG(temperature_f)::numeric, 2) as avg_temp,
    ROUND(MAX(temperature_f)::numeric, 2) as max_temp,
    ROUND(MIN(temperature_f)::numeric, 2) as min_temp,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(wind_speed_mph)::numeric, 2) as avg_wind_speed,
    MODE() WITHIN GROUP (ORDER BY summary) as most_common_weather
FROM regional_mapping
WHERE region IS NOT NULL
GROUP BY region
ORDER BY avg_temp DESC;

-- ----------------------------------------------------------------------------
-- SECTION 5: CORRELATION ANALYSIS
-- ----------------------------------------------------------------------------

-- 5.1: Temperature vs Humidity Correlation
SELECT 
    city,
    state,
    ROUND(CORR(temperature_f, humidity)::numeric, 3) as temp_humidity_correlation,
    COUNT(*) as sample_size
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY city, state
HAVING COUNT(*) >= 20
ORDER BY ABS(CORR(temperature_f, humidity)) DESC;

-- 5.2: Pressure vs Weather Condition Analysis
SELECT 
    summary,
    COUNT(*) as occurrences,
    ROUND(AVG(pressure_hpa)::numeric, 2) as avg_pressure,
    ROUND(STDDEV(pressure_hpa)::numeric, 2) as pressure_stddev,
    ROUND(MIN(pressure_hpa)::numeric, 2) as min_pressure,
    ROUND(MAX(pressure_hpa)::numeric, 2) as max_pressure
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY summary
ORDER BY avg_pressure DESC;

-- ----------------------------------------------------------------------------
-- SECTION 6: TIME-SERIES ANALYSIS
-- ----------------------------------------------------------------------------

-- 6.1: Temperature Change Rate (Degrees per Hour)
WITH temp_changes AS (
    SELECT 
        city,
        state,
        recorded_at,
        temperature_f,
        LAG(temperature_f) OVER (PARTITION BY city, state ORDER BY recorded_at) as prev_temp,
        EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (PARTITION BY city, state ORDER BY recorded_at))) / 3600 as hours_diff
    FROM current_weather
    WHERE recorded_at >= NOW() - INTERVAL '48 hours'
)
SELECT 
    city,
    state,
    recorded_at,
    ROUND(temperature_f::numeric, 2) as current_temp,
    ROUND(prev_temp::numeric, 2) as previous_temp,
    ROUND(((temperature_f - prev_temp) / NULLIF(hours_diff, 0))::numeric, 2) as temp_change_rate_per_hour
FROM temp_changes
WHERE prev_temp IS NOT NULL 
  AND hours_diff > 0
  AND ABS((temperature_f - prev_temp) / NULLIF(hours_diff, 0)) > 2
ORDER BY ABS((temperature_f - prev_temp) / NULLIF(hours_diff, 0)) DESC;

-- 6.2: Moving Average Temperature (7-Day)
SELECT 
    city,
    state,
    DATE_TRUNC('day', recorded_at) as day,
    ROUND(AVG(temperature_f)::numeric, 2) as daily_avg_temp,
    ROUND(AVG(AVG(temperature_f)) OVER (
        PARTITION BY city, state 
        ORDER BY DATE_TRUNC('day', recorded_at) 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )::numeric, 2) as moving_avg_7day
FROM current_weather
WHERE recorded_at >= NOW() - INTERVAL '30 days'
GROUP BY city, state, DATE_TRUNC('day', recorded_at)
ORDER BY city, state, day DESC;

-- ----------------------------------------------------------------------------
-- SECTION 7: SUMMARY STATISTICS
-- ----------------------------------------------------------------------------

-- 7.1: Overall System Health Check
SELECT 
    'Total Cities Tracked' as metric,
    COUNT(DISTINCT city || ', ' || state)::text as value
FROM current_weather
UNION ALL
SELECT 
    'Total Weather Readings',
    COUNT(*)::text
FROM current_weather
UNION ALL
SELECT 
    'Latest Reading Time',
    TO_CHAR(MAX(recorded_at), 'YYYY-MM-DD HH24:MI:SS')
FROM current_weather
UNION ALL
SELECT 
    'Oldest Reading Time',
    TO_CHAR(MIN(recorded_at), 'YYYY-MM-DD HH24:MI:SS')
FROM current_weather
UNION ALL
SELECT 
    'Data Coverage (Days)',
    EXTRACT(DAY FROM (MAX(recorded_at) - MIN(recorded_at)))::text
FROM current_weather
UNION ALL
SELECT 
    'Average Readings Per City',
    ROUND(AVG(reading_count)::numeric, 0)::text
FROM (
    SELECT COUNT(*) as reading_count
    FROM current_weather
    GROUP BY city, state
) sub;

-- 7.2: Data Quality Metrics
SELECT 
    'Null Temperature Values' as check_type,
    COUNT(*) as count
FROM current_weather
WHERE temperature_f IS NULL

UNION ALL

SELECT 
    'Null Humidity Values',
    COUNT(*)
FROM current_weather
WHERE humidity IS NULL

UNION ALL

SELECT 
    'Invalid Humidity Range (>100 or <0)',
    COUNT(*)
FROM current_weather
WHERE humidity > 100 OR humidity < 0

UNION ALL

SELECT 
    'Duplicate Event IDs',
    COUNT(*) - COUNT(DISTINCT event_id)
FROM current_weather;
