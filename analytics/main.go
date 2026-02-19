package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

// ============================================================================
// DATA MODELS
// ============================================================================

type TemperatureTrend struct {
	City         string    `json:"city"`
	State        string    `json:"state"`
	Timestamp    time.Time `json:"timestamp"`
	AvgTemp      float64   `json:"avg_temp"`
	MaxTemp      float64   `json:"max_temp"`
	MinTemp      float64   `json:"min_temp"`
	TempVariance float64   `json:"temp_variance"`
	SampleCount  int       `json:"sample_count"`
}

type WeeklyComparison struct {
	City              string  `json:"city"`
	State             string  `json:"state"`
	CurrentWeekAvg    float64 `json:"current_week_avg"`
	LastWeekAvg       float64 `json:"last_week_avg"`
	TempChange        float64 `json:"temp_change"`
	PercentChange     float64 `json:"percent_change"`
	Trend             string  `json:"trend"`
}

type WeatherPattern struct {
	City            string  `json:"city"`
	State           string  `json:"state"`
	Summary         string  `json:"summary"`
	OccurrenceCount int     `json:"occurrence_count"`
	Percentage      float64 `json:"percentage"`
	AvgTempDuring   float64 `json:"avg_temp_during"`
	AvgHumidity     float64 `json:"avg_humidity"`
}

type ForecastAccuracy struct {
	City                string  `json:"city"`
	State               string  `json:"state"`
	TotalForecasts      int     `json:"total_forecasts"`
	AvgTempError        float64 `json:"avg_temp_error_f"`
	MaxTempError        float64 `json:"max_temp_error_f"`
	ConditionAccuracyPct float64 `json:"condition_accuracy_pct"`
}

type RegionalStats struct {
	Region            string  `json:"region"`
	Locations         int     `json:"locations"`
	AvgTemp           float64 `json:"avg_temp"`
	MaxTemp           float64 `json:"max_temp"`
	MinTemp           float64 `json:"min_temp"`
	AvgHumidity       float64 `json:"avg_humidity"`
	AvgWindSpeed      float64 `json:"avg_wind_speed"`
	MostCommonWeather string  `json:"most_common_weather"`
}

type ExtremeWeather struct {
	City            string    `json:"city"`
	State           string    `json:"state"`
	RecordedAt      time.Time `json:"recorded_at"`
	TemperatureF    float64   `json:"temperature_f"`
	Humidity        int       `json:"humidity"`
	WindSpeedMph    float64   `json:"wind_speed_mph"`
	PrecipitationIn float64   `json:"precipitation_inch"`
	Summary         string    `json:"summary"`
	EventType       string    `json:"event_type"`
}

type TemperatureChangeRate struct {
	City                string    `json:"city"`
	State               string    `json:"state"`
	RecordedAt          time.Time `json:"recorded_at"`
	CurrentTemp         float64   `json:"current_temp"`
	PreviousTemp        float64   `json:"previous_temp"`
	TempChangeRatePerHr float64   `json:"temp_change_rate_per_hour"`
}

// ============================================================================
// ANALYTICS SERVICE
// ============================================================================

type AnalyticsService struct {
	db *sql.DB
}

func NewAnalyticsService(dsn string) (*AnalyticsService, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	log.Println("✅ Connected to PostgreSQL for analytics")
	return &AnalyticsService{db: db}, nil
}

func (s *AnalyticsService) Close() error {
	return s.db.Close()
}

// ============================================================================
// TEMPERATURE TREND ANALYSIS
// ============================================================================

func (s *AnalyticsService) GetHourlyTemperatureTrends(hours int) ([]TemperatureTrend, error) {
	query := `
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
		WHERE recorded_at >= NOW() - INTERVAL '1 hour' * $1
		GROUP BY city, state, DATE_TRUNC('hour', recorded_at)
		ORDER BY city, state, hour DESC
	`

	rows, err := s.db.Query(query, hours)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var trends []TemperatureTrend
	for rows.Next() {
		var t TemperatureTrend
		var variance sql.NullFloat64

		err := rows.Scan(
			&t.City, &t.State, &t.Timestamp,
			&t.AvgTemp, &t.MaxTemp, &t.MinTemp,
			&variance, &t.SampleCount,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		if variance.Valid {
			t.TempVariance = variance.Float64
		}

		trends = append(trends, t)
	}

	return trends, rows.Err()
}

func (s *AnalyticsService) GetWeeklyComparison() ([]WeeklyComparison, error) {
	query := `
		WITH current_week AS (
			SELECT 
				city,
				state,
				AVG(temperature_f) as avg_temp_current
			FROM current_weather
			WHERE recorded_at >= DATE_TRUNC('week', NOW())
			GROUP BY city, state
		),
		last_week AS (
			SELECT 
				city,
				state,
				AVG(temperature_f) as avg_temp_last
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
		ORDER BY ABS(cw.avg_temp_current - lw.avg_temp_last) DESC
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var comparisons []WeeklyComparison
	for rows.Next() {
		var wc WeeklyComparison
		err := rows.Scan(
			&wc.City, &wc.State,
			&wc.CurrentWeekAvg, &wc.LastWeekAvg,
			&wc.TempChange, &wc.PercentChange,
			&wc.Trend,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		comparisons = append(comparisons, wc)
	}

	return comparisons, rows.Err()
}

// ============================================================================
// WEATHER PATTERN ANALYSIS
// ============================================================================

func (s *AnalyticsService) GetWeatherPatternDistribution(days int) ([]WeatherPattern, error) {
	query := `
		SELECT 
			city,
			state,
			summary,
			COUNT(*) as occurrence_count,
			ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city, state))::numeric, 2) as percentage,
			AVG(temperature_f) as avg_temp_during,
			AVG(humidity) as avg_humidity
		FROM current_weather
		WHERE recorded_at >= NOW() - INTERVAL '1 day' * $1
		GROUP BY city, state, summary
		ORDER BY city, state, occurrence_count DESC
	`

	rows, err := s.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var patterns []WeatherPattern
	for rows.Next() {
		var wp WeatherPattern
		err := rows.Scan(
			&wp.City, &wp.State, &wp.Summary,
			&wp.OccurrenceCount, &wp.Percentage,
			&wp.AvgTempDuring, &wp.AvgHumidity,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		patterns = append(patterns, wp)
	}

	return patterns, rows.Err()
}

func (s *AnalyticsService) GetExtremeWeatherEvents(days int) ([]ExtremeWeather, error) {
	query := `
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
		WHERE recorded_at >= NOW() - INTERVAL '1 day' * $1
		  AND (
			temperature_f > 100 OR 
			temperature_f < 0 OR 
			wind_speed_mph > 40 OR 
			precipitation_inch > 1.0
		  )
		ORDER BY recorded_at DESC
	`

	rows, err := s.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var events []ExtremeWeather
	for rows.Next() {
		var ew ExtremeWeather
		err := rows.Scan(
			&ew.City, &ew.State, &ew.RecordedAt,
			&ew.TemperatureF, &ew.Humidity,
			&ew.WindSpeedMph, &ew.PrecipitationIn,
			&ew.Summary, &ew.EventType,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		events = append(events, ew)
	}

	return events, rows.Err()
}

// ============================================================================
// FORECAST ACCURACY ANALYSIS
// ============================================================================

func (s *AnalyticsService) GetForecastAccuracy(days int) ([]ForecastAccuracy, error) {
	query := `
		WITH forecast_actual AS (
			SELECT 
				fc.city,
				fc.state,
				ABS(fc.temperature_f - cw.temperature_f) as temp_error,
				CASE 
					WHEN fc.forecast_summary = cw.summary THEN 1
					ELSE 0
				END as condition_correct
			FROM forecast_comparison fc
			LEFT JOIN current_weather cw 
				ON fc.city = cw.city 
				AND fc.state = cw.state
				AND DATE_TRUNC('hour', fc.forecast_hour) = DATE_TRUNC('hour', cw.recorded_at)
			WHERE fc.recorded_at >= NOW() - INTERVAL '1 day' * $1
			  AND cw.temperature_f IS NOT NULL
		)
		SELECT 
			city,
			state,
			COUNT(*) as total_forecasts,
			ROUND(AVG(temp_error)::numeric, 2) as avg_temp_error_f,
			ROUND(MAX(temp_error)::numeric, 2) as max_temp_error_f,
			ROUND((SUM(condition_correct) * 100.0 / COUNT(*))::numeric, 2) as condition_accuracy_pct
		FROM forecast_actual
		GROUP BY city, state
		HAVING COUNT(*) >= 3
		ORDER BY avg_temp_error_f ASC
	`

	rows, err := s.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var accuracies []ForecastAccuracy
	for rows.Next() {
		var fa ForecastAccuracy
		err := rows.Scan(
			&fa.City, &fa.State,
			&fa.TotalForecasts,
			&fa.AvgTempError, &fa.MaxTempError,
			&fa.ConditionAccuracyPct,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		accuracies = append(accuracies, fa)
	}

	return accuracies, rows.Err()
}

// ============================================================================
// GEOGRAPHIC ANALYSIS
// ============================================================================

func (s *AnalyticsService) GetRegionalStats() ([]RegionalStats, error) {
	query := `
		WITH regional_mapping AS (
			SELECT 
				temperature_f,
				humidity,
				wind_speed_mph,
				summary,
				CASE 
					WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
					WHEN state IN ('OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
					WHEN state IN ('DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA') THEN 'South'
					WHEN state IN ('MT', 'ID', 'WY', 'NV', 'UT', 'CO', 'AZ', 'NM', 'OK', 'TX') THEN 'Southwest'
					WHEN state IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'West'
				END as region,
				city || ', ' || state as location
			FROM current_weather
			WHERE recorded_at >= NOW() - INTERVAL '24 hours'
		)
		SELECT 
			region,
			COUNT(DISTINCT location) as locations,
			ROUND(AVG(temperature_f)::numeric, 2) as avg_temp,
			ROUND(MAX(temperature_f)::numeric, 2) as max_temp,
			ROUND(MIN(temperature_f)::numeric, 2) as min_temp,
			ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
			ROUND(AVG(wind_speed_mph)::numeric, 2) as avg_wind_speed,
			MODE() WITHIN GROUP (ORDER BY summary) as most_common_weather
		FROM regional_mapping
		WHERE region IS NOT NULL
		GROUP BY region
		ORDER BY avg_temp DESC
	`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var stats []RegionalStats
	for rows.Next() {
		var rs RegionalStats
		err := rows.Scan(
			&rs.Region, &rs.Locations,
			&rs.AvgTemp, &rs.MaxTemp, &rs.MinTemp,
			&rs.AvgHumidity, &rs.AvgWindSpeed,
			&rs.MostCommonWeather,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		stats = append(stats, rs)
	}

	return stats, rows.Err()
}

// ============================================================================
// TIME-SERIES ANALYSIS
// ============================================================================

func (s *AnalyticsService) GetTemperatureChangeRates(hours int, minRatePerHour float64) ([]TemperatureChangeRate, error) {
	query := `
		WITH temp_changes AS (
			SELECT 
				city,
				state,
				recorded_at,
				temperature_f,
				LAG(temperature_f) OVER (PARTITION BY city, state ORDER BY recorded_at) as prev_temp,
				EXTRACT(EPOCH FROM (recorded_at - LAG(recorded_at) OVER (PARTITION BY city, state ORDER BY recorded_at))) / 3600 as hours_diff
			FROM current_weather
			WHERE recorded_at >= NOW() - INTERVAL '1 hour' * $1
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
		  AND ABS((temperature_f - prev_temp) / NULLIF(hours_diff, 0)) >= $2
		ORDER BY ABS((temperature_f - prev_temp) / NULLIF(hours_diff, 0)) DESC
	`

	rows, err := s.db.Query(query, hours, minRatePerHour)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var rates []TemperatureChangeRate
	for rows.Next() {
		var tcr TemperatureChangeRate
		err := rows.Scan(
			&tcr.City, &tcr.State, &tcr.RecordedAt,
			&tcr.CurrentTemp, &tcr.PreviousTemp,
			&tcr.TempChangeRatePerHr,
		)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		rates = append(rates, tcr)
	}

	return rates, rows.Err()
}

// ============================================================================
// EXPORT UTILITIES
// ============================================================================

func ExportToJSON(data interface{}, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	log.Printf("✅ Exported to %s", filename)
	return nil
}

func ExportTemperatureTrendsToCSV(trends []TemperatureTrend, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"City", "State", "Timestamp", "AvgTemp", "MaxTemp", "MinTemp", "TempVariance", "SampleCount"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data
	for _, t := range trends {
		record := []string{
			t.City,
			t.State,
			t.Timestamp.Format("2006-01-02 15:04:05"),
			fmt.Sprintf("%.2f", t.AvgTemp),
			fmt.Sprintf("%.2f", t.MaxTemp),
			fmt.Sprintf("%.2f", t.MinTemp),
			fmt.Sprintf("%.2f", t.TempVariance),
			fmt.Sprintf("%d", t.SampleCount),
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	log.Printf("✅ Exported to %s", filename)
	return nil
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

func main() {
	dsn := "postgres://weather_user:weather_pass@localhost:5433/weather_db?sslmode=disable"
	
	service, err := NewAnalyticsService(dsn)
	if err != nil {
		log.Fatalf("Failed to initialize analytics service: %v", err)
	}
	defer service.Close()

	log.Println("\n" + "="*80)
	log.Println("🔬 WEATHER DATA ANALYTICS")
	log.Println("="*80 + "\n")

	// 1. Hourly Temperature Trends
	log.Println("📊 Analyzing hourly temperature trends (last 24 hours)...")
	trends, err := service.GetHourlyTemperatureTrends(24)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found %d hourly data points", len(trends))
		if err := ExportToJSON(trends, "hourly_trends.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
		if err := ExportTemperatureTrendsToCSV(trends, "hourly_trends.csv"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 2. Weekly Comparison
	log.Println("\n📈 Comparing current week vs last week...")
	weeklyComp, err := service.GetWeeklyComparison()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found %d cities with weekly data", len(weeklyComp))
		for i, wc := range weeklyComp {
			if i < 10 { // Show top 10
				log.Printf("  %s, %s: %.2f°F → %.2f°F (%s %.2f°F, %.2f%%)",
					wc.City, wc.State,
					wc.LastWeekAvg, wc.CurrentWeekAvg,
					wc.Trend, wc.TempChange, wc.PercentChange)
			}
		}
		if err := ExportToJSON(weeklyComp, "weekly_comparison.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 3. Weather Pattern Distribution
	log.Println("\n🌤️  Analyzing weather pattern distribution (last 30 days)...")
	patterns, err := service.GetWeatherPatternDistribution(30)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found %d weather patterns", len(patterns))
		if err := ExportToJSON(patterns, "weather_patterns.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 4. Extreme Weather Events
	log.Println("\n⚠️  Detecting extreme weather events (last 30 days)...")
	extremes, err := service.GetExtremeWeatherEvents(30)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found %d extreme weather events", len(extremes))
		for _, e := range extremes {
			log.Printf("  [%s] %s, %s: %.2f°F - %s at %s",
				e.EventType, e.City, e.State,
				e.TemperatureF, e.Summary,
				e.RecordedAt.Format("2006-01-02 15:04"))
		}
		if err := ExportToJSON(extremes, "extreme_events.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 5. Forecast Accuracy
	log.Println("\n🎯 Calculating forecast accuracy (last 7 days)...")
	accuracy, err := service.GetForecastAccuracy(7)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Analyzed %d cities for forecast accuracy", len(accuracy))
		for i, fa := range accuracy {
			if i < 10 { // Show top 10 most accurate
				log.Printf("  %s, %s: Avg error %.2f°F, Condition accuracy %.2f%% (%d forecasts)",
					fa.City, fa.State, fa.AvgTempError,
					fa.ConditionAccuracyPct, fa.TotalForecasts)
			}
		}
		if err := ExportToJSON(accuracy, "forecast_accuracy.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 6. Regional Statistics
	log.Println("\n🗺️  Computing regional statistics...")
	regional, err := service.GetRegionalStats()
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found data for %d regions", len(regional))
		for _, r := range regional {
			log.Printf("  %s: %.2f°F avg, %d locations, %s (most common)",
				r.Region, r.AvgTemp, r.Locations, r.MostCommonWeather)
		}
		if err := ExportToJSON(regional, "regional_stats.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	// 7. Temperature Change Rates
	log.Println("\n🌡️  Detecting rapid temperature changes (>2°F/hour, last 48 hours)...")
	changeRates, err := service.GetTemperatureChangeRates(48, 2.0)
	if err != nil {
		log.Printf("Error: %v", err)
	} else {
		log.Printf("Found %d rapid temperature changes", len(changeRates))
		for i, cr := range changeRates {
			if i < 10 { // Show top 10
				log.Printf("  %s, %s: %.2f°F → %.2f°F (%.2f°F/hr) at %s",
					cr.City, cr.State,
					cr.PreviousTemp, cr.CurrentTemp,
					cr.TempChangeRatePerHr,
					cr.RecordedAt.Format("2006-01-02 15:04"))
			}
		}
		if err := ExportToJSON(changeRates, "temperature_change_rates.json"); err != nil {
			log.Printf("Export error: %v", err)
		}
	}

	log.Println("\n" + "="*80)
	log.Println("✅ Analytics complete! Check output files:")
	log.Println("   - hourly_trends.json / .csv")
	log.Println("   - weekly_comparison.json")
	log.Println("   - weather_patterns.json")
	log.Println("   - extreme_events.json")
	log.Println("   - forecast_accuracy.json")
	log.Println("   - regional_stats.json")
	log.Println("   - temperature_change_rates.json")
	log.Println("="*80 + "\n")
}
