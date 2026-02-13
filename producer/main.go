package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	UpdateIntervalMinutes int        `json:"update_interval_minutes"`
	Cities                []CitySpec `json:"cities"`
}

type CitySpec struct {
	City  string `json:"city"`
	State string `json:"state"`
}

type Location struct {
	City  string
	State string
	Lat   float64
	Lon   float64
}

type CurrentWeatherEvent struct {
	EventID      string    `json:"event_id"`
	City         string    `json:"city"`
	State        string    `json:"state"`
	TemperatureF float64   `json:"temperature_f"`
	Humidity     int       `json:"humidity"`
	PressureHpa  float64   `json:"pressure_hpa"`
	WindSpeedMph float64   `json:"wind_speed_mph"`
	PrecipInch   float64   `json:"precipitation_inch"`
	WeatherCode  int       `json:"weather_code"`
	Summary      string    `json:"summary"`
	RecordedAt   time.Time `json:"recorded_at"` // Changed from Timestamp
}

type HourlyForecastEvent struct {
	EventID      string    `json:"event_id"`
	City         string    `json:"city"`
	State        string    `json:"state"`
	ForecastHour time.Time `json:"forecast_hour"`
	TemperatureF float64   `json:"temperature_f"`
	Humidity     int       `json:"humidity"`
	PressureHpa  float64   `json:"pressure_hpa"`
	WindSpeedMph float64   `json:"wind_speed_mph"`
	PrecipInch   float64   `json:"precipitation_inch"`
	WeatherCode  int       `json:"weather_code"`
	Summary      string    `json:"summary"`
	RecordedAt   time.Time `json:"recorded_at"`
}

type GeocodeResponse struct {
	Results []struct {
		Latitude    float64 `json:"latitude"`
		Longitude   float64 `json:"longitude"`
		Name        string  `json:"name"`
		Admin1      string  `json:"admin1"`
		CountryCode string  `json:"country_code"`
	} `json:"results"`
}

type OpenMeteoResponse struct {
	Current struct {
		Time          string  `json:"time"`
		Temperature   float64 `json:"temperature_2m"`
		Humidity      int     `json:"relative_humidity_2m"`
		Pressure      float64 `json:"surface_pressure"`
		WindSpeed     float64 `json:"wind_speed_10m"`
		Precipitation float64 `json:"precipitation"`
		WeatherCode   int     `json:"weather_code"`
	} `json:"current"`
	Hourly struct {
		Time          []string  `json:"time"`
		Temperature   []float64 `json:"temperature_2m"`
		Humidity      []int     `json:"relative_humidity_2m"`
		Pressure      []float64 `json:"surface_pressure"`
		WindSpeed     []float64 `json:"wind_speed_10m"`
		Precipitation []float64 `json:"precipitation"`
		WeatherCode   []int     `json:"weather_code"`
	} `json:"hourly"`
}

var weatherCodeMap = map[int]string{
	0: "Clear", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
	45: "Foggy", 48: "Rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
	55: "Dense drizzle", 61: "Light rain", 63: "Moderate rain", 65: "Heavy rain",
	71: "Light snow", 73: "Moderate snow", 75: "Heavy snow", 77: "Snow grains",
	80: "Light rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
	85: "Light snow showers", 86: "Heavy snow showers", 95: "Thunderstorm",
	96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}

func main() {
	cfg, err := loadConfig("cities.json")
	if err != nil {
		log.Fatalf("loadConfig: %v", err)
	}

	log.Printf("Loaded %d cities", len(cfg.Cities))

	locations, err := geocodeCities(cfg.Cities)
	if err != nil || len(locations) == 0 {
		log.Fatalf("geocodeCities: %v", err)
	}
	log.Printf("Geocoded %d locations", len(locations))

	kCfg := sarama.NewConfig()
	kCfg.Producer.Return.Successes = true
	kCfg.Producer.RequiredAcks = sarama.WaitForAll
	kCfg.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, kCfg)
	if err != nil {
		log.Fatalf("Kafka: %v", err)
	}
	defer producer.Close()

	interval := time.Duration(cfg.UpdateIntervalMinutes) * time.Minute
	if interval <= 0 {
		interval = 30 * time.Minute
	}

	log.Printf("Starting weather producer, interval=%v", interval)

	runBatch(locations, producer)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		runBatch(locations, producer)
	}
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func geocodeCities(cities []CitySpec) ([]Location, error) {
	var locs []Location
	for _, c := range cities {
		loc, err := geocodeOne(c)
		if err != nil {
			log.Printf("geocode failed for %s, %s: %v", c.City, c.State, err)
			continue
		}
		locs = append(locs, *loc)
		time.Sleep(200 * time.Millisecond)
	}
	if len(locs) == 0 {
		return nil, fmt.Errorf("no locations geocoded")
	}
	return locs, nil
}

func geocodeOne(c CitySpec) (*Location, error) {
	base := "https://geocoding-api.open-meteo.com/v1/search"
	params := url.Values{}
	params.Set("name", c.City)
	params.Set("count", "5")
	params.Set("language", "en")
	params.Set("format", "json")
	params.Set("country_code", "US")

	u := fmt.Sprintf("%s?%s", base, params.Encode())

	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var geo GeocodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&geo); err != nil {
		return nil, err
	}
	if len(geo.Results) == 0 {
		return nil, fmt.Errorf("no results")
	}

	r := geo.Results[0]
	return &Location{
		City:  c.City,
		State: c.State,
		Lat:   r.Latitude,
		Lon:   r.Longitude,
	}, nil
}

func runBatch(locs []Location, producer sarama.SyncProducer) {
	start := time.Now()
	ok := 0
	fail := 0

	log.Printf("%s", "\n"+strings.Repeat("=", 80))
	log.Printf("🚀 Starting batch at %s", start.Format("2006-01-02 15:04:05 MST"))
	log.Printf("%s", strings.Repeat("=", 80)+"\n")

	for _, loc := range locs {
		log.Printf("\n--- Processing: %s, %s ---", loc.City, loc.State)

		current, hourly, err := fetchWeather(loc)
		if err != nil {
			log.Printf("❌ weather error %s, %s: %v\n", loc.City, loc.State, err)
			fail++
			time.Sleep(300 * time.Millisecond)
			continue
		}

		if err := publishCurrent(producer, current); err != nil {
			log.Printf("❌ kafka error (current) %s, %s: %v\n", loc.City, loc.State, err)
			fail++
			continue
		}
		log.Printf("✅ Published current weather to Kafka")

		for i, h := range hourly {
			if err := publishHourly(producer, h); err != nil {
				log.Printf("❌ kafka error (hourly[%d]) %s, %s: %v", i, loc.City, loc.State, err)
			} else {
				log.Printf("✅ Published hourly[%d]: %s | %.1f°F", i+1, h.ForecastHour.Format("15:04"), h.TemperatureF)
			}
		}

		ok++
		log.Printf("✅ SUCCESS: %s, %s | Current: %.1f°F | Hourly forecasts: %d\n",
			current.City, current.State, current.TemperatureF, len(hourly))

		time.Sleep(300 * time.Millisecond)
	}

	log.Printf("%s", "\n"+strings.Repeat("=", 80))
	log.Printf("🏁 Batch complete in %v (success=%d, failed=%d)",
		time.Since(start).Round(time.Second), ok, fail)
	log.Printf("%s", strings.Repeat("=", 80)+"\n")
}

func fetchWeather(loc Location) (*CurrentWeatherEvent, []*HourlyForecastEvent, error) {
	base := "https://api.open-meteo.com/v1/forecast"
	params := url.Values{}
	params.Set("latitude", fmt.Sprintf("%.4f", loc.Lat))
	params.Set("longitude", fmt.Sprintf("%.4f", loc.Lon))
	params.Set("current", "temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,precipitation,weather_code")
	params.Set("hourly", "temperature_2m,relative_humidity_2m,surface_pressure,wind_speed_10m,precipitation,weather_code")
	params.Set("temperature_unit", "fahrenheit")
	params.Set("wind_speed_unit", "mph")
	params.Set("precipitation_unit", "inch")
	params.Set("timezone", "auto")
	params.Set("forecast_days", "1")

	u := fmt.Sprintf("%s?%s", base, params.Encode())

	resp, err := http.Get(u)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var om OpenMeteoResponse
	if err := json.NewDecoder(resp.Body).Decode(&om); err != nil {
		return nil, nil, err
	}

	// Current time when we make the request
	now := time.Now()
	log.Printf("🕐 Current time: %s", now.Format("2006-01-02 15:04:05 MST"))

	eventID := fmt.Sprintf("evt-%d-%s-%s", now.UnixNano(), loc.City, loc.State)

	// Parse current weather time from API
	currentTime, _ := time.Parse("2006-01-02T15:04", om.Current.Time)
	summary := weatherCodeMap[om.Current.WeatherCode]
	if summary == "" {
		summary = "Unknown"
	}

	// Store current conditions
	current := &CurrentWeatherEvent{
		EventID:      eventID,
		City:         loc.City,
		State:        loc.State,
		TemperatureF: om.Current.Temperature,
		Humidity:     om.Current.Humidity,
		PressureHpa:  om.Current.Pressure,
		WindSpeedMph: om.Current.WindSpeed,
		PrecipInch:   om.Current.Precipitation,
		WeatherCode:  om.Current.WeatherCode,
		Summary:      summary,
		RecordedAt:   currentTime,
	}

	log.Printf("📍 %s, %s - Current: %.1f°F at %s",
		loc.City, loc.State, current.TemperatureF, currentTime.Format("15:04 MST"))

	// Calculate next 5 full hours
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	targetHours := make([]time.Time, 5)
	for i := 0; i < 5; i++ {
		targetHours[i] = nextHour.Add(time.Duration(i) * time.Hour)
	}

	log.Printf("🎯 Target forecast hours:")
	for i, t := range targetHours {
		log.Printf("   [%d] %s", i+1, t.Format("15:04 MST"))
	}

	log.Printf("📊 API returned %d hourly data points", len(om.Hourly.Time))
	if len(om.Hourly.Time) > 0 {
		log.Printf("   First: %s", om.Hourly.Time[0])
		log.Printf("   Last:  %s", om.Hourly.Time[len(om.Hourly.Time)-1])
	}

	var hourlyEvents []*HourlyForecastEvent

	// Match API hourly data to our target hours
	for idx, targetHour := range targetHours {
		found := false
		for i := 0; i < len(om.Hourly.Time); i++ {
			hourTime, err := time.Parse("2006-01-02T15:04", om.Hourly.Time[i])
			if err != nil {
				log.Printf("⚠️  Failed to parse hourly time: %s", om.Hourly.Time[i])
				continue
			}

			// Match by hour (ignore minutes)
			if hourTime.Hour() == targetHour.Hour() && hourTime.Day() == targetHour.Day() {
				hSummary := weatherCodeMap[om.Hourly.WeatherCode[i]]
				if hSummary == "" {
					hSummary = "Unknown"
				}

				hourlyEvents = append(hourlyEvents, &HourlyForecastEvent{
					EventID:      eventID,
					City:         loc.City,
					State:        loc.State,
					ForecastHour: hourTime,
					TemperatureF: om.Hourly.Temperature[i],
					Humidity:     om.Hourly.Humidity[i],
					PressureHpa:  om.Hourly.Pressure[i],
					WindSpeedMph: om.Hourly.WindSpeed[i],
					PrecipInch:   om.Hourly.Precipitation[i],
					WeatherCode:  om.Hourly.WeatherCode[i],
					Summary:      hSummary,
					RecordedAt:   now,
				})

				log.Printf("   ✅ Matched target[%d] %s → API data at %s (%.1f°F, %s)",
					idx+1, targetHour.Format("15:04"), hourTime.Format("15:04"),
					om.Hourly.Temperature[i], hSummary)

				found = true
				break
			}
		}

		if !found {
			log.Printf("   ❌ No match found for target[%d] %s", idx+1, targetHour.Format("15:04"))
		}
	}

	log.Printf("📦 Created %d hourly forecast events for %s, %s", len(hourlyEvents), loc.City, loc.State)

	return current, hourlyEvents, nil
}

func publishCurrent(p sarama.SyncProducer, ev *CurrentWeatherEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s,%s", ev.City, ev.State)

	msg := &sarama.ProducerMessage{
		Topic: "weather.current",
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = p.SendMessage(msg)
	return err
}

func publishHourly(p sarama.SyncProducer, ev *HourlyForecastEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s,%s", ev.City, ev.State)

	msg := &sarama.ProducerMessage{
		Topic: "weather.hourly",
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(b),
	}

	_, _, err = p.SendMessage(msg)
	return err
}
