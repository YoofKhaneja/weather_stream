package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
)

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
	RecordedAt   time.Time `json:"recorded_at"`
}

type ForecastComparisonEvent struct {
	EventID             string    `json:"event_id"`
	City                string    `json:"city"`
	State               string    `json:"state"`
	CurrentTime         time.Time `json:"current_time"`
	CurrentWeatherCode  int       `json:"current_weather_code"`
	CurrentSummary      string    `json:"current_summary"`
	ForecastHour        time.Time `json:"forecast_hour"`
	ForecastWeatherCode int       `json:"forecast_weather_code"`
	ForecastSummary     string    `json:"forecast_summary"`
	TemperatureF        float64   `json:"temperature_f"`
	Humidity            int       `json:"humidity"`
	PressureHpa         float64   `json:"pressure_hpa"`
	WindSpeedMph        float64   `json:"wind_speed_mph"`
	PrecipInch          float64   `json:"precipitation_inch"`
	Message             string    `json:"message"`
	RecordedAt          time.Time `json:"recorded_at"`
}

type Consumer struct {
	ready chan bool
	db    *sql.DB
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				return nil
			}

			log.Printf("📩 topic=%s partition=%d offset=%d",
				msg.Topic, msg.Partition, msg.Offset)

			switch msg.Topic {
			case "weather.current":
				var ev CurrentWeatherEvent
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					log.Printf("❌ unmarshal error (current): %v | raw: %s", err, string(msg.Value))
					sess.MarkMessage(msg, "")
					continue
				}

				log.Printf("📦 current: %s, %s | %.1f°F | %s | at %s",
					ev.City, ev.State, ev.TemperatureF, ev.Summary, ev.RecordedAt.Format("15:04"))

				if err := c.storeCurrent(&ev); err != nil {
					log.Printf("❌ DB error (current) %s, %s: %v", ev.City, ev.State, err)
				} else {
					log.Printf("✅ stored current: %s, %s", ev.City, ev.State)
				}

			case "weather.forecast_comparison":
				var ev ForecastComparisonEvent
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					log.Printf("❌ unmarshal error (forecast): %v | raw: %s", err, string(msg.Value))
					sess.MarkMessage(msg, "")
					continue
				}

				log.Printf("📦 forecast: %s, %s | %s | %s",
					ev.City, ev.State, ev.ForecastHour.Format("15:04"), ev.Message)

				if err := c.storeForecastComparison(&ev); err != nil {
					log.Printf("❌ DB error (forecast) %s, %s: %v", ev.City, ev.State, err)
				} else {
					log.Printf("✅ stored forecast: %s, %s", ev.City, ev.State)
				}

			default:
				log.Printf("⚠️  Unknown topic: %s", msg.Topic)
			}

			sess.MarkMessage(msg, "")

		case <-sess.Context().Done():
			return nil
		}
	}
}

func (c *Consumer) storeCurrent(ev *CurrentWeatherEvent) error {
	const q = `
        INSERT INTO current_weather (
            event_id, city, state, temperature_f, humidity,
            pressure_hpa, wind_speed_mph, precipitation_inch,
            weather_code, summary, recorded_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (event_id) DO UPDATE SET
            temperature_f = EXCLUDED.temperature_f,
            humidity = EXCLUDED.humidity,
            pressure_hpa = EXCLUDED.pressure_hpa,
            wind_speed_mph = EXCLUDED.wind_speed_mph,
            precipitation_inch = EXCLUDED.precipitation_inch,
            weather_code = EXCLUDED.weather_code,
            summary = EXCLUDED.summary,
            recorded_at = EXCLUDED.recorded_at;
    `
	_, err := c.db.Exec(q,
		ev.EventID, ev.City, ev.State, ev.TemperatureF, ev.Humidity,
		ev.PressureHpa, ev.WindSpeedMph, ev.PrecipInch,
		ev.WeatherCode, ev.Summary, ev.RecordedAt,
	)
	return err
}

func (c *Consumer) storeForecastComparison(ev *ForecastComparisonEvent) error {
	const q = `
        INSERT INTO forecast_comparison (
            event_id, city, state,
            "current_time", "current_weather_code", current_summary,
            "forecast_hour", "forecast_weather_code", forecast_summary,
            temperature_f, humidity, pressure_hpa, wind_speed_mph, precipitation_inch,
            message, recorded_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        ON CONFLICT (city, state, "current_time", "forecast_hour") DO UPDATE SET
            "current_weather_code" = EXCLUDED."current_weather_code",
            current_summary = EXCLUDED.current_summary,
            "forecast_weather_code" = EXCLUDED."forecast_weather_code",
            forecast_summary = EXCLUDED.forecast_summary,
            temperature_f = EXCLUDED.temperature_f,
            humidity = EXCLUDED.humidity,
            pressure_hpa = EXCLUDED.pressure_hpa,
            wind_speed_mph = EXCLUDED.wind_speed_mph,
            precipitation_inch = EXCLUDED.precipitation_inch,
            message = EXCLUDED.message,
            recorded_at = EXCLUDED.recorded_at;
    `
	_, err := c.db.Exec(q,
		ev.EventID, ev.City, ev.State,
		ev.CurrentTime, ev.CurrentWeatherCode, ev.CurrentSummary,
		ev.ForecastHour, ev.ForecastWeatherCode, ev.ForecastSummary,
		ev.TemperatureF, ev.Humidity, ev.PressureHpa, ev.WindSpeedMph, ev.PrecipInch,
		ev.Message, ev.RecordedAt,
	)
	return err
}

func main() {
	dsn := "postgres://weather_user:weather_pass@localhost:5433/weather_db?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	log.Println("✅ Connected to Postgres")

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Return.Errors = true

	groupID := "weather-consumer-group"
	brokers := []string{"localhost:9092"}
	topics := []string{"weather.current", "weather.forecast_comparison"}

	client, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatalf("consumer group: %v", err)
	}
	defer client.Close()

	consumer := &Consumer{
		ready: make(chan bool),
		db:    db,
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			log.Println("🔄 Starting consume cycle...")
			if err := client.Consume(ctx, topics, consumer); err != nil {
				log.Printf("❌ consume error: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Println("✅ Consumer up and running, topics:", topics)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("🛑 Shutdown signal: %v", sig)
	case <-ctx.Done():
	}

	cancel()
	wg.Wait()
	log.Println("✅ Consumer shutdown complete")
}
