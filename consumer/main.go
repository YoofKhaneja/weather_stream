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

			log.Printf("📩 Received from topic=%s partition=%d offset=%d",
				msg.Topic, msg.Partition, msg.Offset)

			switch msg.Topic {
			case "weather.current":
				var ev CurrentWeatherEvent
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					log.Printf("❌ unmarshal error (current): %v | raw: %s", err, string(msg.Value))
					sess.MarkMessage(msg, "")
					continue
				}

				log.Printf("📦 Parsed current: %s, %s | %.1f°F | %s | time=%s",
					ev.City, ev.State, ev.TemperatureF, ev.Summary, ev.RecordedAt.Format("15:04"))

				if err := c.storeCurrent(&ev); err != nil {
					log.Printf("❌ DB error (current) %s, %s: %v", ev.City, ev.State, err)
				} else {
					log.Printf("✅ STORED current: %s, %s | %.1f°F at %s",
						ev.City, ev.State, ev.TemperatureF, ev.RecordedAt.Format("15:04"))
				}

			case "weather.hourly":
				var ev HourlyForecastEvent
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					log.Printf("❌ unmarshal error (hourly): %v | raw: %s", err, string(msg.Value))
					sess.MarkMessage(msg, "")
					continue
				}

				log.Printf("📦 Parsed hourly: %s, %s | forecast_hour=%s | %.1f°F",
					ev.City, ev.State, ev.ForecastHour.Format("15:04"), ev.TemperatureF)

				if err := c.storeHourly(&ev); err != nil {
					log.Printf("❌ DB error (hourly) %s, %s: %v", ev.City, ev.State, err)
				} else {
					log.Printf("✅ STORED hourly: %s, %s | forecast=%s | %.1f°F",
						ev.City, ev.State, ev.ForecastHour.Format("15:04 MST"), ev.TemperatureF)
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
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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

	result, err := c.db.Exec(q,
		ev.EventID, ev.City, ev.State, ev.TemperatureF, ev.Humidity,
		ev.PressureHpa, ev.WindSpeedMph, ev.PrecipInch,
		ev.WeatherCode, ev.Summary, ev.RecordedAt,
	)

	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	log.Printf("💾 current_weather insert affected %d rows", rows)

	return nil
}

func (c *Consumer) storeHourly(ev *HourlyForecastEvent) error {
	const q = `
		INSERT INTO hourly_forecast (
			event_id, city, state, forecast_hour, temperature_f,
			humidity, pressure_hpa, wind_speed_mph, precipitation_inch,
			weather_code, summary, recorded_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (city, state, forecast_hour, recorded_at) DO UPDATE SET
			temperature_f = EXCLUDED.temperature_f,
			humidity = EXCLUDED.humidity,
			pressure_hpa = EXCLUDED.pressure_hpa,
			wind_speed_mph = EXCLUDED.wind_speed_mph,
			precipitation_inch = EXCLUDED.precipitation_inch,
			weather_code = EXCLUDED.weather_code,
			summary = EXCLUDED.summary;
	`

	result, err := c.db.Exec(q,
		ev.EventID, ev.City, ev.State, ev.ForecastHour, ev.TemperatureF,
		ev.Humidity, ev.PressureHpa, ev.WindSpeedMph, ev.PrecipInch,
		ev.WeatherCode, ev.Summary, ev.RecordedAt,
	)

	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	log.Printf("💾 hourly_forecast insert affected %d rows", rows)

	return nil
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

	// Test query to verify tables exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM current_weather").Scan(&count)
	if err != nil {
		log.Printf("⚠️  Warning: current_weather table check failed: %v", err)
	} else {
		log.Printf("📊 current_weather has %d rows", count)
	}

	err = db.QueryRow("SELECT COUNT(*) FROM hourly_forecast").Scan(&count)
	if err != nil {
		log.Printf("⚠️  Warning: hourly_forecast table check failed: %v", err)
	} else {
		log.Printf("📊 hourly_forecast has %d rows", count)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(10 * time.Minute)

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_3_0_0
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest // Changed to read from beginning
	cfg.Consumer.Return.Errors = true

	groupID := "weather-consumer-group"
	brokers := []string{"localhost:9092"}
	topics := []string{"weather.current", "weather.hourly"}

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
	log.Println("✅ Consumer up and running, listening to topics:", topics)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("🛑 Shutdown signal received: %v", sig)
	case <-ctx.Done():
	}

	cancel()
	wg.Wait()
	log.Println("✅ Consumer shutdown complete")
}
