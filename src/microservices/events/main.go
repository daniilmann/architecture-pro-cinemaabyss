package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type Config struct {
	Port         string
	KafkaBrokers []string
}

type Event struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

type eventResponse struct {
	Status    string `json:"status"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type Server struct {
	cfg Config
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := NewServer(cfg)
	server.startConsumers(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", server.handleHealth)
	mux.HandleFunc("/api/events/movie", server.handleEvent(movieTopic, "movie"))
	mux.HandleFunc("/api/events/user", server.handleEvent(userTopic, "user"))
	mux.HandleFunc("/api/events/payment", server.handleEvent(paymentTopic, "payment"))

	httpServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("Starting events microservice on port %s", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("events microservice failed: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("events microservice shutdown failed: %v", err)
	}
}

func loadConfig() Config {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8082"
	}

	brokersRaw := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersRaw == "" {
		brokersRaw = "localhost:9092"
	}

	brokers := make([]string, 0)
	for _, broker := range strings.Split(brokersRaw, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}

	return Config{
		Port:         port,
		KafkaBrokers: brokers,
	}
}

func NewServer(cfg Config) *Server {
	return &Server{cfg: cfg}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (s *Server) handleEvent(topic, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		event := Event{
			ID:        fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano()),
			Type:      eventType,
			Timestamp: time.Now().UTC(),
			Payload:   payload,
		}

		partition, offset, err := s.publishEvent(r.Context(), topic, event)
		if err != nil {
			log.Printf("publish %s event failed: %v", topic, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusCreated, eventResponse{
			Status:    "success",
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			Event:     event,
		})
	}
}

func (s *Server) publishEvent(ctx context.Context, topic string, event Event) (int, int64, error) {
	message, err := json.Marshal(event)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal event: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		partition, offset, err := s.writeToKafka(ctx, topic, message)
		if err == nil {
			log.Printf("published %s event id=%s partition=%d offset=%d", topic, event.ID, partition, offset)
			return partition, offset, nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		}
	}

	return 0, 0, fmt.Errorf("publish event to Kafka topic %s: %w", topic, lastErr)
}

func (s *Server) writeToKafka(ctx context.Context, topic string, message []byte) (int, int64, error) {
	const partition = 0

	conn, err := kafka.DialLeader(ctx, "tcp", s.cfg.KafkaBrokers[0], topic, partition)
	if err != nil {
		return 0, 0, fmt.Errorf("dial kafka leader: %w", err)
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("read last offset: %w", err)
	}

	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return 0, 0, fmt.Errorf("set write deadline: %w", err)
	}
	if _, err := conn.WriteMessages(kafka.Message{Value: message}); err != nil {
		return 0, 0, fmt.Errorf("write message: %w", err)
	}

	return partition, offset, nil
}

func (s *Server) startConsumers(ctx context.Context) {
	for _, topic := range []string{movieTopic, userTopic, paymentTopic} {
		topic := topic
		go s.consumeTopic(ctx, topic)
	}
}

func (s *Server) consumeTopic(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        s.cfg.KafkaBrokers,
		Topic:          topic,
		GroupID:        "cinemaabyss-events-service",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("consume %s failed: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf(
			"processed %s event partition=%d offset=%d key=%s payload=%s",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
			string(msg.Value),
		)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response failed: %v", err)
	}
}
