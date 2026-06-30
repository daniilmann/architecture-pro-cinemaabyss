package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	upstreamMonolith = "monolith"
	upstreamMovies   = "movies-service"
	upstreamEvents   = "events-service"
)

type Config struct {
	Port                   string
	MonolithURL            *url.URL
	MoviesServiceURL       *url.URL
	EventsServiceURL       *url.URL
	GradualMigration       bool
	MoviesMigrationPercent int
}

type Server struct {
	cfg          Config
	client       *http.Client
	movieCounter uint64
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           NewServer(cfg),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("Starting proxy service on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("proxy service failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("proxy service shutdown failed: %v", err)
	}
}

func LoadConfig() (Config, error) {
	monolithURL, err := parseServiceURL("MONOLITH_URL", "http://localhost:8080")
	if err != nil {
		return Config{}, err
	}
	moviesURL, err := parseServiceURL("MOVIES_SERVICE_URL", "http://localhost:8081")
	if err != nil {
		return Config{}, err
	}
	eventsURL, err := parseServiceURL("EVENTS_SERVICE_URL", "http://localhost:8082")
	if err != nil {
		return Config{}, err
	}

	return Config{
		Port:                   env("PORT", "8000"),
		MonolithURL:            monolithURL,
		MoviesServiceURL:       moviesURL,
		EventsServiceURL:       eventsURL,
		GradualMigration:       parseBool(env("GRADUAL_MIGRATION", "false")),
		MoviesMigrationPercent: clampPercent(envInt("MOVIES_MIGRATION_PERCENT", 0)),
	}, nil
}

func NewServer(cfg Config) *Server {
	return &Server{
		cfg: cfg,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		s.handleRoot(w, r)
		return
	}
	if r.URL.Path == "/health" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "Strangler Fig Proxy is healthy")
		return
	}

	upstreamName, upstreamURL := s.selectUpstream(r)
	w.Header().Set("X-Upstream-Service", upstreamName)
	if err := s.proxy(w, r, upstreamURL, upstreamName); err != nil {
		log.Printf("proxy %s %s to %s failed: %v", r.Method, r.URL.RequestURI(), upstreamName, err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"service":                  "cinemaabyss-proxy",
		"status":                   "ok",
		"health":                   "/health",
		"movies_migration_percent": s.cfg.MoviesMigrationPercent,
		"routes": []string{
			"/api/movies",
			"/api/users",
			"/api/payments",
			"/api/subscriptions",
			"/api/events/*",
		},
	})
	if err != nil {
		log.Printf("write root response failed: %v", err)
	}
}

func (s *Server) selectUpstream(r *http.Request) (string, *url.URL) {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/api/events"):
		return upstreamEvents, s.cfg.EventsServiceURL
	case strings.HasPrefix(path, "/api/movies"):
		if s.routeMoviesToMicroservice() {
			return upstreamMovies, s.cfg.MoviesServiceURL
		}
		return upstreamMonolith, s.cfg.MonolithURL
	default:
		return upstreamMonolith, s.cfg.MonolithURL
	}
}

func (s *Server) routeMoviesToMicroservice() bool {
	if !s.cfg.GradualMigration {
		return false
	}
	percent := clampPercent(s.cfg.MoviesMigrationPercent)
	if percent == 0 {
		return false
	}
	if percent == 100 {
		return true
	}

	next := atomic.AddUint64(&s.movieCounter, 1)
	return int((next-1)%100) < percent
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request, upstream *url.URL, upstreamName string) error {
	target := *upstream
	target.Path = singleJoiningSlash(upstream.Path, r.URL.Path)
	target.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), r.Body)
	if err != nil {
		return fmt.Errorf("create upstream request: %w", err)
	}
	copyHeader(req.Header, r.Header)
	req.Host = upstream.Host
	req.Header.Set("X-Forwarded-Host", r.Host)
	req.Header.Set("X-Forwarded-Proto", forwardedProto(r))
	req.Header.Set("X-Forwarded-For", appendForwardedFor(r))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send upstream request: %w", err)
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)
	w.Header().Set("X-Upstream-Service", upstreamName)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return fmt.Errorf("copy upstream response: %w", err)
	}
	return nil
}

func parseServiceURL(name, fallback string) (*url.URL, error) {
	raw := env(name, fallback)
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("%s must include scheme and host", name)
	}
	return parsed, nil
}

func env(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func clampPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func singleJoiningSlash(basePath, requestPath string) string {
	switch {
	case basePath == "":
		return requestPath
	case requestPath == "":
		return basePath
	case strings.HasSuffix(basePath, "/") && strings.HasPrefix(requestPath, "/"):
		return basePath + requestPath[1:]
	case !strings.HasSuffix(basePath, "/") && !strings.HasPrefix(requestPath, "/"):
		return basePath + "/" + requestPath
	default:
		return basePath + requestPath
	}
}

func forwardedProto(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func appendForwardedFor(r *http.Request) string {
	prior := r.Header.Get("X-Forwarded-For")
	remote := r.RemoteAddr
	if idx := strings.LastIndex(remote, ":"); idx > -1 {
		remote = remote[:idx]
	}
	if prior == "" {
		return remote
	}
	return prior + ", " + remote
}
