package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestRouteMoviesToMicroserviceByPercent(t *testing.T) {
	tests := []struct {
		name      string
		percent   int
		requests  int
		wantMovie int
	}{
		{name: "zero percent stays on monolith", percent: 0, requests: 20, wantMovie: 0},
		{name: "half traffic goes to movies service", percent: 50, requests: 100, wantMovie: 50},
		{name: "full migration goes to movies service", percent: 100, requests: 20, wantMovie: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewServer(Config{
				GradualMigration:       true,
				MoviesMigrationPercent: tt.percent,
			})

			gotMovie := 0
			for i := 0; i < tt.requests; i++ {
				if server.routeMoviesToMicroservice() {
					gotMovie++
				}
			}

			if gotMovie != tt.wantMovie {
				t.Fatalf("movies-routed requests = %d, want %d", gotMovie, tt.wantMovie)
			}
		})
	}
}

func TestGradualMigrationDisabledRoutesMoviesToMonolith(t *testing.T) {
	server := NewServer(Config{
		GradualMigration:       false,
		MoviesMigrationPercent: 100,
	})

	for i := 0; i < 10; i++ {
		if server.routeMoviesToMicroservice() {
			t.Fatal("expected disabled gradual migration to route movies to monolith")
		}
	}
}

func TestServeHTTPRoutesToExpectedUpstreams(t *testing.T) {
	monolith := upstreamServer(t, "monolith")
	movies := upstreamServer(t, "movies-service")
	events := upstreamServer(t, "events-service")

	server := NewServer(Config{
		MonolithURL:            mustURL(t, monolith.URL),
		MoviesServiceURL:       mustURL(t, movies.URL),
		EventsServiceURL:       mustURL(t, events.URL),
		GradualMigration:       true,
		MoviesMigrationPercent: 100,
	})

	tests := []struct {
		path        string
		wantService string
	}{
		{path: "/api/movies", wantService: "movies-service"},
		{path: "/api/users", wantService: "monolith"},
		{path: "/api/events/movie", wantService: "events-service"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.path+"?id=1", nil)
			rec := httptest.NewRecorder()

			server.ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}

			if res.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", res.StatusCode)
			}
			if string(body) != tt.wantService {
				t.Fatalf("body = %q, want %q", string(body), tt.wantService)
			}
			if res.Header.Get("X-Upstream-Service") != tt.wantService {
				t.Fatalf("X-Upstream-Service = %q, want %q", res.Header.Get("X-Upstream-Service"), tt.wantService)
			}
		})
	}
}

func TestRootReturnsGatewayInfo(t *testing.T) {
	server := NewServer(Config{MoviesMigrationPercent: 50})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	server.ServeHTTP(rec, req)

	res := rec.Result()
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if got := res.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}

	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode root response: %v", err)
	}
	if body["service"] != "cinemaabyss-proxy" {
		t.Fatalf("service = %v, want cinemaabyss-proxy", body["service"])
	}
	if body["movies_migration_percent"] != float64(50) {
		t.Fatalf("movies_migration_percent = %v, want 50", body["movies_migration_percent"])
	}
}

func upstreamServer(t *testing.T, name string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "id=1" {
			t.Errorf("raw query = %q, want id=1", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(server.Close)
	return server
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL %q: %v", raw, err)
	}
	return parsed
}
