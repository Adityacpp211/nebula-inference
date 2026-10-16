package promquery_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adityasatwar321/nebula/packages/promquery"
)

func TestVector(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.URL.Query().Get("query") != "up" {
			http.Error(w, "unexpected", 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{
			"resultType": "vector", "result": []any{
				map[string]any{"metric": map[string]string{"job": "a"}, "value": []any{1.0, "1"}},
				map[string]any{"metric": map[string]string{"job": "b"}, "value": []any{1.0, "NaN"}},
				map[string]any{"metric": map[string]string{}, "value": []any{1.0, "3"}},
			}}})
	}))
	defer srv.Close()
	got, err := (&promquery.Client{BaseURL: srv.URL + "/"}).Vector(context.Background(), "up", "job")
	if err != nil || len(got) != 2 || got["a"] != 1 || got[""] != 3 {
		t.Fatalf("%v %v", got, err)
	}
	if _, ok := got["b"]; ok {
		t.Error("NaN is an absence, not a value")
	}
}

func TestErrors(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "errorType": "bad_data", "error": "parse error"})
	}))
	defer srv.Close()
	if _, err := (&promquery.Client{BaseURL: srv.URL}).Vector(context.Background(), "(", "x"); err == nil {
		t.Error("an error response must be an error")
	}
	if _, err := (&promquery.Client{}).Vector(context.Background(), "up", "x"); err == nil {
		t.Error("no URL must be an error")
	}
	if _, err := (&promquery.Client{BaseURL: "http://127.0.0.1:1"}).Vector(context.Background(), "up", "x"); err == nil {
		t.Error("an unreachable Prometheus must be an error")
	}
}

func TestDuration(t *testing.T) {
	t.Parallel()
	if got := promquery.Duration(90*time.Second, 30*time.Second); got != "90s" {
		t.Error(got)
	}
	if got := promquery.Duration(5*time.Second, 30*time.Second); got != "30s" {
		t.Error(got)
	}
}
