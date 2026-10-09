package telemetry_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// documented parses the metric tables of docs/observability.md §2.1, §2.2 and
// §2.4 (the Go services; §2.3 is the Python worker's). A row whose name carries
// "(Phase N)" is planned, not implemented.
func documented(t *testing.T) (implemented map[string][]string, planned map[string]bool) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "docs", "observability.md"))
	if err != nil {
		t.Fatal(err)
	}
	implemented, planned = map[string][]string{}, map[string]bool{}
	section := ""
	tick := regexp.MustCompile("`([a-z_]+)`")
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "### ") {
			section = line
			continue
		}
		inScope := strings.HasPrefix(section, "### 2.1") || strings.HasPrefix(section, "### 2.2") ||
			strings.HasPrefix(section, "### 2.4")
		if !inScope || !strings.HasPrefix(line, "| `nebula_") {
			continue
		}
		// An escaped pipe is inside a cell, not between cells.
		cells := strings.Split(strings.ReplaceAll(line, `\|`, "/"), "|")
		if len(cells) < 4 {
			continue
		}
		names := tick.FindAllStringSubmatch(cells[1], -1)
		var labels []string
		for _, m := range tick.FindAllStringSubmatch(cells[3], -1) {
			labels = append(labels, m[1])
		}
		for _, n := range names {
			if strings.Contains(cells[1], "(Phase") {
				planned[n[1]] = true
				continue
			}
			implemented[n[1]] = labels
		}
	}
	if len(implemented) < 20 {
		t.Fatalf("parsed only %d metrics from docs/observability.md; the table format changed", len(implemented))
	}
	return implemented, planned
}

// The metrics contract: every documented metric exists with exactly the
// documented labels and type, and nothing undocumented is exported. A rename on
// either side fails here instead of silently emptying a dashboard.
func TestCatalogMatchesTheDocumentedCatalogue(t *testing.T) {
	t.Parallel()
	docs, planned := documented(t)
	for name, labels := range docs {
		d, ok := telemetry.Lookup(name)
		if !ok {
			t.Errorf("%s is documented as implemented but is not in telemetry.Catalog", name)
			continue
		}
		if !slices.Equal(d.Labels, labels) {
			t.Errorf("%s: catalogue labels %v, documented %v", name, d.Labels, labels)
		}
	}
	for _, d := range telemetry.Catalog {
		if _, ok := docs[d.Name]; !ok {
			if planned[d.Name] {
				t.Errorf("%s is exported but documented as planned", d.Name)
			} else {
				t.Errorf("%s is exported but not documented in docs/observability.md", d.Name)
			}
		}
		if d.Kind == telemetry.KindCounter && !strings.HasSuffix(d.Name, "_total") {
			t.Errorf("%s: counters end in _total", d.Name)
		}
		if d.Kind == telemetry.KindHistogram && len(d.Buckets) == 0 {
			t.Errorf("%s: a histogram needs declared buckets", d.Name)
		}
		for _, l := range d.Labels {
			// §2.5: these never become labels.
			if slices.Contains([]string{"request_id", "trace_id", "api_key_id", "user", "prompt", "status_code", "org"}, l) &&
				d.Name != "nebula_estimated_cost_micros_total" {
				t.Errorf("%s: label %q is outside the cardinality budget", d.Name, l)
			}
		}
	}
}

func TestUndeclaredMetricPanics(t *testing.T) {
	t.Parallel()
	m := telemetry.NewMetrics()
	for name, fn := range map[string]func(){
		"undeclared": func() { m.Counter("nebula_invented_total") },
		"wrong kind": func() { m.Gauge("nebula_requests_total") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestHandlerServesDeclaredMetricsWithExemplars(t *testing.T) {
	t.Parallel()
	m := telemetry.NewMetrics()
	m.Counter("nebula_requests_total").WithLabelValues("r", "d", "v", "chat", "2xx", "", "").Inc()
	telemetry.Observe(m.Histogram("nebula_ttft_seconds").WithLabelValues("r", "d", "v"), 0.12,
		"4bf92f3577b34da6a3ce929d0e0e4736")
	m.GaugeFunc("nebula_endpoints", func() []telemetry.Sample {
		return []telemetry.Sample{{Labels: []string{"d", "ready"}, Value: 2}}
	})

	req := httptest.NewRequest("GET", "/metrics", http.NoBody)
	req.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	text := string(body)
	for _, want := range []string{
		`nebula_requests_total{deployment="d",endpoint="chat"`,
		`nebula_endpoints{deployment="d",state="ready"} 2`,
		`trace_id="4bf92f3577b34da6a3ce929d0e0e4736"`,
		"go_goroutines",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
}

func TestStatusClass(t *testing.T) {
	t.Parallel()
	for code, want := range map[int]string{200: "2xx", 429: "4xx", 499: "4xx", 503: "5xx", 0: "other"} {
		if got := telemetry.StatusClass(code); got != want {
			t.Errorf("%d: %s", code, got)
		}
	}
}
