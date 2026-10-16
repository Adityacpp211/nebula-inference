// Package promquery runs Prometheus instant queries for the services that decide
// from metrics: the autoscaler (docs/architecture.md §7.2) and the rollout
// reconciler (§7.3).
//
// A result is a map from one label's value to the sample. Values that are not
// finite — a quantile over no requests is NaN — are dropped: they are absences,
// and a caller must never read an absence as zero.
package promquery

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client queries one Prometheus.
type Client struct {
	BaseURL string
	// HTTP is the client to use; nil uses http.DefaultClient. Set a timeout.
	HTTP *http.Client
}

type apiResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// Vector runs an instant query and returns its finite values keyed by label.
// Series without the label are keyed by "".
func (c *Client) Vector(ctx context.Context, query, label string) (map[string]float64, error) {
	if c == nil || c.BaseURL == "" {
		return nil, fmt.Errorf("prometheus: no URL configured")
	}
	u := strings.TrimRight(c.BaseURL, "/") + "/api/v1/query?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, err
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var body apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decoding the response (HTTP %d): %w", resp.StatusCode, err)
	}
	if body.Status != "success" {
		return nil, fmt.Errorf("prometheus: %s: %s", body.ErrorType, body.Error)
	}
	if body.Data.ResultType != "vector" {
		return nil, fmt.Errorf("prometheus: want a vector, got %q", body.Data.ResultType)
	}
	out := make(map[string]float64, len(body.Data.Result))
	for _, s := range body.Data.Result {
		raw, ok := s.Value[1].(string)
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out[s.Metric[label]] = v
	}
	return out, nil
}

// Duration renders a range in PromQL's syntax, in whole seconds, never below the
// floor: a rate over fewer than two scrapes is noise.
func Duration(d, floor time.Duration) string {
	if d < floor {
		d = floor
	}
	return strconv.Itoa(int(d.Seconds())) + "s"
}
