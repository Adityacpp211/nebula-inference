// Package openai implements the wire format of NEBULA's OpenAI-compatible
// inference API (docs/api.md §2): request parsing and validation, chat prompt
// templates, and the response and stream-chunk shapes.
//
// The validation rule that shapes everything here: a parameter NEBULA cannot
// honour is refused by name with code "unsupported_parameter", never silently
// ignored. Ignoring a parameter that changes output semantics (n, logprobs, tools,
// logit_bias) produces a response that looks right and is not, which is worse than
// a 400 the caller can read.
package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/adityasatwar321/nebula/packages/httpx"
)

// Message is one chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Name    string `json:"name,omitempty"`
}

// StreamOptions mirrors OpenAI's stream_options.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// Extension is the namespaced "nebula" block (docs/api.md §2). Namespaced so no
// future OpenAI field can collide with it.
type Extension struct {
	TimeoutMS    *int64 `json:"timeout_ms,omitempty"`
	DeploymentID string `json:"deployment_id,omitempty"`
	Queue        string `json:"queue,omitempty"`
	Trace        bool   `json:"trace,omitempty"`
}

// Sampling holds the parameters shared by chat and text completion.
type Sampling struct {
	MaxTokens        *int     `json:"-"`
	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"top_p,omitempty"`
	TopK             *int     `json:"top_k,omitempty"`
	Stop             []string `json:"-"`
	Seed             *int64   `json:"seed,omitempty"`
	PresencePenalty  *float64 `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64 `json:"frequency_penalty,omitempty"`
}

// Request is a parsed, validated chat or text completion request.
type Request struct {
	Model string
	// Messages is set for chat; Prompt for text completion. Exactly one is used.
	Messages []Message
	Prompt   string
	Stream   bool
	Usage    bool // stream_options.include_usage
	JSONMode bool // response_format {"type":"json_object"}
	User     string
	Sampling
	Nebula Extension
}

// Kind distinguishes the two endpoints, which share most of their parsing.
type Kind int

// Kinds.
const (
	KindChat Kind = iota
	KindCompletion
)

func (k Kind) String() string {
	if k == KindCompletion {
		return "completions"
	}
	return "chat.completions"
}

// maxStops is OpenAI's own limit on stop sequences.
const maxStops = 4

// maxMessages bounds how many messages one request may carry. A request is bounded
// in bytes by the body limit already; this bounds the per-message work.
const maxMessages = 1024

// fieldRule says what the gateway does with one top-level field.
type fieldRule int

const (
	accepted fieldRule = iota
	// unsupported fields are refused unless they hold their "off" value. Clients
	// that always send the default (n=1, logprobs=false) must keep working.
	unsupported
)

// field table, per endpoint. Anything not listed is refused as unknown; the
// response is the same 400 code, because to a caller "not supported" and "not
// recognised" call for the same fix.
var chatFields = map[string]fieldRule{
	"model": accepted, "messages": accepted, "max_tokens": accepted, "max_completion_tokens": accepted,
	"temperature": accepted, "top_p": accepted, "top_k": accepted, "stop": accepted, "seed": accepted,
	"stream": accepted, "stream_options": accepted, "presence_penalty": accepted,
	"frequency_penalty": accepted, "user": accepted, "response_format": accepted, "nebula": accepted,

	"n": unsupported, "logprobs": unsupported, "top_logprobs": unsupported, "logit_bias": unsupported,
	"tools": unsupported, "tool_choice": unsupported, "parallel_tool_calls": unsupported,
	"functions": unsupported, "function_call": unsupported, "audio": unsupported,
	"modalities": unsupported, "prediction": unsupported, "reasoning_effort": unsupported,
	"service_tier": unsupported, "store": unsupported, "metadata": unsupported,
	"web_search_options": unsupported,
}

var completionFields = map[string]fieldRule{
	"model": accepted, "prompt": accepted, "max_tokens": accepted, "temperature": accepted,
	"top_p": accepted, "top_k": accepted, "stop": accepted, "seed": accepted, "stream": accepted,
	"stream_options": accepted, "presence_penalty": accepted, "frequency_penalty": accepted,
	"user": accepted, "nebula": accepted,

	"n": unsupported, "logprobs": unsupported, "logit_bias": unsupported, "echo": unsupported,
	"best_of": unsupported, "suffix": unsupported,
}

// Parse decodes and validates a request body. It does not know the route: checks
// that depend on the model (context window, JSON mode) are in Check.
func Parse(kind Kind, body []byte) (*Request, error) {
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, invalid("the request body must be a JSON object", "invalid_json", "")
	}
	if dec.More() {
		return nil, invalid("the request body must contain exactly one JSON object", "invalid_json", "")
	}
	if raw == nil {
		return nil, invalid("the request body must be a JSON object", "invalid_json", "")
	}

	rules := chatFields
	if kind == KindCompletion {
		rules = completionFields
	}
	// Sorted so the error names the same field every time for the same body.
	names := make([]string, 0, len(raw))
	for k := range raw {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		rule, known := rules[name]
		if !known {
			return nil, unsupportedParam(name, fmt.Sprintf("%q is not a supported parameter", name))
		}
		if rule == unsupported && !isOff(name, raw[name]) {
			return nil, unsupportedParam(name, fmt.Sprintf("%q is not supported by NEBULA", name)+unsupportedHint(name))
		}
	}

	req := &Request{}
	var err error
	if req.Model, err = requiredString(raw, "model"); err != nil {
		return nil, err
	}

	switch kind {
	case KindChat:
		if req.Messages, err = parseMessages(raw["messages"]); err != nil {
			return nil, err
		}
	case KindCompletion:
		if req.Prompt, err = parsePrompt(raw["prompt"]); err != nil {
			return nil, err
		}
	}

	// max_completion_tokens is the newer OpenAI name. Both are accepted; both at
	// once must agree, rather than one silently winning.
	mt, err := optionalInt(raw, "max_tokens")
	if err != nil {
		return nil, err
	}
	mct, err := optionalInt(raw, "max_completion_tokens")
	if err != nil {
		return nil, err
	}
	switch {
	case mt != nil && mct != nil && *mt != *mct:
		return nil, invalid("max_tokens and max_completion_tokens disagree; send one", "conflicting_parameters", "max_completion_tokens")
	case mct != nil:
		mt = mct
	}
	if mt != nil && *mt < 1 {
		return nil, invalid("max_tokens must be at least 1", "invalid_value", "max_tokens")
	}
	req.MaxTokens = mt

	if req.Temperature, err = optionalFloat(raw, "temperature", 0, 2); err != nil {
		return nil, err
	}
	if req.TopP, err = optionalFloat(raw, "top_p", math.SmallestNonzeroFloat64, 1); err != nil {
		return nil, err
	}
	if req.PresencePenalty, err = optionalFloat(raw, "presence_penalty", -2, 2); err != nil {
		return nil, err
	}
	if req.FrequencyPenalty, err = optionalFloat(raw, "frequency_penalty", -2, 2); err != nil {
		return nil, err
	}
	if req.TopK, err = optionalInt(raw, "top_k"); err != nil {
		return nil, err
	}
	if req.TopK != nil && *req.TopK < 0 {
		return nil, invalid("top_k must not be negative", "invalid_value", "top_k")
	}
	if v, ok := present(raw, "seed"); ok {
		var n json.Number
		if isQuoted(v) || json.Unmarshal(v, &n) != nil {
			return nil, invalid("seed must be an integer", "invalid_type", "seed")
		}
		s, err := n.Int64()
		if err != nil {
			return nil, invalid("seed must be an integer", "invalid_type", "seed")
		}
		req.Seed = &s
	}
	if req.Stop, err = parseStop(raw["stop"]); err != nil {
		return nil, err
	}
	if v, ok := present(raw, "stream"); ok {
		if err := json.Unmarshal(v, &req.Stream); err != nil {
			return nil, invalid("stream must be a boolean", "invalid_type", "stream")
		}
	}
	if v, ok := present(raw, "stream_options"); ok {
		var so StreamOptions
		if err := strictUnmarshal(v, &so); err != nil {
			return nil, invalid("stream_options must be {\"include_usage\": bool}", "invalid_type", "stream_options")
		}
		if !req.Stream {
			// OpenAI refuses this too: the option has no meaning without a stream.
			return nil, invalid("stream_options is only allowed when stream is true", "invalid_value", "stream_options")
		}
		req.Usage = so.IncludeUsage
	}
	if v, ok := present(raw, "user"); ok {
		if err := json.Unmarshal(v, &req.User); err != nil {
			return nil, invalid("user must be a string", "invalid_type", "user")
		}
		if len(req.User) > 256 {
			return nil, invalid("user must be at most 256 characters", "invalid_value", "user")
		}
	}
	if v, ok := present(raw, "response_format"); ok {
		var rf struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(v, &rf); err != nil {
			return nil, invalid("response_format must be an object with a type", "invalid_type", "response_format")
		}
		switch rf.Type {
		case "text":
		case "json_object":
			req.JSONMode = true
		default:
			return nil, unsupportedParam("response_format",
				fmt.Sprintf("response_format type %q is not supported; use \"text\" or \"json_object\"", rf.Type))
		}
	}
	if v, ok := present(raw, "nebula"); ok {
		if err := strictUnmarshal(v, &req.Nebula); err != nil {
			return nil, invalid("nebula must be an object with timeout_ms, deployment_id, queue and trace: "+err.Error(),
				"invalid_type", "nebula")
		}
		if t := req.Nebula.TimeoutMS; t != nil && *t < 1 {
			return nil, invalid("nebula.timeout_ms must be positive", "invalid_value", "nebula.timeout_ms")
		}
		switch req.Nebula.Queue {
		case "", "allow", "reject":
		default:
			return nil, invalid("nebula.queue must be \"allow\" or \"reject\"", "invalid_value", "nebula.queue")
		}
	}
	return req, nil
}

// isOff reports whether an unsupported field holds the value that means "not
// used". The OpenAI SDKs send some of these by default in some versions, and a
// client that asks for exactly what NEBULA does must not be refused.
func isOff(name string, v json.RawMessage) bool {
	s := strings.TrimSpace(string(v))
	if s == "null" {
		return true
	}
	switch name {
	case "n", "best_of":
		return s == "1"
	case "logprobs", "echo", "parallel_tool_calls", "store":
		return s == "false" || (name == "logprobs" && s == "0")
	case "tools", "functions", "modalities":
		return s == "[]"
	case "logit_bias", "metadata":
		return s == "{}"
	case "tool_choice", "function_call":
		return s == `"none"`
	case "suffix":
		return s == `""`
	}
	return false
}

func unsupportedHint(name string) string {
	switch name {
	case "n", "best_of":
		return " (only 1 is supported; send parallel requests instead)"
	case "tools", "tool_choice", "functions", "function_call", "parallel_tool_calls":
		return " (no NEBULA runtime supports tool calling yet)"
	case "logprobs", "top_logprobs":
		return " (the runtimes do not report token log-probabilities)"
	}
	return ""
}

// parseMessages accepts string content, or the array-of-parts form restricted to
// text parts, which the SDKs send for multi-part messages.
func parseMessages(v json.RawMessage) ([]Message, error) {
	if len(v) == 0 || string(v) == "null" {
		return nil, invalid("messages is required", "missing_field", "messages")
	}
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(v, &raw); err != nil {
		return nil, invalid("messages must be an array of message objects", "invalid_type", "messages")
	}
	if len(raw) == 0 {
		return nil, invalid("messages must not be empty", "invalid_value", "messages")
	}
	if len(raw) > maxMessages {
		return nil, invalid(fmt.Sprintf("messages must contain at most %d entries", maxMessages), "invalid_value", "messages")
	}
	out := make([]Message, 0, len(raw))
	for i, m := range raw {
		param := fmt.Sprintf("messages[%d]", i)
		for k := range m {
			switch k {
			case "role", "content", "name":
			case "tool_calls", "tool_call_id", "function_call", "audio", "refusal":
				if strings.TrimSpace(string(m[k])) == "null" {
					continue
				}
				return nil, unsupportedParam(param+"."+k, fmt.Sprintf("%s.%s is not supported", param, k))
			default:
				return nil, unsupportedParam(param+"."+k, fmt.Sprintf("%s.%s is not a supported message field", param, k))
			}
		}
		var msg Message
		if err := json.Unmarshal(m["role"], &msg.Role); err != nil || msg.Role == "" {
			return nil, invalid(param+".role is required", "missing_field", param+".role")
		}
		switch msg.Role {
		case "system", "user", "assistant":
		case "developer":
			// OpenAI's newer name for a system message; the meaning is the same.
			msg.Role = "system"
		case "tool", "function":
			return nil, unsupportedParam(param+".role", fmt.Sprintf("role %q is not supported: no NEBULA runtime supports tool calling yet", msg.Role))
		default:
			return nil, invalid(fmt.Sprintf("%s.role %q is not one of system, user, assistant", param, msg.Role), "invalid_value", param+".role")
		}
		content, err := parseContent(m["content"], param+".content")
		if err != nil {
			return nil, err
		}
		msg.Content = content
		if n, ok := m["name"]; ok && string(n) != "null" {
			if err := json.Unmarshal(n, &msg.Name); err != nil {
				return nil, invalid(param+".name must be a string", "invalid_type", param+".name")
			}
		}
		out = append(out, msg)
	}
	return out, nil
}

func parseContent(v json.RawMessage, param string) (string, error) {
	if len(v) == 0 || string(v) == "null" {
		return "", invalid(param+" is required", "missing_field", param)
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(v, &parts); err != nil {
		return "", invalid(param+" must be a string or an array of text parts", "invalid_type", param)
	}
	var b strings.Builder
	for i, p := range parts {
		if p.Type != "text" {
			return "", unsupportedParam(fmt.Sprintf("%s[%d].type", param, i),
				fmt.Sprintf("content part type %q is not supported; only text is", p.Type))
		}
		b.WriteString(p.Text)
	}
	return b.String(), nil
}

// parsePrompt accepts a string or a one-element array of strings. Batched prompts
// would return several choices, which is n>1 by another name.
func parsePrompt(v json.RawMessage) (string, error) {
	if len(v) == 0 || string(v) == "null" {
		return "", invalid("prompt is required", "missing_field", "prompt")
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s, nil
	}
	var arr []string
	if err := json.Unmarshal(v, &arr); err == nil {
		if len(arr) == 1 {
			return arr[0], nil
		}
		return "", unsupportedParam("prompt", "batched prompts are not supported; send one prompt per request")
	}
	return "", unsupportedParam("prompt", "prompt must be a string; token-array prompts are not supported")
}

func parseStop(v json.RawMessage) ([]string, error) {
	if len(v) == 0 || string(v) == "null" {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		if s == "" {
			return nil, nil
		}
		return []string{s}, nil
	}
	var arr []string
	if err := json.Unmarshal(v, &arr); err != nil {
		return nil, invalid("stop must be a string or an array of strings", "invalid_type", "stop")
	}
	if len(arr) > maxStops {
		return nil, invalid(fmt.Sprintf("stop must contain at most %d sequences", maxStops), "invalid_value", "stop")
	}
	out := arr[:0]
	for _, s := range arr {
		if s != "" {
			out = append(out, s)
		}
	}
	return out, nil
}

func present(raw map[string]json.RawMessage, name string) (json.RawMessage, bool) {
	v, ok := raw[name]
	if !ok || strings.TrimSpace(string(v)) == "null" {
		return nil, false
	}
	return v, true
}

func requiredString(raw map[string]json.RawMessage, name string) (string, error) {
	v, ok := present(raw, name)
	if !ok {
		return "", invalid(name+" is required", "missing_field", name)
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", invalid(name+" must be a string", "invalid_type", name)
	}
	if strings.TrimSpace(s) == "" {
		return "", invalid(name+" must not be empty", "invalid_value", name)
	}
	return s, nil
}

func optionalInt(raw map[string]json.RawMessage, name string) (*int, error) {
	v, ok := present(raw, name)
	if !ok {
		return nil, nil
	}
	// json.Number also accepts a quoted numeric string; "9" is a type error in the
	// OpenAI API, so it is one here.
	var n json.Number
	if isQuoted(v) || json.Unmarshal(v, &n) != nil {
		return nil, invalid(name+" must be an integer", "invalid_type", name)
	}
	i, err := n.Int64()
	if err != nil || i > math.MaxInt32 || i < math.MinInt32 {
		return nil, invalid(name+" must be an integer", "invalid_type", name)
	}
	out := int(i)
	return &out, nil
}

func optionalFloat(raw map[string]json.RawMessage, name string, lo, hi float64) (*float64, error) {
	v, ok := present(raw, name)
	if !ok {
		return nil, nil
	}
	var f float64
	if err := json.Unmarshal(v, &f); err != nil {
		return nil, invalid(name+" must be a number", "invalid_type", name)
	}
	if math.IsNaN(f) || f < lo || f > hi {
		lower := fmt.Sprint(lo)
		if lo == math.SmallestNonzeroFloat64 {
			return nil, invalid(fmt.Sprintf("%s must be greater than 0 and at most %g", name, hi), "invalid_value", name)
		}
		return nil, invalid(fmt.Sprintf("%s must be between %s and %g", name, lower, hi), "invalid_value", name)
	}
	return &f, nil
}

func isQuoted(v json.RawMessage) bool {
	s := strings.TrimSpace(string(v))
	return strings.HasPrefix(s, `"`)
}

func strictUnmarshal(v json.RawMessage, into any) error {
	dec := json.NewDecoder(bytes.NewReader(v))
	dec.DisallowUnknownFields()
	return dec.Decode(into)
}

func invalid(message, code, param string) *httpx.APIError {
	return httpx.ErrInvalidRequest(message, code, param)
}

func unsupportedParam(param, message string) *httpx.APIError {
	return httpx.ErrInvalidRequest(message, "unsupported_parameter", param)
}

// ---------------------------------------------------------------------------
// route-dependent checks
// ---------------------------------------------------------------------------

// Limits are what a route allows, supplied by the caller so this package does not
// import the route table.
type Limits struct {
	ContextWindow    int
	DefaultMaxTokens int
	JSONMode         bool
	Streaming        bool
}

// MaxBytesPerToken is a deliberately generous upper bound on how many bytes of
// prompt one token covers. The gateway does not tokenize (axiom A2), so the prompt
// length it can be SURE of is bytes / this. Using a generous bound means the check
// only refuses requests that cannot possibly fit; the worker's own tokenizer
// catches the rest.
const MaxBytesPerToken = 16

// ErrContextLength is the code for a request that cannot fit the context window.
const ErrContextLength = "context_length_exceeded"

// Check applies the checks that depend on the route and fills in defaults.
func (r *Request) Check(model string, l Limits) error {
	if r.JSONMode && !l.JSONMode {
		return unsupportedParam("response_format",
			fmt.Sprintf("response_format json_object is not supported by the runtime serving %s", model))
	}
	if r.Stream && !l.Streaming {
		return unsupportedParam("stream", fmt.Sprintf("streaming is not supported by the runtime serving %s", model))
	}
	if r.MaxTokens == nil {
		d := l.DefaultMaxTokens
		// The default never claims more than the window can hold once the prompt's
		// guaranteed minimum is accounted for.
		if room := l.ContextWindow - r.minPromptTokens(); d > room {
			d = room
		}
		if d < 1 {
			return invalid(fmt.Sprintf("the prompt is too long for %s's context window of %d tokens", model, l.ContextWindow),
				ErrContextLength, "messages")
		}
		r.MaxTokens = &d
	}
	if *r.MaxTokens > l.ContextWindow {
		return invalid(fmt.Sprintf("max_tokens (%d) exceeds the context window (%d) of %s",
			*r.MaxTokens, l.ContextWindow, model), ErrContextLength, "max_tokens")
	}
	if min := r.minPromptTokens(); *r.MaxTokens+min > l.ContextWindow {
		return invalid(fmt.Sprintf("max_tokens (%d) plus the prompt (at least %d tokens) exceeds the context window (%d) of %s",
			*r.MaxTokens, min, l.ContextWindow, model), ErrContextLength, "max_tokens")
	}
	return nil
}

// minPromptTokens is a lower bound on the prompt's token count.
func (r *Request) minPromptTokens() int {
	n := len(r.Prompt)
	for _, m := range r.Messages {
		n += len(m.Content)
	}
	return (n + MaxBytesPerToken - 1) / MaxBytesPerToken
}

// PromptBytes is the size of the prompt text, for the rate limiter's token
// reservation.
func (r *Request) PromptBytes() int {
	n := len(r.Prompt)
	for _, m := range r.Messages {
		n += len(m.Content)
	}
	return n
}
