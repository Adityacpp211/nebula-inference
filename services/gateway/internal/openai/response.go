package openai

// Response shapes. Field order and omission follow OpenAI's own responses, because
// SDKs deserialize these into typed objects and a missing required field is a
// client-side exception rather than a visible difference.

// Usage is token accounting, always from the runtime's counters (axiom A2).
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// NewUsage fills TotalTokens.
func NewUsage(prompt, completion int) *Usage {
	return &Usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}
}

// Meta is the "nebula" block on a response and the payload of the nebula.meta
// stream event (docs/api.md §2). Every field is measured; a value that could not be
// measured is absent rather than zero (axiom A6).
type Meta struct {
	RequestID       string   `json:"request_id"`
	Route           string   `json:"route,omitempty"`
	Deployment      string   `json:"deployment"`
	ModelVersion    string   `json:"model_version"`
	Variant         string   `json:"variant,omitempty"`
	QueueWaitMS     *int64   `json:"queue_wait_ms,omitempty"`
	TTFTMS          *int64   `json:"ttft_ms,omitempty"`
	DurationMS      *int64   `json:"duration_ms,omitempty"`
	TokensPerSecond *float64 `json:"tokens_per_second,omitempty"`
	Attempts        int      `json:"attempts,omitempty"`
	// Degraded names a degradation that served the response, such as a failover
	// to another route. Absent when none did.
	Degraded string `json:"degraded,omitempty"`
	// Runtime names the engine that produced the tokens, so a response from the
	// declared mock stub can never be mistaken for a real model's.
	Runtime string `json:"runtime,omitempty"`
}

// ChatMessage is the assistant message in a non-streamed response.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatChoice is one choice in a non-streamed chat response.
type ChatChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	Logprobs     *struct{}   `json:"logprobs"`
	FinishReason string      `json:"finish_reason"`
}

// ChatCompletion is a non-streamed chat response.
type ChatCompletion struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []ChatChoice `json:"choices"`
	Usage   *Usage       `json:"usage"`
	Nebula  *Meta        `json:"nebula,omitempty"`
}

// Delta is the incremental part of a chat chunk.
type Delta struct {
	Role    string  `json:"role,omitempty"`
	Content *string `json:"content,omitempty"`
}

// ChunkChoice is one choice in a streamed chat chunk.
type ChunkChoice struct {
	Index        int       `json:"index"`
	Delta        Delta     `json:"delta"`
	Logprobs     *struct{} `json:"logprobs"`
	FinishReason *string   `json:"finish_reason"`
}

// ChatChunk is one streamed chat event. Usage is set only on the final usage
// chunk, whose choices array is empty, exactly as OpenAI does.
type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

// TextChoice is one choice in a text completion or chunk.
type TextChoice struct {
	Index        int       `json:"index"`
	Text         string    `json:"text"`
	Logprobs     *struct{} `json:"logprobs"`
	FinishReason *string   `json:"finish_reason"`
}

// TextCompletion is a text completion, streamed or not; the object field differs.
type TextCompletion struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []TextChoice `json:"choices"`
	Usage   *Usage       `json:"usage,omitempty"`
	Nebula  *Meta        `json:"nebula,omitempty"`
}

// Object names.
const (
	ObjectChatCompletion = "chat.completion"
	ObjectChatChunk      = "chat.completion.chunk"
	ObjectTextCompletion = "text_completion"
	ObjectModel          = "model"
	ObjectList           = "list"
)

// ModelTarget describes one weighted target of a route in GET /v1/models.
type ModelTarget struct {
	Deployment   string `json:"deployment"`
	ModelVersion string `json:"model_version"`
	Weight       int    `json:"weight"`
	Label        string `json:"label,omitempty"`
	// State is the deployment's lifecycle state; ReadyEndpoints counts the
	// replicas this gateway could send a request to right now.
	State          string `json:"state,omitempty"`
	ReadyEndpoints int    `json:"ready_endpoints"`
}

// ModelExtension is the nebula block on a model entry.
type ModelExtension struct {
	RouteID       string        `json:"route_id"`
	ContextWindow int           `json:"context_window"`
	Task          string        `json:"task"`
	Targets       []ModelTarget `json:"targets"`
	Streaming     bool          `json:"streaming"`
	Embeddings    bool          `json:"embeddings"`
	// Source says where the route came from: "controlplane", or "static" for a
	// route file, stated so nobody mistakes a file for the registry.
	Source string `json:"source"`
}

// Model is one entry of GET /v1/models: a route, not a deployment.
type Model struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	OwnedBy string          `json:"owned_by"`
	Nebula  *ModelExtension `json:"nebula,omitempty"`
}

// ModelList is GET /v1/models.
type ModelList struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

// StreamError is the terminal error frame of an interrupted stream (docs/api.md
// §2): {"error": {...}} followed by [DONE], with no finish_reason ever sent, so a
// client can tell an interrupted response from a completed one.
type StreamError struct {
	Error StreamErrorBody `json:"error"`
}

// StreamErrorBody is the error inside a StreamError.
type StreamErrorBody struct {
	Message   string `json:"message"`
	Type      string `json:"type"`
	Code      string `json:"code"`
	RequestID string `json:"request_id"`
}
