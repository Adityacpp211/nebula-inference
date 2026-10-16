package openai_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/gateway/internal/openai"
)

func apiErr(t *testing.T, err error) *httpx.APIError {
	t.Helper()
	var e *httpx.APIError
	if !errors.As(err, &e) {
		t.Fatalf("want *httpx.APIError, got %T: %v", err, err)
	}
	return e
}

// Every refusal names the parameter, and unsupported ones use one code. This is
// the contract docs/api.md §2 states: refused by name, never silently ignored.
func TestParseRefusesByName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		kind  openai.Kind
		body  string
		code  string
		param string
	}{
		{"n greater than one", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"n":2}`, "unsupported_parameter", "n"},
		{"logprobs on", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"logprobs":true}`, "unsupported_parameter", "logprobs"},
		{"tools", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function"}]}`, "unsupported_parameter", "tools"},
		{"logit_bias", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"logit_bias":{"1":2}}`, "unsupported_parameter", "logit_bias"},
		{"unknown field", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"temprature":1}`, "unsupported_parameter", "temprature"},
		{"echo on completions", openai.KindCompletion, `{"model":"m","prompt":"x","echo":true}`, "unsupported_parameter", "echo"},
		{"best_of", openai.KindCompletion, `{"model":"m","prompt":"x","best_of":3}`, "unsupported_parameter", "best_of"},
		{"messages on completions", openai.KindCompletion, `{"model":"m","prompt":"x","messages":[]}`, "unsupported_parameter", "messages"},
		{"tool role", openai.KindChat, `{"model":"m","messages":[{"role":"tool","content":"x"}]}`, "unsupported_parameter", "messages[0].role"},
		{"tool_calls in message", openai.KindChat, `{"model":"m","messages":[{"role":"assistant","content":"x","tool_calls":[{}]}]}`, "unsupported_parameter", "messages[0].tool_calls"},
		{"image part", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{}}]}]}`, "unsupported_parameter", "messages[0].content[0].type"},
		{"batched prompt", openai.KindCompletion, `{"model":"m","prompt":["a","b"]}`, "unsupported_parameter", "prompt"},
		{"token prompt", openai.KindCompletion, `{"model":"m","prompt":[1,2,3]}`, "unsupported_parameter", "prompt"},
		{"json_schema format", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"response_format":{"type":"json_schema"}}`, "unsupported_parameter", "response_format"},

		{"missing model", openai.KindChat, `{"messages":[{"role":"user","content":"x"}]}`, "missing_field", "model"},
		{"missing messages", openai.KindChat, `{"model":"m"}`, "missing_field", "messages"},
		{"empty messages", openai.KindChat, `{"model":"m","messages":[]}`, "invalid_value", "messages"},
		{"bad role", openai.KindChat, `{"model":"m","messages":[{"role":"robot","content":"x"}]}`, "invalid_value", "messages[0].role"},
		{"temperature too high", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"temperature":2.5}`, "invalid_value", "temperature"},
		{"top_p zero", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"top_p":0}`, "invalid_value", "top_p"},
		{"max_tokens zero", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":0}`, "invalid_value", "max_tokens"},
		{"max_tokens string", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":"9"}`, "invalid_type", "max_tokens"},
		{"max_tokens fractional", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":1.5}`, "invalid_type", "max_tokens"},
		{"conflicting max", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"max_tokens":5,"max_completion_tokens":6}`, "conflicting_parameters", "max_completion_tokens"},
		{"five stops", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"stop":["a","b","c","d","e"]}`, "invalid_value", "stop"},
		{"stream_options without stream", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"stream_options":{"include_usage":true}}`, "invalid_value", "stream_options"},
		{"unknown nebula field", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"nebula":{"priority":"HIGH"}}`, "invalid_type", "nebula"},
		{"bad queue mode", openai.KindChat, `{"model":"m","messages":[{"role":"user","content":"x"}],"nebula":{"queue":"maybe"}}`, "invalid_value", "nebula.queue"},
		{"not an object", openai.KindChat, `[1,2]`, "invalid_json", ""},
		{"two objects", openai.KindChat, `{"model":"m"} {"model":"n"}`, "invalid_json", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := openai.Parse(tc.kind, []byte(tc.body))
			if err == nil {
				t.Fatal("expected an error")
			}
			e := apiErr(t, err)
			if e.Status != 400 || e.Code != tc.code || e.Param != tc.param {
				t.Errorf("got status=%d code=%q param=%q (%s), want 400 %q %q", e.Status, e.Code, e.Param, e.Message, tc.code, tc.param)
			}
		})
	}
}

// Clients that send the "off" value of an unsupported parameter — as some SDK
// versions do by default — must keep working.
func TestParseAcceptsOffValues(t *testing.T) {
	t.Parallel()
	body := `{"model":"m","messages":[{"role":"user","content":"x","tool_calls":null}],
		"n":1,"logprobs":false,"tools":[],"tool_choice":"none","logit_bias":{},"store":false,
		"parallel_tool_calls":false,"metadata":null}`
	if _, err := openai.Parse(openai.KindChat, []byte(body)); err != nil {
		t.Fatalf("off values must be accepted: %v", err)
	}
}

func TestParseFullChatRequest(t *testing.T) {
	t.Parallel()
	body := `{"model":"qwen","messages":[
			{"role":"developer","content":"be brief"},
			{"role":"user","content":[{"type":"text","text":"hel"},{"type":"text","text":"lo"}],"name":"ada"}],
		"max_completion_tokens":64,"temperature":0.2,"top_p":0.9,"top_k":40,"stop":"END","seed":7,
		"stream":true,"stream_options":{"include_usage":true},"presence_penalty":0.5,
		"frequency_penalty":-0.5,"user":"u-1","response_format":{"type":"json_object"},
		"nebula":{"timeout_ms":5000,"deployment_id":"qwen-prod","queue":"reject","trace":true}}`
	r, err := openai.Parse(openai.KindChat, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if r.Model != "qwen" || len(r.Messages) != 2 {
		t.Fatalf("model/messages: %+v", r)
	}
	if r.Messages[0].Role != "system" {
		t.Errorf("developer must map to system, got %q", r.Messages[0].Role)
	}
	if r.Messages[1].Content != "hello" || r.Messages[1].Name != "ada" {
		t.Errorf("text parts must concatenate: %+v", r.Messages[1])
	}
	if *r.MaxTokens != 64 || *r.Temperature != 0.2 || *r.TopP != 0.9 || *r.TopK != 40 || *r.Seed != 7 {
		t.Errorf("sampling: %+v", r.Sampling)
	}
	if len(r.Stop) != 1 || r.Stop[0] != "END" {
		t.Errorf("stop: %v", r.Stop)
	}
	if !r.Stream || !r.Usage || !r.JSONMode || r.User != "u-1" {
		t.Errorf("flags: stream=%v usage=%v json=%v user=%q", r.Stream, r.Usage, r.JSONMode, r.User)
	}
	if *r.Nebula.TimeoutMS != 5000 || r.Nebula.DeploymentID != "qwen-prod" || r.Nebula.Queue != "reject" || !r.Nebula.Trace {
		t.Errorf("nebula: %+v", r.Nebula)
	}
}

func TestParseCompletionPromptForms(t *testing.T) {
	t.Parallel()
	for _, body := range []string{`{"model":"m","prompt":"hi"}`, `{"model":"m","prompt":["hi"]}`} {
		r, err := openai.Parse(openai.KindCompletion, []byte(body))
		if err != nil || r.Prompt != "hi" {
			t.Errorf("%s: prompt=%q err=%v", body, r.Prompt, err)
		}
	}
}

func intp(n int) *int { return &n }

func TestCheckContextWindow(t *testing.T) {
	t.Parallel()
	lim := openai.Limits{ContextWindow: 100, DefaultMaxTokens: 256, Streaming: true}

	// No max_tokens: the default is clamped to what the window can hold.
	r := &openai.Request{Prompt: "short"}
	if err := r.Check("m", lim); err != nil {
		t.Fatal(err)
	}
	if *r.MaxTokens != 99 { // 5 bytes → at least 1 token; 100 − 1
		t.Errorf("default max_tokens: got %d, want 99", *r.MaxTokens)
	}

	r = &openai.Request{Prompt: "x", Sampling: openai.Sampling{MaxTokens: intp(101)}}
	e := apiErr(t, r.Check("m", lim))
	if e.Code != openai.ErrContextLength || e.Param != "max_tokens" {
		t.Errorf("max_tokens over window: %+v", e)
	}

	// A prompt that is certainly too long: 1600 bytes is at least 100 tokens.
	r = &openai.Request{Prompt: strings.Repeat("a", 1600), Sampling: openai.Sampling{MaxTokens: intp(1)}}
	e = apiErr(t, r.Check("m", lim))
	if e.Code != openai.ErrContextLength {
		t.Errorf("long prompt: %+v", e)
	}

	// JSON mode on a runtime without it is refused by name.
	r = &openai.Request{Prompt: "x", JSONMode: true}
	e = apiErr(t, r.Check("m", lim))
	if e.Code != "unsupported_parameter" || e.Param != "response_format" {
		t.Errorf("json mode: %+v", e)
	}
}

func TestRenderTemplates(t *testing.T) {
	t.Parallel()
	msgs := []openai.Message{{Role: "system", Content: "S"}, {Role: "user", Content: "U"}}

	r, err := openai.Render("chatml", msgs)
	if err != nil {
		t.Fatal(err)
	}
	want := "<|im_start|>system\nS<|im_end|>\n<|im_start|>user\nU<|im_end|>\n<|im_start|>assistant\n"
	if r.Prompt != want {
		t.Errorf("chatml:\n got %q\nwant %q", r.Prompt, want)
	}
	if len(r.Stops) == 0 || r.Stops[0] != "<|im_end|>" {
		t.Errorf("chatml stops: %v", r.Stops)
	}

	r, _ = openai.Render("llama3", msgs)
	if !strings.HasPrefix(r.Prompt, "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\nS<|eot_id|>") ||
		!strings.HasSuffix(r.Prompt, "<|start_header_id|>assistant<|end_header_id|>\n\n") {
		t.Errorf("llama3: %q", r.Prompt)
	}

	r, _ = openai.Render("plain", msgs)
	if r.Prompt != "S\nU" || len(r.Stops) != 0 {
		t.Errorf("plain: %q %v", r.Prompt, r.Stops)
	}

	if _, err := openai.Render("nonsense", msgs); err == nil {
		t.Error("an unknown template must be an error")
	}
}

func TestMergeStops(t *testing.T) {
	t.Parallel()
	got := openai.MergeStops([]string{"END", "<|im_end|>"}, []string{"<|im_end|>", "<|im_start|>"})
	want := []string{"END", "<|im_end|>", "<|im_start|>"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
	many := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	if n := len(openai.MergeStops(many, nil)); n != 8 {
		t.Errorf("merged stops must be capped at the worker's 8, got %d", n)
	}
}
