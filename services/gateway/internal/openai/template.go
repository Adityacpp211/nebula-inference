package openai

import (
	"fmt"
	"strings"
)

// Rendered is a chat conversation turned into the prompt the worker receives.
type Rendered struct {
	Prompt string
	// Stops are the template's end-of-turn markers, added to the caller's stop
	// sequences so a model that does not emit EOS still ends at its turn boundary
	// instead of writing the user's next message for them.
	Stops []string
}

// Render applies a chat template.
//
// Templates are rendered by the gateway because the worker protocol carries a
// prompt, and the prompt format belongs to the model rather than to the engine
// (runtimes/base.py). In Phase 4 the template is named in the route table; reading
// the GGUF's own tokenizer.chat_template arrives with artifact metadata parsing in
// Phase 5. TODO(NEB-144).
//
// Every template ends with an open assistant turn (the generation prompt), because
// every chat request asks the model to speak next.
func Render(template string, msgs []Message) (Rendered, error) {
	switch template {
	case "chatml", "":
		return chatML(msgs), nil
	case "llama3":
		return llama3(msgs), nil
	case "plain":
		return plain(msgs), nil
	}
	return Rendered{}, fmt.Errorf("unknown chat template %q", template)
}

// chatML is the format Qwen, many Mistral fine-tunes and OpenAI's own early
// models use:
//
//	<|im_start|>system
//	You are helpful.<|im_end|>
//	<|im_start|>user
//	Hi<|im_end|>
//	<|im_start|>assistant
func chatML(msgs []Message) Rendered {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString("<|im_start|>")
		b.WriteString(m.Role)
		b.WriteString("\n")
		b.WriteString(m.Content)
		b.WriteString("<|im_end|>\n")
	}
	b.WriteString("<|im_start|>assistant\n")
	return Rendered{Prompt: b.String(), Stops: []string{"<|im_end|>", "<|im_start|>"}}
}

// llama3 is Meta's Llama 3 instruct format.
func llama3(msgs []Message) Rendered {
	var b strings.Builder
	b.WriteString("<|begin_of_text|>")
	for _, m := range msgs {
		b.WriteString("<|start_header_id|>")
		b.WriteString(m.Role)
		b.WriteString("<|end_header_id|>\n\n")
		b.WriteString(m.Content)
		b.WriteString("<|eot_id|>")
	}
	b.WriteString("<|start_header_id|>assistant<|end_header_id|>\n\n")
	return Rendered{Prompt: b.String(), Stops: []string{"<|eot_id|>"}}
}

// plain is for base models with no chat training — including the Phase 3 fixture,
// which memorised one paragraph and knows no special tokens. The messages are
// concatenated as text, so a base model sees exactly what was written. A turn
// marker would only be noise it was never trained on.
func plain(msgs []Message) Rendered {
	parts := make([]string, 0, len(msgs))
	for _, m := range msgs {
		parts = append(parts, m.Content)
	}
	return Rendered{Prompt: strings.Join(parts, "\n")}
}

// MergeStops combines caller and template stop sequences, caller first, without
// duplicates, bounded by the worker's limit of eight.
func MergeStops(caller, template []string) []string {
	const workerMax = 8
	seen := map[string]bool{}
	out := make([]string, 0, len(caller)+len(template))
	for _, s := range append(append([]string{}, caller...), template...) {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
		if len(out) == workerMax {
			break
		}
	}
	return out
}
