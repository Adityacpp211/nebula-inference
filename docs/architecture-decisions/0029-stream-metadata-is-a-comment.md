# 29. Stream routing metadata travels as headers and an SSE comment, not a named event

Date: 2026-09-25

## Status

Accepted. Implemented in Phase 4 (`services/gateway/internal/server/stream.go`).

Supersedes the `event: nebula.meta` design in docs/api.md §2 as written in Phase 0.

## Context

A streamed response needs to tell an operator which route, deployment, model version
and variant served it, before the first token and without changing what an OpenAI
client parses. Phase 0 chose a named SSE event, `event: nebula.meta`, on the
assumption that "OpenAI SDKs ignore named events".

Phase 4's end-to-end test ran the real OpenAI Python SDK (3.19.2) against the gateway,
and the assumption was false. The SDK's stream loop special-cases events named
`thread.*` and treats every other event, named or not, as a data frame. It parsed the
meta event into a `ChatCompletionChunk` whose fields were all `None`. Code that reads
`chunk.choices[0]` on every chunk — which is most code — would crash on the first
frame. That breaks the one guarantee Phase 4 exists to deliver: existing client code
works unmodified.

Earlier SDK versions did skip named events. That is the deeper problem: which events a
client skips is an implementation detail of each SDK version, not a property of the
protocol, and NEBULA cannot depend on it.

## Decision

The routing context is delivered in two places, neither of which any conforming client
can mistake for a chunk:

1. **Response headers**: `X-Nebula-Route`, `X-Nebula-Deployment`,
   `X-Nebula-Model-Version`, `X-Nebula-Variant`, on streamed and non-streamed responses
   alike. For a stream they arrive before the first token, which is the property the
   named event was for. Every SDK exposes headers (`with_raw_response` in OpenAI's).
2. **An SSE comment on the first line**: `: nebula.meta {"request_id":...}`. The SSE
   specification requires every parser to ignore lines beginning with a colon, so this
   is safe by standard rather than by SDK behaviour, and a human reading the stream
   with `curl -N` still sees the context first.

No frame of a NEBULA stream is ever a named event.

## Consequences

- The OpenAI SDK sees only real chunks, which a test in each suite pins: the Go frame
  test refuses any named event, and the end-to-end test asserts the headers and
  iterates the stream through the SDK.
- Tools that want the meta object read a header or parse one comment line; neither
  needs an SSE library that exposes event names.
- The fields that only exist at the end of a generation (queue wait, TTFT, tokens per
  second) are not in the meta comment. They are in the usage record, and in the
  `nebula` block of a non-streamed response.
- The lesson generalizes: compatibility claims about third-party clients are verified
  against the clients, in CI, or they are not claims. The e2e job exists for that.
