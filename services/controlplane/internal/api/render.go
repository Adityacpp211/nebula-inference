// Package api holds the control plane's HTTP handlers.
//
// The whole package obeys four rules, and most of the helpers here exist to make
// them cheap to obey:
//
//  1. The tenant comes from the authenticated identity, never from a path, query
//     or body parameter. There is no code path that accepts an org_id
//     (docs/security-boundaries.md §2, B2).
//  2. A handler opens exactly one transaction and does all of its work inside it,
//     including the audit record. A change whose audit row is written in a second
//     transaction is a change that can happen unrecorded.
//  3. Store sentinels are mapped to HTTP statuses in one place (storeError), so no
//     handler decides for itself what a conflict looks like.
//  4. Nothing here reports a state it has not observed. Endpoints that depend on
//     the Phase 5 controller say so explicitly rather than pretending the work
//     happened (axiom A6, A9).
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// maxJSONBytes bounds a control-plane request body. The global MaxBody middleware
// bounds inference payloads, which are far larger; a deployment spec that arrives
// at 300 kB is a mistake worth rejecting early.
const maxJSONBytes = 256 << 10

// decodeJSON reads a JSON body strictly: unknown fields are an error.
//
// Strictness is deliberate. A caller who sends {"replicas": 3} to an endpoint that
// expects {"desired_replicas": 3} should be told, not silently given the default.
// Every such typo that reaches production is a scaling operation that did nothing.
func decodeJSON(r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if mt := strings.TrimSpace(strings.Split(ct, ";")[0]); mt != "application/json" {
			return httpx.ErrInvalidRequest(
				"Content-Type must be application/json", "unsupported_media_type", "Content-Type")
		}
	}

	dec := json.NewDecoder(io.LimitReader(r.Body, maxJSONBytes+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.Is(err, io.EOF):
			return httpx.ErrInvalidRequest("a JSON request body is required", "missing_body", "body")
		case errors.As(err, &typeErr):
			return httpx.ErrInvalidRequest(
				fmt.Sprintf("field %q must be a %s", typeErr.Field, typeErr.Type.String()),
				"invalid_type", typeErr.Field)
		default:
			return httpx.ErrInvalidRequest(
				"request body is not valid JSON: "+err.Error(), "invalid_json", "body")
		}
	}
	// A second value in the stream means the caller sent two documents, which is
	// never intentional.
	if dec.More() {
		return httpx.ErrInvalidRequest(
			"request body must contain exactly one JSON object", "trailing_content", "body")
	}
	return nil
}

// ---------------------------------------------------------------------------
// error mapping
// ---------------------------------------------------------------------------

// storeError maps a repository error to an HTTP error.
//
// resource names what was being acted on, so a 404 reads "model version not
// found" rather than "not found". The wrapped store message is carried through for
// conflicts because it is written for a human ("a deployment in ready must be
// stopped before it can be deleted") and is the difference between a usable 409
// and a mystifying one.
func storeError(err error, resource string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return httpx.ErrNotFound(resource + " not found")
	case errors.Is(err, store.ErrConflict):
		return conflict(trimSentinel(err, store.ErrConflict), "conflict")
	case errors.Is(err, store.ErrImmutable):
		return conflict(trimSentinel(err, store.ErrImmutable), "immutable")
	case errors.Is(err, store.ErrInUse):
		return conflict(trimSentinel(err, store.ErrInUse), "in_use")
	case errors.Is(err, store.ErrAuditHorizon):
		// An audit insert past the partition horizon is an operational failure, not
		// a client error: the caller did nothing wrong and a retry will not help.
		return httpx.ErrInternal(err)
	default:
		return httpx.ErrInternal(err)
	}
}

// conflict builds a 409 in the standard envelope.
func conflict(message, code string) *httpx.APIError {
	return &httpx.APIError{
		Status:  http.StatusConflict,
		Message: message,
		Type:    httpx.TypeConflict,
		Code:    code,
	}
}

// unprocessable builds a 422: the request was well-formed but cannot be acted on.
func unprocessable(message, code, param string) *httpx.APIError {
	return &httpx.APIError{
		Status:  http.StatusUnprocessableEntity,
		Message: message,
		Type:    httpx.TypeUnprocessable,
		Code:    code,
		Param:   param,
	}
}

// forbidden builds a 403.
func forbidden(message, code string) *httpx.APIError {
	return &httpx.APIError{
		Status:  http.StatusForbidden,
		Message: message,
		Type:    httpx.TypePermission,
		Code:    code,
	}
}

// unauthenticated builds a 401. The message never distinguishes "no such key"
// from "wrong key": that difference is an oracle for enumerating valid prefixes.
func unauthenticated(message, code string) *httpx.APIError {
	return &httpx.APIError{
		Status:  http.StatusUnauthorized,
		Message: message,
		Type:    httpx.TypeAuthentication,
		Code:    code,
	}
}

// trimSentinel removes the "conflict: " prefix a store error carries, leaving the
// human sentence behind it. If there is nothing behind it, the sentinel's own text
// is returned rather than an empty message.
func trimSentinel(err, sentinel error) string {
	msg := err.Error()
	prefix := sentinel.Error() + ": "
	if rest, ok := strings.CutPrefix(msg, prefix); ok && rest != "" {
		return rest
	}
	return msg
}

// ---------------------------------------------------------------------------
// path and query parsing
// ---------------------------------------------------------------------------

// pathUUID reads a UUID path parameter.
//
// An unparseable id is a 404 rather than a 400: /v1/models/not-a-uuid and
// /v1/models/<a valid but unknown uuid> should be indistinguishable, because
// telling a caller which of their guesses was well-formed is the first step of an
// enumeration.
func pathUUID(r *http.Request, name, resource string) (uuid.UUID, error) {
	raw := r.PathValue(name)
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.ErrNotFound(resource + " not found")
	}
	return id, nil
}

// page parses ?limit= and ?cursor=.
func page(r *http.Request) (store.Page, error) {
	q := r.URL.Query()

	limit := 0
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return store.Page{}, httpx.ErrInvalidRequest(
				"limit must be a positive integer", "invalid_limit", "limit")
		}
		if n > store.MaxPageLimit {
			return store.Page{}, httpx.ErrInvalidRequest(
				fmt.Sprintf("limit must not exceed %d", store.MaxPageLimit), "invalid_limit", "limit")
		}
		limit = n
	}

	var cursor *uuid.UUID
	if raw := q.Get("cursor"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			return store.Page{}, httpx.ErrInvalidRequest(
				"cursor is not a valid page cursor", "invalid_cursor", "cursor")
		}
		cursor = &id
	}
	return store.NewPage(limit, cursor), nil
}

// ---------------------------------------------------------------------------
// responses
// ---------------------------------------------------------------------------

// listEnvelope is the shape of every collection response (docs/api.md §1).
//
// has_more is derived from whether the page came back full rather than from a
// count query: a count of a large table is a slow query run on every page, and the
// answer is only advisory anyway.
type listEnvelope[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
	HasMore    bool    `json:"has_more"`
}

// newList builds a list envelope. cursorOf extracts the cursor of the last item.
func newList[T any](items []T, limit int32, cursorOf func(T) string) listEnvelope[T] {
	out := listEnvelope[T]{Data: items}
	if out.Data == nil {
		// An empty collection serialises as [] rather than null: a client that
		// iterates the field should not have to special-case emptiness.
		out.Data = []T{}
	}
	// len is compared as int64 rather than the slice length being narrowed to int32:
	// a conversion here would be unchecked, and a page is bounded by MaxPageLimit
	// anyway, so the comparison is the honest way to express it.
	if limit > 0 && int64(len(items)) == int64(limit) {
		c := cursorOf(items[len(items)-1])
		out.NextCursor = &c
		out.HasMore = true
	}
	return out
}

// write renders a successful response, logging a failed write rather than
// swallowing it. A write failure means the client went away mid-response, which is
// worth knowing when a handler looks slow.
func (a *API) write(w http.ResponseWriter, r *http.Request, status int, body any) {
	if err := httpx.WriteJSON(w, status, body); err != nil {
		a.logger(r).WarnContext(r.Context(), "writing response body failed",
			slog.String("cause", err.Error()))
	}
}

// fail renders an error response.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	httpx.WriteError(w, r, err, a.Logger)
}

// handler adapts a function that returns an error into an http.HandlerFunc, so
// handlers can `return err` instead of remembering to write and return.
func (a *API) handler(fn func(http.ResponseWriter, *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			a.fail(w, r, err)
		}
	}
}

// pageLimit parses ?limit= alone, for collections whose cursor is not a bare uuid.
func pageLimit(r *http.Request) (int32, error) {
	p, err := page(withoutCursor(r))
	if err != nil {
		return 0, err
	}
	return p.Limit, nil
}

// withoutCursor returns a shallow copy of the request with the cursor removed, so
// the shared limit parsing can be reused without it rejecting a composite cursor.
func withoutCursor(r *http.Request) *http.Request {
	if r.URL.Query().Get("cursor") == "" {
		return r
	}
	clone := *r
	u := *r.URL
	q := u.Query()
	q.Del("cursor")
	u.RawQuery = q.Encode()
	clone.URL = &u
	return &clone
}

// uuidParam parses a uuid query parameter.
func uuidParam(raw, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, httpx.ErrInvalidRequest(
			field+" must be a UUID", "invalid_"+field, field)
	}
	return id, nil
}

// timeParam parses an optional RFC 3339 timestamp.
func timeParam(raw, field string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, httpx.ErrInvalidRequest(
			field+" must be an RFC 3339 timestamp, e.g. 2026-09-22T10:00:00Z",
			"invalid_"+field, field)
	}
	return &t, nil
}

// The audit cursor carries both halves of the sort key, separated by a space
// (percent-encoded in a URL). A composite key needs a composite cursor: paging a
// partitioned table by id alone would have to search every partition.
const auditCursorSep = " "

func formatAuditCursor(at time.Time, id string) string {
	return at.UTC().Format(time.RFC3339Nano) + auditCursorSep + id
}

func parseAuditCursor(raw string) (*store.AuditCursor, error) {
	atRaw, idRaw, ok := strings.Cut(raw, auditCursorSep)
	if !ok {
		return nil, httpx.ErrInvalidRequest(
			"cursor is not a valid page cursor", "invalid_cursor", "cursor")
	}
	at, err := time.Parse(time.RFC3339Nano, atRaw)
	if err != nil {
		return nil, httpx.ErrInvalidRequest(
			"cursor is not a valid page cursor", "invalid_cursor", "cursor")
	}
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return nil, httpx.ErrInvalidRequest(
			"cursor is not a valid page cursor", "invalid_cursor", "cursor")
	}
	return &store.AuditCursor{At: at, ID: id}, nil
}
