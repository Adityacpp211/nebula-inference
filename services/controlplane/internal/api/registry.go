package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/auth"
	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/httpx"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// ---------------------------------------------------------------------------
// models
// ---------------------------------------------------------------------------

func (a *API) listModels(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	p, err := page(r)
	if err != nil {
		return err
	}

	rows, err := a.Store.Models.List(r.Context(), a.Store.Pool(), ident.OrgID, p)
	if err != nil {
		return storeError(err, "model")
	}

	out := make([]modelResponse, 0, len(rows))
	for _, m := range rows {
		out = append(out, newModelResponse(m))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(m modelResponse) string {
		return m.ID.String()
	}))
	return nil
}

func (a *API) createModel(w http.ResponseWriter, r *http.Request) error {
	var req createModelRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.Name, "name", modelNamePattern,
		"name must be lowercase alphanumeric with dots, dashes or underscores, 1-64 characters, "+
			"starting and ending with a letter or digit"); err != nil {
		return err
	}
	if err := requireEnum(req.Task, "task", []string{
		string(models.TaskChat), string(models.TaskCompletion), string(models.TaskEmbedding),
	}); err != nil {
		return err
	}

	ident := auth.MustFromContext(r.Context())
	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}

	m := &models.Model{
		ID:          id,
		OrgID:       ident.OrgID,
		Name:        req.Name,
		Family:      req.Family,
		Task:        models.ModelTask(req.Task),
		Description: req.Description,
		CreatedBy:   createdBy(ident),
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if err := a.Store.Models.Create(ctx, q, m); err != nil {
			return storeError(err, "model")
		}
		entry := auditFor(r, store.ActionModelCreate, store.ResourceModel)
		entry.ResourceID = &m.ID
		entry.After = newModelResponse(m)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusCreated, newModelResponse(m))
	return nil
}

func (a *API) getModel(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "model_id", "model")
	if err != nil {
		return err
	}
	m, err := a.Store.Models.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "model")
	}
	a.write(w, r, http.StatusOK, newModelResponse(m))
	return nil
}

func (a *API) updateModel(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "model_id", "model")
	if err != nil {
		return err
	}
	var req updateModelRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}

	var updated *models.Model
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Models.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model")
		}

		family, description := before.Family, before.Description
		if req.Family != nil {
			family = req.Family
		}
		if req.Description != nil {
			description = req.Description
		}
		if err := a.Store.Models.UpdateDescription(ctx, q, ident.OrgID, id,
			family, description); err != nil {
			return storeError(err, "model")
		}

		after, err := a.Store.Models.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model")
		}
		updated = after

		entry := auditFor(r, store.ActionModelUpdate, store.ResourceModel)
		entry.ResourceID = &id
		entry.Before = newModelResponse(before)
		entry.After = newModelResponse(after)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusOK, newModelResponse(updated))
	return nil
}

func (a *API) deleteModel(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "model_id", "model")
	if err != nil {
		return err
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Models.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model")
		}
		if err := a.Store.Models.SoftDelete(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "model")
		}
		entry := auditFor(r, store.ActionModelDelete, store.ResourceModel)
		entry.ResourceID = &id
		entry.Before = newModelResponse(before)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---------------------------------------------------------------------------
// model versions
// ---------------------------------------------------------------------------

func (a *API) listVersions(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	modelID, err := pathUUID(r, "model_id", "model")
	if err != nil {
		return err
	}
	p, err := page(r)
	if err != nil {
		return err
	}

	// The model is fetched first so a listing for an unknown model is a 404 rather
	// than an empty page, which a client cannot distinguish from a model with no
	// versions.
	if _, err := a.Store.Models.Get(r.Context(), a.Store.Pool(), ident.OrgID, modelID); err != nil {
		return storeError(err, "model")
	}

	rows, err := a.Store.Versions.ListForModel(r.Context(), a.Store.Pool(), ident.OrgID, modelID, p)
	if err != nil {
		return storeError(err, "model version")
	}

	out := make([]versionResponse, 0, len(rows))
	for _, v := range rows {
		out = append(out, newVersionResponse(v))
	}
	a.write(w, r, http.StatusOK, newList(out, p.Limit, func(v versionResponse) string {
		return v.ID.String()
	}))
	return nil
}

// createVersion registers a version and, from Phase 3, hands back an upload target.
//
// The checksum, size and context window are declared by the client at creation
// because the schema requires them and because the two-phase upload needs the
// digest to name the object. They are CLAIMS at this point; finalize is where the
// claim is checked. Nothing that reads a version treats an uploading one as
// usable, so a false claim cannot reach a deployment: a deployment requires a ready
// version.
func (a *API) createVersion(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	modelID, err := pathUUID(r, "model_id", "model")
	if err != nil {
		return err
	}

	var req createVersionRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := requireName(req.Version, "version", versionPattern,
		"version must be lowercase alphanumeric with dots, dashes or underscores, 1-64 characters"); err != nil {
		return err
	}
	if err := requireEnum(req.Format, "format", []string{
		string(models.FormatGGUF), string(models.FormatSafetensors), string(models.FormatMock),
	}); err != nil {
		return err
	}
	if err := requireEnum(req.Runtime, "runtime", []string{
		string(models.RuntimeLlamaCPP), string(models.RuntimeVLLM), string(models.RuntimeMock),
	}); err != nil {
		return err
	}
	if err := a.requireRuntimeAvailable(models.Runtime(req.Runtime), models.ModelFormat(req.Format)); err != nil {
		return err
	}
	if err := requireArtifactURI(req.ArtifactURI, !a.Config.Env.IsProduction()); err != nil {
		return err
	}
	checksum, err := requireChecksum(deref(req.ChecksumSHA256), "checksum_sha256")
	if err != nil {
		return err
	}
	if req.SizeBytes == nil || *req.SizeBytes <= 0 {
		return httpx.ErrInvalidRequest(
			"size_bytes is required and must be positive", "invalid_size", "size_bytes")
	}
	if req.ContextWindow == nil || *req.ContextWindow <= 0 {
		return httpx.ErrInvalidRequest(
			"context_window is required and must be positive", "invalid_context_window", "context_window")
	}
	runtimeConfig, err := requireJSONObject(req.RuntimeConfig, "runtime_config")
	if err != nil {
		return err
	}
	hardware, err := hardwareProfile(req.HardwareProfile)
	if err != nil {
		return err
	}

	if _, err := a.Store.Models.Get(r.Context(), a.Store.Pool(), ident.OrgID, modelID); err != nil {
		return storeError(err, "model")
	}

	id, err := db.NewID()
	if err != nil {
		return httpx.ErrInternal(err)
	}

	v := &models.ModelVersion{
		ID:              id,
		ModelID:         modelID,
		Version:         req.Version,
		Format:          models.ModelFormat(req.Format),
		Runtime:         models.Runtime(req.Runtime),
		Quantization:    req.Quantization,
		ParameterCount:  req.ParameterCount,
		SizeBytes:       *req.SizeBytes,
		ChecksumSHA256:  checksum,
		ContextWindow:   *req.ContextWindow,
		ArtifactURI:     req.ArtifactURI,
		HardwareProfile: hardware,
		RuntimeConfig:   runtimeConfig,
		Status:          models.VersionUploading,
		CreatedBy:       createdBy(ident),
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		if err := a.Store.Versions.Create(ctx, q, v); err != nil {
			return storeError(err, "model version")
		}
		entry := auditFor(r, store.ActionModelVersionCreate, store.ResourceModelVersion)
		entry.ResourceID = &v.ID
		entry.After = newVersionResponse(v)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusCreated, createdVersionResponse{
		versionResponse: newVersionResponse(v),
		Upload:          nil,
		Note: "upload is null because object storage arrives in Phase 3: the version is registered " +
			"against the artifact_uri you supplied, and no presigned upload target was issued. " +
			"Call POST /v1/model-versions/" + v.ID.String() + "/finalize once the artifact is in place.",
	})
	return nil
}

func (a *API) getVersion(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "version_id", "model version")
	if err != nil {
		return err
	}
	v, err := a.Store.Versions.Get(r.Context(), a.Store.Pool(), ident.OrgID, id)
	if err != nil {
		return storeError(err, "model version")
	}
	a.write(w, r, http.StatusOK, newVersionResponse(v))
	return nil
}

// finalizeVersion moves a version from uploading to ready.
//
// What actually happens, stated plainly because the difference matters: the
// control plane compares the checksum the client declared at creation with the one
// it presents now. It does NOT hash the artifact bytes — that is the artifact
// service's job in Phase 3, and this endpoint is the transition it will call. The
// response says which verification was performed, and the audit record stores it,
// so a version that became ready without its bytes being read is identifiable
// afterwards rather than indistinguishable from a verified one (axiom A6).
//
// Both transitions (uploading -> verifying -> ready) happen in one transaction: a
// version stuck in verifying with nothing running to move it would need manual
// repair, which is a worse outcome than a single atomic step.
func (a *API) finalizeVersion(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "version_id", "model version")
	if err != nil {
		return err
	}

	var req finalizeVersionRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	presented, err := requireChecksum(req.ChecksumSHA256, "checksum_sha256")
	if err != nil {
		return err
	}

	var out *models.ModelVersion
	var mismatch *httpx.APIError

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Versions.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model version")
		}
		if before.Status != models.VersionUploading {
			return conflict(fmt.Sprintf(
				"model version is %s; only an uploading version can be finalized", before.Status),
				"invalid_status")
		}

		declared := hexString(before.ChecksumSHA256)
		if !strings.EqualFold(declared, hexString(presented)) {
			// The version is failed, not left uploading: a mismatch is terminal, and
			// leaving it retryable would let a caller keep guessing until one matched.
			reason := fmt.Sprintf(
				"checksum mismatch: declared %s at creation, presented %s at finalize",
				declared, hexString(presented))
			if err := a.Store.Versions.SetStatus(ctx, q, ident.OrgID, id,
				models.VersionUploading, models.VersionFailed, &reason); err != nil {
				return storeError(err, "model version")
			}
			entry := auditFor(r, store.ActionModelVersionFail, store.ResourceModelVersion)
			entry.ResourceID = &id
			entry.Before = newVersionResponse(before)
			entry.After = map[string]any{"status": string(models.VersionFailed), "failure_reason": reason}
			if err := a.Store.Audit.AppendFromContext(ctx, q, entry); err != nil {
				return err
			}
			mismatch = unprocessable(reason, "checksum_mismatch", "checksum_sha256")
			return nil
		}

		if err := a.Store.Versions.SetStatus(ctx, q, ident.OrgID, id,
			models.VersionUploading, models.VersionVerifying, nil); err != nil {
			return storeError(err, "model version")
		}
		if err := a.Store.Versions.SetStatus(ctx, q, ident.OrgID, id,
			models.VersionVerifying, models.VersionReady, nil); err != nil {
			return storeError(err, "model version")
		}

		after, err := a.Store.Versions.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model version")
		}
		out = after

		entry := auditFor(r, store.ActionModelVersionFinalize, store.ResourceModelVersion)
		entry.ResourceID = &id
		entry.Before = newVersionResponse(before)
		entry.After = map[string]any{
			"status":       string(after.Status),
			"ready_at":     after.ReadyAt,
			"verification": verificationDeclared,
		}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}
	// The mismatch is returned after the transaction commits, so the failed status
	// and its audit record are durable before the caller is told.
	if mismatch != nil {
		return mismatch
	}

	a.write(w, r, http.StatusOK, struct {
		versionResponse
		Verification string `json:"verification"`
		Note         string `json:"note"`
	}{
		versionResponse: newVersionResponse(out),
		Verification:    verificationDeclared,
		Note: "the control plane compared the checksum declared at creation with the one presented " +
			"here; it did not read the artifact bytes. Byte-level verification is performed by the " +
			"artifact service in Phase 3.",
	})
	return nil
}

// verificationDeclared names the weakest verification the registry will record. A
// stronger one ("computed") is what Phase 3 writes.
const verificationDeclared = "declared_checksum"

// failVersion records that a version could not be made ready.
//
// It exists because the alternative is a version sitting in uploading forever with
// no explanation. The reason is required: "failed" with no cause is the state that
// makes an incident unexplainable an hour later.
func (a *API) failVersion(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "version_id", "model version")
	if err != nil {
		return err
	}
	var req failVersionRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Reason == "" {
		return httpx.ErrInvalidRequest("reason is required", "missing_field", "reason")
	}
	if len(req.Reason) > 1024 {
		return httpx.ErrInvalidRequest("reason must be at most 1024 characters", "invalid_reason", "reason")
	}

	var out *models.ModelVersion
	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Versions.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model version")
		}
		if err := a.Store.Versions.SetStatus(ctx, q, ident.OrgID, id,
			before.Status, models.VersionFailed, &req.Reason); err != nil {
			return storeError(err, "model version")
		}
		after, err := a.Store.Versions.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model version")
		}
		out = after

		entry := auditFor(r, store.ActionModelVersionFail, store.ResourceModelVersion)
		entry.ResourceID = &id
		entry.Before = newVersionResponse(before)
		entry.After = newVersionResponse(after)
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	a.write(w, r, http.StatusOK, newVersionResponse(out))
	return nil
}

// archiveVersion retires a ready version.
//
// It is a DELETE that archives rather than removes, because a version is referenced
// by revisions, usage records and cost records: deleting the row would make an
// invoice unexplainable. The store refuses while a live deployment references it.
func (a *API) archiveVersion(w http.ResponseWriter, r *http.Request) error {
	ident := auth.MustFromContext(r.Context())
	id, err := pathUUID(r, "version_id", "model version")
	if err != nil {
		return err
	}

	err = a.inTx(r, func(ctx context.Context, q store.Querier) error {
		before, err := a.Store.Versions.Get(ctx, q, ident.OrgID, id)
		if err != nil {
			return storeError(err, "model version")
		}
		if err := a.Store.Versions.Archive(ctx, q, ident.OrgID, id); err != nil {
			return storeError(err, "model version")
		}
		entry := auditFor(r, store.ActionModelVersionArchive, store.ResourceModelVersion)
		entry.ResourceID = &id
		entry.Before = newVersionResponse(before)
		entry.After = map[string]any{"status": string(models.VersionArchived)}
		return a.Store.Audit.AppendFromContext(ctx, q, entry)
	})
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// requireRuntimeAvailable refuses a runtime this build cannot serve.
//
// Accepting a runtime that no worker implements would produce a version that looks
// registered and a deployment that never starts. Phase 2 ships no runtime at all,
// so every value is refused with a message naming the phase that adds it — except
// mock, which is allowed when the development stub is explicitly enabled and is
// named "mock" everywhere it appears (axiom A9).
func (a *API) requireRuntimeAvailable(rt models.Runtime, format models.ModelFormat) error {
	if rt == models.RuntimeMock || format == models.FormatMock {
		if !a.Config.Dev.MockRuntime {
			return unprocessable(
				"the mock runtime is a development stub and is disabled: set NEBULA_DEV_MOCK_RUNTIME=true to register mock artifacts",
				"runtime_unavailable", "runtime")
		}
		if a.Config.Env.IsProduction() {
			return unprocessable(
				"the mock runtime is refused in production", "runtime_unavailable", "runtime")
		}
		if rt != models.RuntimeMock || format != models.FormatMock {
			return httpx.ErrInvalidRequest(
				"the mock runtime requires the mock format, and vice versa",
				"runtime_format_mismatch", "runtime")
		}
	}
	return nil
}

// hardwareProfile validates the hardware requirements object.
func hardwareProfile(in json.RawMessage) (json.RawMessage, error) {
	return requireJSONObject(in, "hardware_profile")
}

// createdBy returns the human behind a credential.
//
// created_by references users(id), so it is the key's owning user — not the key
// itself. A key with no owner records no creator rather than a fabricated one: the
// audit trail already records which key made the change, so nothing is lost.
func createdBy(ident auth.Identity) *uuid.UUID {
	switch {
	case ident.ActorType == models.ActorUser && ident.ActorID != uuid.Nil:
		id := ident.ActorID
		return &id
	case ident.UserID != nil:
		id := *ident.UserID
		return &id
	default:
		return nil
	}
}

// deref reads an optional string.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
