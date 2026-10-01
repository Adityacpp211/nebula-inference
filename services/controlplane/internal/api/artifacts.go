package api

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/artifact"
	"github.com/adityasatwar321/nebula/packages/config"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/services/controlplane/internal/store"
)

// Verification kinds, recorded in the finalize response and the audit trail.
const (
	// verificationComputed: the control plane read every byte of the stored
	// artifact and computed its SHA-256 (docs/security-boundaries.md §2, B7).
	verificationComputed = "computed"
)

// NewArtifactStore builds the configured store, or nil for "none".
func NewArtifactStore(cfg config.ArtifactConfig) (artifact.Store, error) {
	switch cfg.Store {
	case "s3":
		return artifact.NewS3(artifact.S3Config{
			Endpoint:       cfg.S3Endpoint,
			PublicEndpoint: cfg.S3PublicEndpoint,
			Bucket:         cfg.S3Bucket,
			AccessKey:      cfg.S3AccessKey.Reveal(),
			SecretKey:      cfg.S3SecretKey.Reveal(),
			Region:         cfg.S3Region,
			UseTLS:         cfg.S3UseTLS,
			MaxBytes:       cfg.MaxBytes,
		})
	case "file":
		return artifact.NewDir(cfg.Dir)
	}
	return nil, nil
}

// storeKey returns the key when an artifact URI lives in this control plane's
// store. A version registered against an external URI returns false and keeps the
// declared-checksum behaviour.
func (a *API) storeKey(uri string) (string, bool) {
	if a.Artifacts == nil {
		return "", false
	}
	scheme, _, key, err := artifact.ParseURI(uri)
	if err != nil || scheme != a.Artifacts.Scheme() {
		return "", false
	}
	// The whole URI must be the one this store would mint for the key: same bucket
	// or directory. A URI naming another bucket is someone else's artifact.
	if a.Artifacts.URI(key) != uri {
		return "", false
	}
	return key, true
}

// ---------------------------------------------------------------------------
// the verifier
// ---------------------------------------------------------------------------

// Verifier reads stored artifacts end to end and moves their versions from
// verifying to ready or failed.
//
// It runs in the control plane rather than inside the finalize request because a
// multi-gigabyte hash takes minutes: the request returns 202 at once and the
// client polls the version. Work comes from the database (status = verifying),
// not from memory, so a restart mid-verification resumes rather than leaving a
// version stranded; a PostgreSQL advisory lock per version keeps two replicas from
// hashing the same artifact at once.
type Verifier struct {
	api  *API
	kick chan struct{}
}

// NewVerifier builds a verifier for an API with an artifact store.
func NewVerifier(a *API) *Verifier {
	return &Verifier{api: a, kick: make(chan struct{}, 1)}
}

// Kick asks for an immediate pass, e.g. right after a finalize.
func (v *Verifier) Kick() {
	select {
	case v.kick <- struct{}{}:
	default:
	}
}

// Run works until ctx ends.
func (v *Verifier) Run(ctx context.Context) {
	t := time.NewTicker(v.api.Config.Artifact.VerifyInterval.Duration())
	defer t.Stop()
	for {
		v.pass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-v.kick:
		}
	}
}

func (v *Verifier) pass(ctx context.Context) {
	pending, err := v.api.Store.Versions.ListVerifying(ctx, v.api.Store.Pool(), 16)
	if err != nil {
		v.api.Logger.WarnContext(ctx, "listing versions to verify failed", slog.String("cause", err.Error()))
		return
	}
	for _, p := range pending {
		if ctx.Err() != nil {
			return
		}
		v.one(ctx, p)
	}
}

// advisoryClass namespaces the verifier's advisory locks.
const advisoryClass = 0x4e42 // "NB"

func (v *Verifier) one(ctx context.Context, p store.Verifying) {
	a := v.api
	ver := p.Version
	logger := a.Logger.With(slog.String("model_version_id", ver.ID.String()), slog.String("org_id", p.OrgID.String()))

	conn, err := a.Store.Pool().Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()
	lockKey := int32(ver.ID.ID()) // uuid's first 32 bits: enough to spread, collisions only delay
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1, $2)`, advisoryClass, lockKey).Scan(&got); err != nil || !got {
		return // another replica is verifying it
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1, $2)`, advisoryClass, lockKey)
	}()

	started := time.Now()
	result, meta, failure, retry := v.inspect(ctx, ver)
	if retry || ctx.Err() != nil {
		// A transient store error, or shutdown: leave it verifying. Recording
		// anything here would be recording a verification that did not happen.
		return
	}

	system := store.Actor{Type: string(models.ActorSystem)}
	err = a.Store.InTxForOrg(ctx, p.OrgID, system, func(tx pgx.Tx) error {
		entry := store.Entry{
			OrgID:        p.OrgID,
			ActorType:    models.ActorSystem,
			Action:       store.ActionModelVersionVerify,
			ResourceType: store.ResourceModelVersion,
			ResourceID:   &ver.ID,
		}
		after := map[string]any{
			"verification":    verificationComputed,
			"computed_sha256": result.SHA256,
			"bytes_read":      result.Size,
			"duration_ms":     time.Since(started).Milliseconds(),
		}
		if meta != nil {
			after["gguf"] = meta
		}
		if failure != "" {
			if err := a.Store.Versions.SetStatus(ctx, tx, p.OrgID, ver.ID,
				models.VersionVerifying, models.VersionFailed, &failure); err != nil {
				return err
			}
			after["status"], after["failure_reason"] = string(models.VersionFailed), failure
		} else {
			if err := a.Store.Versions.SetStatus(ctx, tx, p.OrgID, ver.ID,
				models.VersionVerifying, models.VersionReady, nil); err != nil {
				return err
			}
			after["status"] = string(models.VersionReady)
		}
		entry.After = after
		return a.Store.Audit.Append(ctx, tx, entry)
	})
	if err != nil {
		logger.WarnContext(ctx, "recording verification failed; it will be retried", slog.String("cause", err.Error()))
		return
	}
	if failure != "" {
		logger.WarnContext(ctx, "artifact verification failed", slog.String("reason", failure))
	} else {
		logger.InfoContext(ctx, "artifact verified", slog.Int64("bytes", result.Size),
			slog.Duration("took", time.Since(started)))
	}
}

// inspect reads the artifact. It returns a failure reason for anything that is the
// artifact's fault, and retry=true for a transient store error, which leaves the
// version verifying for the next pass.
func (v *Verifier) inspect(ctx context.Context, ver *models.ModelVersion) (
	result artifact.Verified, meta *artifact.GGUF, failure string, retry bool,
) {
	a := v.api
	key, ok := a.storeKey(ver.ArtifactURI)
	if !ok {
		return result, nil, "the artifact is not in this installation's store, so its bytes cannot be verified", false
	}
	declared := hex.EncodeToString(ver.ChecksumSHA256)

	rc, err := a.Artifacts.Open(ctx, key)
	if err != nil {
		if errors.Is(err, artifact.ErrNotFound) {
			return result, nil, "the artifact was not found in the store; upload it before finalizing", false
		}
		return result, nil, "", true
	}
	result, err = artifact.Hash(rc, a.Config.Artifact.MaxBytes)
	_ = rc.Close()
	if err != nil {
		if errors.Is(err, artifact.ErrTooLarge) {
			return result, nil, fmt.Sprintf("the artifact exceeds the %d-byte limit", a.Config.Artifact.MaxBytes), false
		}
		return result, nil, "", true
	}
	if err := result.Check(declared, ver.SizeBytes); err != nil {
		// The computed value goes in the reason, so the caller can see what the store
		// actually holds (docs/api.md §4).
		return result, nil, err.Error(), false
	}

	if ver.Format != models.FormatGGUF {
		return result, nil, "", false
	}
	rc, err = a.Artifacts.Open(ctx, key)
	if err != nil {
		return result, nil, "", true
	}
	defer rc.Close()
	meta, err = artifact.ParseGGUF(rc)
	if err != nil {
		return result, nil, "the artifact is registered as gguf but is not a readable GGUF file: " + err.Error(), false
	}
	if meta.ContextLength > 0 && uint64(ver.ContextWindow) > meta.ContextLength {
		return result, meta, fmt.Sprintf("context_window %d exceeds the model's own context length %d",
			ver.ContextWindow, meta.ContextLength), false
	}
	return result, meta, "", false
}
