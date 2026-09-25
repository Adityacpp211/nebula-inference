package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// ---------------------------------------------------------------------------
// models
// ---------------------------------------------------------------------------

// ModelRepo reads and writes model families.
type ModelRepo struct{}

const modelColumns = `id, org_id, name, family, task, description, created_by,
	created_at, updated_at, deleted_at`

func scanModel(row interface{ Scan(...any) error }) (*models.Model, error) {
	var m models.Model
	err := row.Scan(&m.ID, &m.OrgID, &m.Name, &m.Family, &m.Task, &m.Description,
		&m.CreatedBy, &m.CreatedAt, &m.UpdatedAt, &m.DeletedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &m, nil
}

// Create inserts a model.
func (r *ModelRepo) Create(ctx context.Context, q Querier, m *models.Model) error {
	err := q.QueryRow(ctx, `
		INSERT INTO models (id, org_id, name, family, task, description, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at, updated_at`,
		m.ID, m.OrgID, m.Name, m.Family, m.Task, m.Description, m.CreatedBy,
	).Scan(&m.CreatedAt, &m.UpdatedAt)
	return classify(err)
}

// Get returns a model within a tenant.
func (r *ModelRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.Model, error) {
	return scanModel(q.QueryRow(ctx, `SELECT `+modelColumns+`
		  FROM models WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID))
}

// GetByName returns a model by its name within a tenant.
func (r *ModelRepo) GetByName(ctx context.Context, q Querier, orgID uuid.UUID, name string) (*models.Model, error) {
	return scanModel(q.QueryRow(ctx, `SELECT `+modelColumns+`
		  FROM models WHERE org_id = $1 AND name = $2 AND deleted_at IS NULL`, orgID, name))
}

// List returns a tenant's models, newest first.
func (r *ModelRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, p Page) ([]*models.Model, error) {
	rows, err := q.Query(ctx, `SELECT `+modelColumns+`
		  FROM models
		 WHERE org_id = $1 AND deleted_at IS NULL AND ($2::uuid IS NULL OR id < $2)
		 ORDER BY id DESC LIMIT $3`, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.Model
	for rows.Next() {
		m, err := scanModel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, classify(rows.Err())
}

// UpdateDescription changes the mutable metadata of a model.
//
// Only description and family are mutable. The name is not: it appears in
// Kubernetes object names and in every audit record that references the model, and
// renaming it would make history unreadable.
func (r *ModelRepo) UpdateDescription(ctx context.Context, q Querier, orgID, id uuid.UUID,
	family, description *string) error {
	tag, err := q.Exec(ctx, `
		UPDATE models SET family = $3, description = $4
		 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
		id, orgID, family, description)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SoftDelete marks a model deleted, refusing while any version is deployed.
//
// The check is a query rather than a foreign key because the relationship is
// indirect (model -> version -> deployment) and the error needs to name what is
// blocking the delete.
func (r *ModelRepo) SoftDelete(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	var blocking int
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM deployments d
		  JOIN model_versions mv ON mv.id = d.model_version_id
		 WHERE mv.model_id = $1 AND d.org_id = $2 AND d.deleted_at IS NULL`, id, orgID).Scan(&blocking)
	if err != nil {
		return classify(err)
	}
	if blocking > 0 {
		return fmt.Errorf("%w: %d deployment(s) still reference a version of this model", ErrInUse, blocking)
	}

	tag, err := q.Exec(ctx, `
		UPDATE models SET deleted_at = now()
		 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// model versions
// ---------------------------------------------------------------------------

// ModelVersionRepo reads and writes model versions.
//
// A version is immutable once ready, enforced by a database trigger (ADR-0010).
// The methods here are shaped so no code path even attempts to change a frozen
// field: there is no general Update, only the specific transitions the lifecycle
// permits.
type ModelVersionRepo struct{}

const versionColumns = `id, model_id, version, format, runtime, quantization,
	parameter_count, size_bytes, checksum_sha256, context_window, artifact_uri,
	hardware_profile, runtime_config, status, failure_reason, created_by,
	created_at, ready_at`

func scanVersion(row interface{ Scan(...any) error }) (*models.ModelVersion, error) {
	var v models.ModelVersion
	err := row.Scan(&v.ID, &v.ModelID, &v.Version, &v.Format, &v.Runtime, &v.Quantization,
		&v.ParameterCount, &v.SizeBytes, &v.ChecksumSHA256, &v.ContextWindow, &v.ArtifactURI,
		&v.HardwareProfile, &v.RuntimeConfig, &v.Status, &v.FailureReason, &v.CreatedBy,
		&v.CreatedAt, &v.ReadyAt)
	if err != nil {
		return nil, classify(err)
	}
	return &v, nil
}

// Create inserts a version in the uploading state.
func (r *ModelVersionRepo) Create(ctx context.Context, q Querier, v *models.ModelVersion) error {
	if v.RuntimeConfig == nil {
		v.RuntimeConfig = []byte(`{}`)
	}
	err := q.QueryRow(ctx, `
		INSERT INTO model_versions (
			id, model_id, version, format, runtime, quantization, parameter_count,
			size_bytes, checksum_sha256, context_window, artifact_uri,
			hardware_profile, runtime_config, status, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING created_at`,
		v.ID, v.ModelID, v.Version, v.Format, v.Runtime, v.Quantization, v.ParameterCount,
		v.SizeBytes, v.ChecksumSHA256, v.ContextWindow, v.ArtifactURI,
		v.HardwareProfile, v.RuntimeConfig, v.Status, v.CreatedBy,
	).Scan(&v.CreatedAt)
	return classify(err)
}

// Get returns a version, checking the tenant through its model.
//
// The join is what makes the tenant check real: model_versions carries no org_id,
// so a query that only filtered by id would cross tenants (row-level security
// would still catch it, but the application check gives the better error).
func (r *ModelVersionRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.ModelVersion, error) {
	return scanVersion(q.QueryRow(ctx, `
		SELECT `+versionColumns+`
		  FROM model_versions mv
		 WHERE mv.id = $1
		   AND EXISTS (SELECT 1 FROM models m
		                WHERE m.id = mv.model_id AND m.org_id = $2 AND m.deleted_at IS NULL)`,
		id, orgID))
}

// GetByVersion returns a version by model and version label.
func (r *ModelVersionRepo) GetByVersion(ctx context.Context, q Querier, orgID, modelID uuid.UUID,
	version string) (*models.ModelVersion, error) {
	return scanVersion(q.QueryRow(ctx, `
		SELECT `+versionColumns+`
		  FROM model_versions mv
		 WHERE mv.model_id = $1 AND mv.version = $2
		   AND EXISTS (SELECT 1 FROM models m
		                WHERE m.id = mv.model_id AND m.org_id = $3 AND m.deleted_at IS NULL)`,
		modelID, version, orgID))
}

// ListForModel returns a model's versions, newest first.
func (r *ModelVersionRepo) ListForModel(ctx context.Context, q Querier, orgID, modelID uuid.UUID,
	p Page) ([]*models.ModelVersion, error) {
	rows, err := q.Query(ctx, `
		SELECT `+versionColumns+`
		  FROM model_versions mv
		 WHERE mv.model_id = $1
		   AND EXISTS (SELECT 1 FROM models m
		                WHERE m.id = mv.model_id AND m.org_id = $2 AND m.deleted_at IS NULL)
		   AND ($3::uuid IS NULL OR mv.id < $3)
		 ORDER BY mv.id DESC LIMIT $4`, modelID, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.ModelVersion
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, classify(rows.Err())
}

// SetStatus moves a version through its lifecycle with a compare-and-set.
//
// The expected-status predicate is what makes concurrent finalize requests safe:
// the second one affects no rows and gets ErrConflict rather than both appearing to
// succeed. Postgres' own trigger independently refuses anything the lifecycle
// forbids, so a bug here cannot produce an illegal state.
// Verifying is a version awaiting byte-level verification, with its tenant.
type Verifying struct {
	OrgID   uuid.UUID
	Version *models.ModelVersion
}

// ListVerifying returns versions in status verifying across every organization,
// oldest first. It is the registry verifier's work queue, and the reason a version
// whose verification was interrupted by a restart is picked up again rather than
// left in verifying forever. System use only: it is not tenant-scoped.
func (r *ModelVersionRepo) ListVerifying(ctx context.Context, q Querier, limit int) ([]Verifying, error) {
	rows, err := q.Query(ctx, `
		SELECT m.org_id, `+prefixed("mv.", versionColumns)+`
		  FROM model_versions mv
		  JOIN models m ON m.id = mv.model_id
		 WHERE mv.status = 'verifying' AND m.deleted_at IS NULL
		 ORDER BY mv.created_at
		 LIMIT $1`, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []Verifying
	for rows.Next() {
		var v models.ModelVersion
		var org uuid.UUID
		if err := rows.Scan(&org, &v.ID, &v.ModelID, &v.Version, &v.Format, &v.Runtime, &v.Quantization,
			&v.ParameterCount, &v.SizeBytes, &v.ChecksumSHA256, &v.ContextWindow, &v.ArtifactURI,
			&v.HardwareProfile, &v.RuntimeConfig, &v.Status, &v.FailureReason, &v.CreatedBy,
			&v.CreatedAt, &v.ReadyAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, Verifying{OrgID: org, Version: &v})
	}
	return out, classify(rows.Err())
}

// prefixed qualifies every column in a comma-separated list.
func prefixed(prefix, columns string) string {
	parts := strings.Split(columns, ",")
	for i, p := range parts {
		parts[i] = prefix + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

func (r *ModelVersionRepo) SetStatus(ctx context.Context, q Querier, orgID, id uuid.UUID,
	from, to models.ModelVersionStatus, failureReason *string) error {
	if !from.CanTransitionTo(to) {
		return fmt.Errorf("%w: a model version cannot move from %s to %s", ErrConflict, from, to)
	}

	// ready_at is set exactly when the version becomes ready, which the
	// ck_model_versions__ready_fields constraint requires.
	//
	// The literal in the CASE is cast to the enum rather than the parameter being
	// compared as text: a placeholder has exactly one inferred type in PostgreSQL, and
	// using $4 as both an enum and a text value makes the whole statement fail with
	// "inconsistent types deduced for parameter $4" (SQLSTATE 42P08).
	tag, err := q.Exec(ctx, `
		UPDATE model_versions mv
		   SET status = $4,
		       failure_reason = $5,
		       ready_at = CASE WHEN $4 = 'ready'::model_version_status
		                       THEN now() ELSE mv.ready_at END
		 WHERE mv.id = $1
		   AND mv.status = $3
		   AND EXISTS (SELECT 1 FROM models m
		                WHERE m.id = mv.model_id AND m.org_id = $2 AND m.deleted_at IS NULL)`,
		id, orgID, from, to, failureReason)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		// Distinguish "gone" from "someone else moved it first".
		cur, getErr := r.Get(ctx, q, orgID, id)
		if getErr != nil {
			return getErr
		}
		return fmt.Errorf("%w: model version is %s, not %s", ErrConflict, cur.Status, from)
	}
	return nil
}

// Archive moves a ready version to archived, refusing while it backs a deployment.
func (r *ModelVersionRepo) Archive(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	var blocking int
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM deployments
		 WHERE model_version_id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID).Scan(&blocking)
	if err != nil {
		return classify(err)
	}
	if blocking > 0 {
		return fmt.Errorf("%w: %d deployment(s) still reference this version", ErrInUse, blocking)
	}
	return r.SetStatus(ctx, q, orgID, id, models.VersionReady, models.VersionArchived, nil)
}

// CountDeployments reports how many live deployments reference a version. Used by
// the read API so a caller can see why a version cannot be archived.
func (r *ModelVersionRepo) CountDeployments(ctx context.Context, q Querier, orgID, id uuid.UUID) (int, error) {
	var n int
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM deployments
		 WHERE model_version_id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID).Scan(&n)
	return n, classify(err)
}
