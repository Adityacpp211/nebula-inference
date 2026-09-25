package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// ---------------------------------------------------------------------------
// organizations
// ---------------------------------------------------------------------------

// OrganizationRepo reads and writes tenants.
//
// Organizations are the one entity NOT scoped by a tenant, because creating one is
// how a tenant comes into existence. Every method here therefore runs outside the
// row-level-security session and is reachable only with the admin scope.
type OrganizationRepo struct{}

const orgColumns = `id, slug, name, pricing_profile_id, default_namespace, quota,
	created_at, updated_at, deleted_at`

// Create inserts an organization.
func (r *OrganizationRepo) Create(ctx context.Context, q Querier, o *models.Organization) error {
	if o.Quota == nil {
		o.Quota = []byte(`{}`)
	}
	err := q.QueryRow(ctx, `
		INSERT INTO organizations (id, slug, name, pricing_profile_id, default_namespace, quota)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at, updated_at`,
		o.ID, o.Slug, o.Name, o.PricingProfileID, o.DefaultNamespace, o.Quota,
	).Scan(&o.CreatedAt, &o.UpdatedAt)
	return classify(err)
}

// Get returns an organization by id.
func (r *OrganizationRepo) Get(ctx context.Context, q Querier, id uuid.UUID) (*models.Organization, error) {
	var o models.Organization
	err := q.QueryRow(ctx, `SELECT `+orgColumns+`
		  FROM organizations WHERE id = $1 AND deleted_at IS NULL`, id).
		Scan(&o.ID, &o.Slug, &o.Name, &o.PricingProfileID, &o.DefaultNamespace, &o.Quota,
			&o.CreatedAt, &o.UpdatedAt, &o.DeletedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &o, nil
}

// GetBySlug returns an organization by slug.
func (r *OrganizationRepo) GetBySlug(ctx context.Context, q Querier, slug string) (*models.Organization, error) {
	var o models.Organization
	err := q.QueryRow(ctx, `SELECT `+orgColumns+`
		  FROM organizations WHERE slug = $1 AND deleted_at IS NULL`, slug).
		Scan(&o.ID, &o.Slug, &o.Name, &o.PricingProfileID, &o.DefaultNamespace, &o.Quota,
			&o.CreatedAt, &o.UpdatedAt, &o.DeletedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &o, nil
}

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

// UserRepo reads and writes users.
type UserRepo struct{}

const userColumns = `id, org_id, email, name, role, password_hash, last_login_at,
	created_at, updated_at, deleted_at`

func scanUser(row interface{ Scan(...any) error }) (*models.User, error) {
	var u models.User
	err := row.Scan(&u.ID, &u.OrgID, &u.Email, &u.Name, &u.Role, &u.PasswordHash,
		&u.LastLoginAt, &u.CreatedAt, &u.UpdatedAt, &u.DeletedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &u, nil
}

// Create inserts a user. Email is lowercased here rather than relying on the
// caller, because the database has a CHECK requiring it and a 500 from a
// constraint is a worse experience than normalising the input.
func (r *UserRepo) Create(ctx context.Context, q Querier, u *models.User) error {
	u.Email = strings.ToLower(strings.TrimSpace(u.Email))
	err := q.QueryRow(ctx, `
		INSERT INTO users (id, org_id, email, name, role, password_hash)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at, updated_at`,
		u.ID, u.OrgID, u.Email, u.Name, u.Role, u.PasswordHash,
	).Scan(&u.CreatedAt, &u.UpdatedAt)
	return classify(err)
}

// Get returns a user within a tenant.
func (r *UserRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userColumns+`
		  FROM users WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`, id, orgID))
}

// GetByEmail returns a user by email across tenants, for login. Email is globally
// unique, so this is how a caller who does not yet know their tenant authenticates.
func (r *UserRepo) GetByEmail(ctx context.Context, q Querier, email string) (*models.User, error) {
	return scanUser(q.QueryRow(ctx, `SELECT `+userColumns+`
		  FROM users WHERE email = $1 AND deleted_at IS NULL`,
		strings.ToLower(strings.TrimSpace(email))))
}

// List returns a tenant's users, newest first.
func (r *UserRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, p Page) ([]*models.User, error) {
	rows, err := q.Query(ctx, `SELECT `+userColumns+`
		  FROM users
		 WHERE org_id = $1 AND deleted_at IS NULL AND ($2::uuid IS NULL OR id < $2)
		 ORDER BY id DESC LIMIT $3`, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, classify(rows.Err())
}

// UpdateRole changes a user's role.
func (r *UserRepo) UpdateRole(ctx context.Context, q Querier, orgID, id uuid.UUID, role models.UserRole) error {
	tag, err := q.Exec(ctx, `
		UPDATE users SET role = $3 WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
		id, orgID, role)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SoftDelete marks a user deleted.
//
// The last owner cannot be removed: an organization with no owner cannot be
// administered, and the check is here rather than in a handler so every path is
// covered.
func (r *UserRepo) SoftDelete(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	var role models.UserRole
	if err := q.QueryRow(ctx,
		`SELECT role FROM users WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
		id, orgID).Scan(&role); err != nil {
		return classify(err)
	}

	if role == models.RoleOwner {
		var owners int
		if err := q.QueryRow(ctx,
			`SELECT count(*) FROM users
			  WHERE org_id = $1 AND role = 'owner' AND deleted_at IS NULL`, orgID).Scan(&owners); err != nil {
			return classify(err)
		}
		if owners <= 1 {
			return fmt.Errorf("%w: an organization must keep at least one owner", ErrConflict)
		}
	}

	tag, err := q.Exec(ctx, `
		UPDATE users SET deleted_at = now() WHERE id = $1 AND org_id = $2 AND deleted_at IS NULL`,
		id, orgID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// TouchLogin records a successful login.
func (r *UserRepo) TouchLogin(ctx context.Context, q Querier, id uuid.UUID) error {
	_, err := q.Exec(ctx, `UPDATE users SET last_login_at = now() WHERE id = $1`, id)
	return classify(err)
}

// ---------------------------------------------------------------------------
// api keys
// ---------------------------------------------------------------------------

// APIKeyRepo reads and writes API keys.
type APIKeyRepo struct{}

const keyColumns = `id, org_id, user_id, name, prefix, key_hash, scopes, priority,
	rate_limit_policy_id, last_used_at, expires_at, revoked_at, created_at`

func scanKey(row interface{ Scan(...any) error }) (*models.APIKey, error) {
	var k models.APIKey
	err := row.Scan(&k.ID, &k.OrgID, &k.UserID, &k.Name, &k.Prefix, &k.KeyHash,
		&k.Scopes, &k.Priority, &k.RateLimitPolicyID, &k.LastUsedAt, &k.ExpiresAt,
		&k.RevokedAt, &k.CreatedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &k, nil
}

// Create inserts an API key. The plaintext never reaches this layer.
func (r *APIKeyRepo) Create(ctx context.Context, q Querier, k *models.APIKey) error {
	err := q.QueryRow(ctx, `
		INSERT INTO api_keys (id, org_id, user_id, name, prefix, key_hash, scopes,
		                      priority, rate_limit_policy_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING created_at`,
		k.ID, k.OrgID, k.UserID, k.Name, k.Prefix, k.KeyHash, k.Scopes,
		k.Priority, k.RateLimitPolicyID, k.ExpiresAt,
	).Scan(&k.CreatedAt)
	return classify(err)
}

// GetByPrefix looks a key up for authentication.
//
// Deliberately NOT tenant-scoped: the tenant is what this lookup discovers. The
// prefix is unique across the installation and indexed, so this is one index probe
// before the constant-time hash comparison (ADR-0011).
//
// Revoked and expired keys are returned rather than filtered, so the caller can
// distinguish "no such key" from "revoked key" in its logs without a second query,
// while still refusing both.
func (r *APIKeyRepo) GetByPrefix(ctx context.Context, q Querier, prefix string) (*models.APIKey, error) {
	return scanKey(q.QueryRow(ctx, `SELECT `+keyColumns+` FROM api_keys WHERE prefix = $1`, prefix))
}

// Get returns a key within a tenant.
func (r *APIKeyRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.APIKey, error) {
	return scanKey(q.QueryRow(ctx, `SELECT `+keyColumns+`
		  FROM api_keys WHERE id = $1 AND org_id = $2`, id, orgID))
}

// List returns a tenant's keys, newest first. Revoked keys are included: an
// operator auditing access needs to see what was revoked and when.
func (r *APIKeyRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, p Page) ([]*models.APIKey, error) {
	rows, err := q.Query(ctx, `SELECT `+keyColumns+`
		  FROM api_keys
		 WHERE org_id = $1 AND ($2::uuid IS NULL OR id < $2)
		 ORDER BY id DESC LIMIT $3`, orgID, p.Cursor, p.Limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.APIKey
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, classify(rows.Err())
}

// Revoke marks a key unusable. Idempotent: revoking an already-revoked key
// succeeds without changing revoked_at, so a retried request does not rewrite
// history.
func (r *APIKeyRepo) Revoke(ctx context.Context, q Querier, orgID, id uuid.UUID) error {
	tag, err := q.Exec(ctx, `
		UPDATE api_keys SET revoked_at = now()
		 WHERE id = $1 AND org_id = $2 AND revoked_at IS NULL`, id, orgID)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() == 0 {
		// Either it does not exist or it is already revoked; distinguish so a
		// retry is not reported as a missing key.
		var exists bool
		if err := q.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM api_keys WHERE id = $1 AND org_id = $2)`,
			id, orgID).Scan(&exists); err != nil {
			return classify(err)
		}
		if !exists {
			return ErrNotFound
		}
	}
	return nil
}

// TouchUsed records that a key authenticated a request.
//
// Called at most once per key per minute by the authenticator, because updating a
// row on every request would make authentication a write path and put the audit
// trail's cost on the hot path.
func (r *APIKeyRepo) TouchUsed(ctx context.Context, q Querier, id uuid.UUID, notBefore time.Duration) error {
	_, err := q.Exec(ctx, `
		UPDATE api_keys SET last_used_at = now()
		 WHERE id = $1 AND (last_used_at IS NULL OR last_used_at < now() - $2::interval)`,
		id, notBefore.String())
	return classify(err)
}

// ---------------------------------------------------------------------------
// pagination
// ---------------------------------------------------------------------------

// Page is a cursor-based page request.
//
// Cursor pagination rather than offset: with offset, concurrent inserts make rows
// appear twice or not at all, and every list endpoint here is over data that is
// being written while it is read. The cursor is the last id seen, which works
// because identifiers are UUIDv7 and therefore time-ordered (ADR-0023).
type Page struct {
	Limit  int32
	Cursor *uuid.UUID
}

// Pagination bounds.
const (
	DefaultPageLimit = 50
	MaxPageLimit     = 200
)

// NewPage clamps a requested page size into range.
//
// The clamp is what makes the int32 conversion safe: limit is bounded to
// [1, MaxPageLimit] before it is narrowed, so there is no value that could wrap.
// Converting first and validating afterwards would be the bug.
func NewPage(limit int, cursor *uuid.UUID) Page {
	switch {
	case limit <= 0:
		limit = DefaultPageLimit
	case limit > MaxPageLimit:
		limit = MaxPageLimit
	}
	return Page{Limit: int32(limit), Cursor: cursor} // #nosec G115 -- clamped above
}

// ---------------------------------------------------------------------------
// rate-limit policies
// ---------------------------------------------------------------------------

// RateLimitPolicyRepo reads rate-limit policies. Written by an admin surface that
// arrives with Phase 17's policy management; read today so the gateway enforces the
// policy a key is actually bound to rather than a default.
type RateLimitPolicyRepo struct{}

// Get returns a policy visible to an org: its own, or a built-in (org_id NULL).
func (r *RateLimitPolicyRepo) Get(ctx context.Context, q Querier, orgID, id uuid.UUID) (*models.RateLimitPolicy, error) {
	var p models.RateLimitPolicy
	err := q.QueryRow(ctx, `
		SELECT id, org_id, name, requests_per_minute, tokens_per_minute, max_concurrency,
		       max_queue_depth, created_at
		  FROM rate_limit_policies
		 WHERE id = $1 AND (org_id = $2 OR org_id IS NULL)`, id, orgID).
		Scan(&p.ID, &p.OrgID, &p.Name, &p.RequestsPerMinute, &p.TokensPerMinute,
			&p.MaxConcurrency, &p.MaxQueueDepth, &p.CreatedAt)
	if err != nil {
		return nil, classify(err)
	}
	return &p, nil
}

// ---------------------------------------------------------------------------
// nodes (read-only here: the controller's inventory reconciler writes them)
// ---------------------------------------------------------------------------

// NodeRepo reads the node cache for capacity admission.
type NodeRepo struct{}

// ListPresent returns nodes currently in the cluster, as last synced.
func (r *NodeRepo) ListPresent(ctx context.Context, q Querier) ([]*models.Node, error) {
	rows, err := q.Query(ctx, `
		SELECT id, name, provider_id, labels, taints, capacity, allocatable, requested,
		       conditions, schedulable, kubelet_version, first_seen_at, synced_at, removed_at
		  FROM nodes WHERE removed_at IS NULL ORDER BY name`)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*models.Node
	for rows.Next() {
		var n models.Node
		if err := rows.Scan(&n.ID, &n.Name, &n.ProviderID, &n.Labels, &n.Taints, &n.Capacity,
			&n.Allocatable, &n.Requested, &n.Conditions, &n.Schedulable, &n.KubeletVersion,
			&n.FirstSeenAt, &n.SyncedAt, &n.RemovedAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, &n)
	}
	return out, classify(rows.Err())
}
