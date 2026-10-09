package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/telemetry"
)

// AuditRepo appends to the audit trail.
//
// The table is append-only twice over: the application role holds INSERT and
// SELECT but not UPDATE or DELETE (migration 000007), and a trigger refuses
// UPDATE. So there is deliberately no Update or Delete method here — not as a
// convention, but because the grants would refuse them.
//
// Audit rows are written in the same transaction as the change they describe.
// That is the only way the trail can be trusted: a separate transaction, or a
// write after the commit, produces a trail that is missing exactly the changes
// that happened during an incident.
type AuditRepo struct{}

// Audit action names. They are constants rather than strings written at each call
// site so a dashboard filter cannot be defeated by "model.create" in one handler
// and "models.created" in another.
const (
	ActionOrgCreate = "organization.create"

	ActionUserCreate     = "user.create"
	ActionUserUpdateRole = "user.update_role"
	ActionUserDelete     = "user.delete"

	// These two trip gosec's hardcoded-credential heuristic (G101), which pattern
	// matches on identifiers containing "key" beside a string literal. They are audit
	// action names. The annotation is per line rather than a global exclusion so G101
	// keeps working everywhere else: a real credential reaching a constant is exactly
	// what it is for.
	ActionAPIKeyCreate = "api_key.create" //nolint:gosec // G101: an audit action name
	ActionAPIKeyRevoke = "api_key.revoke" //nolint:gosec // G101: an audit action name

	ActionModelCreate = "model.create"
	ActionModelUpdate = "model.update"
	ActionModelDelete = "model.delete"

	ActionModelVersionCreate   = "model_version.create"
	ActionModelVersionFinalize = "model_version.finalize"
	ActionModelVersionFail     = "model_version.fail"
	ActionModelVersionArchive  = "model_version.archive"
	ActionModelVersionVerify   = "model_version.verify"

	ActionDeploymentCreate     = "deployment.create"
	ActionDeploymentUpdate     = "deployment.update"
	ActionDeploymentScale      = "deployment.scale"
	ActionDeploymentRollback   = "deployment.rollback"
	ActionDeploymentStop       = "deployment.stop"
	ActionDeploymentStart      = "deployment.start"
	ActionDeploymentTransition = "deployment.transition"
	ActionDeploymentDelete     = "deployment.delete"

	ActionRouteCreate = "route.create"
	ActionRouteUpdate = "route.update"
	ActionRouteDelete = "route.delete"
)

// Audit resource types.
const (
	ResourceOrganization = "organization"
	ResourceUser         = "user"
	ResourceAPIKey       = "api_key"
	ResourceModel        = "model"
	ResourceModelVersion = "model_version"
	ResourceDeployment   = "deployment"
	ResourceRoute        = "route"
)

// Entry is one audit record.
//
// Before and After are optional. When both are present they are the same shape,
// so a diff is mechanical; when only After is present the record is a creation.
type Entry struct {
	ID           uuid.UUID
	OrgID        uuid.UUID
	ActorType    models.ActorType
	ActorID      *uuid.UUID
	ActorLabel   *string
	Action       string
	ResourceType string
	ResourceID   *uuid.UUID
	Before       any
	After        any
	RequestID    *uuid.UUID
	IP           net.IP
	UserAgent    *string
	At           time.Time
}

// ErrAuditHorizon means the audit table has no partition covering the row's
// timestamp.
//
// audit_logs is partitioned by month with no default partition, deliberately: a
// row landing in a default partition silently blocks attaching the real partition
// later. So an insert past the horizon fails loudly, and this sentinel exists so
// the failure is reported as "extend the partitions" rather than as an opaque
// constraint error (docs/data-model.md §7).
var ErrAuditHorizon = errors.New("audit log has no partition for this timestamp")

// Append writes one audit record.
func (r *AuditRepo) Append(ctx context.Context, q Querier, e Entry) error {
	if e.OrgID == uuid.Nil {
		return errors.New("refusing to write an audit record with no organization")
	}
	if e.Action == "" || e.ResourceType == "" {
		return errors.New("refusing to write an audit record with no action or resource type")
	}
	if e.ID == uuid.Nil {
		id, err := db.NewID()
		if err != nil {
			return fmt.Errorf("generating audit id: %w", err)
		}
		e.ID = id
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	if e.ActorType == "" {
		e.ActorType = models.ActorSystem
	}

	before, err := toJSON(e.Before)
	if err != nil {
		return fmt.Errorf("encoding audit before state: %w", err)
	}
	after, err := toJSON(e.After)
	if err != nil {
		return fmt.Errorf("encoding audit after state: %w", err)
	}

	var ip *string
	if e.IP != nil {
		s := e.IP.String()
		ip = &s
	}

	_, err = q.Exec(ctx, `
		INSERT INTO audit_logs (
			id, org_id, created_at, actor_type, actor_id, actor_label,
			action, resource_type, resource_id, before, after,
			request_id, ip, user_agent)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::inet,$14)`,
		e.ID, e.OrgID, e.At, e.ActorType, e.ActorID, e.ActorLabel,
		e.Action, e.ResourceType, e.ResourceID, before, after,
		e.RequestID, ip, e.UserAgent)
	if err != nil {
		var pgErr *pgconn.PgError
		// 23514 with no partition found is how Postgres reports the missing range.
		if errors.As(err, &pgErr) && pgErr.Code == pgCheckViolation && pgErr.TableName == "audit_logs" {
			return fmt.Errorf("%w: %s", ErrAuditHorizon, pgErr.Message)
		}
		return classify(err)
	}
	return nil
}

// AppendFromContext writes a record, filling the actor and request fields from the
// request context so no handler has to remember to thread them through.
func (r *AuditRepo) AppendFromContext(ctx context.Context, q Querier, e Entry) error {
	if e.RequestID == nil {
		// The request id is a string in the correlation context because it is also
		// an HTTP header value; audit_logs stores it as a uuid. A non-uuid value
		// (a client-supplied header, say) is dropped rather than failing the write:
		// losing the correlation is better than losing the audit record.
		if id, err := uuid.Parse(telemetry.RequestID(ctx)); err == nil {
			e.RequestID = &id
		}
	}
	return r.Append(ctx, q, e)
}

// AuditFilter narrows a listing of the audit trail. A nil field means "any".
//
// Paging is by (created_at, id) rather than by id alone: audit_logs is partitioned
// by created_at, so an id-only predicate would have to scan every partition.
type AuditFilter struct {
	Action       *string
	ResourceType *string
	ResourceID   *uuid.UUID
	Since        *time.Time
	Until        *time.Time
}

// AuditCursor is the position of a page in the audit trail.
type AuditCursor struct {
	At time.Time
	ID uuid.UUID
}

// List returns audit records for a tenant.
//
// ip comes back through host(), which renders an inet as plain text. pgx has no
// natural Go type for inet, and net.IP loses the distinction the column can carry;
// rendering it in SQL keeps the scan simple and the value exactly what an operator
// would see in psql.
func (r *AuditRepo) List(ctx context.Context, q Querier, orgID uuid.UUID, f AuditFilter,
	cursor *AuditCursor, limit int32) ([]*models.AuditLog, error) {
	var cursorAt *time.Time
	var cursorID *uuid.UUID
	if cursor != nil {
		cursorAt = &cursor.At
		cursorID = &cursor.ID
	}

	rows, err := q.Query(ctx, `
		SELECT id, org_id, created_at, actor_type, actor_id, actor_label,
		       action, resource_type, resource_id, before, after,
		       request_id, host(ip), user_agent
		  FROM audit_logs
		 WHERE org_id = $1
		   AND ($2::text IS NULL OR action = $2)
		   AND ($3::text IS NULL OR resource_type = $3)
		   AND ($4::uuid IS NULL OR resource_id = $4)
		   AND ($5::timestamptz IS NULL OR created_at >= $5)
		   AND ($6::timestamptz IS NULL OR created_at < $6)
		   AND ($7::timestamptz IS NULL
		        OR (created_at, id) < ($7::timestamptz, $8::uuid))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $9`,
		orgID, f.Action, f.ResourceType, f.ResourceID, f.Since, f.Until,
		cursorAt, cursorID, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()

	var out []*models.AuditLog
	for rows.Next() {
		var a models.AuditLog
		var ip *string
		if err := rows.Scan(&a.ID, &a.OrgID, &a.CreatedAt, &a.ActorType, &a.ActorID,
			&a.ActorLabel, &a.Action, &a.ResourceType, &a.ResourceID, &a.Before, &a.After,
			&a.RequestID, &ip, &a.UserAgent); err != nil {
			return nil, classify(err)
		}
		if ip != nil {
			parsed := net.ParseIP(*ip)
			if parsed != nil {
				a.IP = &parsed
			}
		}
		out = append(out, &a)
	}
	return out, classify(rows.Err())
}

// toJSON encodes an audit payload, mapping nil to a SQL NULL rather than to the
// four bytes "null", so "no before state" and "before state was null" stay
// distinguishable.
func toJSON(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return nil, nil
		}
		return raw, nil
	}
	return json.Marshal(v)
}
