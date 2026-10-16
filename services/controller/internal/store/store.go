// Package store is the controller's restricted view of PostgreSQL.
//
// The control plane owns desired state; the controller reads it and writes only
// OBSERVED columns (docs/architecture.md axiom A1). That split is enforced twice:
// here, where no query writes a spec column, and in the database, where the
// controller connects as nebula_controller and its column-level grants refuse
// anything else (migration 000007). State moves only through the same trigger that
// validates every edge and records history, attributed to actor_type "system".
package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/adityasatwar321/nebula/packages/db"
	"github.com/adityasatwar321/nebula/packages/db/models"
)

// Store is the controller's query set.
type Store struct {
	pool *db.Pool
}

// New builds a store.
func New(pool *db.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the pool for health checks.
func (s *Store) Pool() *db.Pool { return s.pool }

// Version is what the controller needs from a deployment's model version.
type Version struct {
	ID              uuid.UUID
	Model           string
	Version         string
	Format          models.ModelFormat
	Runtime         models.Runtime
	ChecksumHex     string
	SizeBytes       int64
	ArtifactURI     string
	ContextWindow   int32
	HardwareProfile json.RawMessage
	RuntimeConfig   json.RawMessage
}

// Ref is the "model:version" string the gateway asserts and the worker serves.
func (v Version) Ref() string { return v.Model + ":" + v.Version }

// Deployment is one deployment as the controller sees it.
type Deployment struct {
	ID                 uuid.UUID
	OrgID              uuid.UUID
	OrgSlug            string
	Name               string
	State              models.DeploymentState
	StateEnteredAt     time.Time
	Generation         int64
	ObservedGeneration int64
	CurrentRevision    int32
	DesiredReplicas    int32
	Resources          json.RawMessage
	RuntimeOverrides   json.RawMessage
	QueueConfig        json.RawMessage
	ReadyReplicas      int32
	UpdatedReplicas    int32
	Conditions         []Condition
	Version            Version
}

const deploymentSelect = `
	SELECT d.id, d.org_id, o.slug, d.name, d.state, d.state_entered_at, d.generation,
	       d.observed_generation, d.current_revision, d.desired_replicas, d.resources,
	       d.runtime_overrides, d.queue_config, d.ready_replicas, d.updated_replicas, d.conditions,
	       mv.id, m.name, mv.version, mv.format, mv.runtime, mv.checksum_sha256, mv.size_bytes,
	       mv.artifact_uri, mv.context_window, mv.hardware_profile, mv.runtime_config
	  FROM deployments d
	  JOIN organizations o ON o.id = d.org_id
	  JOIN model_versions mv ON mv.id = d.model_version_id
	  JOIN models m ON m.id = mv.model_id`

func scanDeployment(row pgx.Row) (*Deployment, error) {
	var d Deployment
	var sum, conds []byte
	if err := row.Scan(&d.ID, &d.OrgID, &d.OrgSlug, &d.Name, &d.State, &d.StateEnteredAt, &d.Generation,
		&d.ObservedGeneration, &d.CurrentRevision, &d.DesiredReplicas, &d.Resources,
		&d.RuntimeOverrides, &d.QueueConfig, &d.ReadyReplicas, &d.UpdatedReplicas, &conds,
		&d.Version.ID, &d.Version.Model, &d.Version.Version, &d.Version.Format, &d.Version.Runtime,
		&sum, &d.Version.SizeBytes, &d.Version.ArtifactURI, &d.Version.ContextWindow,
		&d.Version.HardwareProfile, &d.Version.RuntimeConfig); err != nil {
		return nil, err
	}
	d.Version.ChecksumHex = hex.EncodeToString(sum)
	_ = json.Unmarshal(conds, &d.Conditions)
	return &d, nil
}

// ErrNotFound means the deployment no longer exists (or was deleted).
var ErrNotFound = errors.New("deployment not found")

// Get returns a live deployment.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Deployment, error) {
	d, err := scanDeployment(s.pool.QueryRow(ctx, deploymentSelect+` WHERE d.id = $1 AND d.deleted_at IS NULL`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// ListLiveIDs returns every deployment that exists, for the resync and for garbage
// collection. The error is what matters most: a failed list must stop garbage
// collection, because deleting objects the controller cannot verify are orphans
// is how an outage becomes data loss (docs/architecture.md §8.4).
func (s *Store) ListLiveIDs(ctx context.Context) (map[uuid.UUID]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM deployments WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// ListNeedingWork returns deployments whose desired state is ahead of what the
// controller has observed, or whose state is one the controller advances.
func (s *Store) ListNeedingWork(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM deployments
		 WHERE deleted_at IS NULL
		   AND (generation <> observed_generation
		        OR state IN ('pending', 'provisioning', 'starting', 'degraded', 'stopping'))`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Transition moves a deployment from one state to another with a reason. It is a
// compare-and-set on the current state: a user who stopped the deployment a moment
// ago wins, and the controller re-reads rather than overwriting. The database
// trigger validates the edge and writes the history row.
//
// It returns false, nil when the state was no longer from.
func (s *Store) Transition(ctx context.Context, id uuid.UUID, from, to models.DeploymentState, reason, message string) (bool, error) {
	var changed bool
	err := db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.current_actor_type', 'system', true)`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT set_config('app.current_actor', '', true)`); err != nil {
			return err
		}
		var msg *string
		if message != "" {
			if len(message) > 1000 {
				message = message[:1000]
			}
			msg = &message
		}
		tag, err := tx.Exec(ctx, `
			UPDATE deployments SET state = $3, state_reason = $4, state_message = $5
			 WHERE id = $1 AND state = $2 AND deleted_at IS NULL`, id, from, to, reason, msg)
		if err != nil {
			return err
		}
		changed = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("transition %s -> %s: %w", from, to, err)
	}
	return changed, nil
}

// Condition is one entry of deployments.conditions, in the Kubernetes style.
type Condition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message,omitempty"`
	LastTransitionTime time.Time `json:"last_transition_time"`
}

// Status is the OBSERVED state written back after a reconcile.
type Status struct {
	ObservedGeneration int64
	ReadyReplicas      int32
	UpdatedReplicas    int32
	Conditions         []Condition
	LastError          string
}

// WriteStatus records observed state. observed_generation only ever moves
// forward: a slow reconcile of an older generation must not rewind it.
func (s *Store) WriteStatus(ctx context.Context, id uuid.UUID, st Status) error {
	conds, err := json.Marshal(st.Conditions)
	if err != nil {
		return err
	}
	if st.Conditions == nil {
		conds = []byte("[]")
	}
	var lastErr *string
	if st.LastError != "" {
		lastErr = &st.LastError
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE deployments
		   SET observed_generation = GREATEST(observed_generation, $2),
		       ready_replicas = $3, updated_replicas = $4, conditions = $5,
		       last_error = $6, last_synced_at = now()
		 WHERE id = $1`, id, st.ObservedGeneration, st.ReadyReplicas, st.UpdatedReplicas, conds, lastErr)
	return err
}

// ---------------------------------------------------------------------------
// inventory
// ---------------------------------------------------------------------------

// Amounts are normalized resource figures.
type Amounts struct {
	CPUMilli  int64 `json:"cpu_milli"`
	MemoryMiB int64 `json:"memory_mib"`
	GPU       int64 `json:"gpu"`
}

// Taint is a node taint, reduced to what admission reads.
type Taint struct {
	Key    string `json:"key"`
	Effect string `json:"effect"`
}

// Node is one node as the inventory writes it.
type Node struct {
	Name           string
	ProviderID     string
	Labels         map[string]string
	Taints         []Taint
	Capacity       Amounts
	Allocatable    Amounts
	Requested      Amounts
	Conditions     []Condition
	Schedulable    bool
	KubeletVersion string
}

// SyncNodes upserts the observed nodes and marks every other present node removed.
// One transaction, so the cache is never half one sync and half another.
func (s *Store) SyncNodes(ctx context.Context, nodes []Node) error {
	return db.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		names := make([]string, 0, len(nodes))
		for _, n := range nodes {
			names = append(names, n.Name)
			id, err := db.NewID()
			if err != nil {
				return err
			}
			var provider *string
			if n.ProviderID != "" {
				provider = &n.ProviderID
			}
			var kubelet *string
			if n.KubeletVersion != "" {
				kubelet = &n.KubeletVersion
			}
			labels := n.Labels
			if labels == nil {
				// A nil map encodes as SQL NULL, which the column refuses.
				labels = map[string]string{}
			}
			taints := n.Taints
			if taints == nil {
				taints = []Taint{}
			}
			conds := n.Conditions
			if conds == nil {
				conds = []Condition{}
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO nodes (id, name, provider_id, labels, taints, capacity, allocatable, requested,
				                   conditions, schedulable, kubelet_version, synced_at, removed_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), NULL)
				ON CONFLICT (name) DO UPDATE SET
				    provider_id = EXCLUDED.provider_id, labels = EXCLUDED.labels, taints = EXCLUDED.taints,
				    capacity = EXCLUDED.capacity, allocatable = EXCLUDED.allocatable,
				    requested = EXCLUDED.requested, conditions = EXCLUDED.conditions,
				    schedulable = EXCLUDED.schedulable, kubelet_version = EXCLUDED.kubelet_version,
				    synced_at = now(), removed_at = NULL`,
				id, n.Name, provider, labels, taints, n.Capacity, n.Allocatable, n.Requested,
				conds, n.Schedulable, kubelet); err != nil {
				return fmt.Errorf("upserting node %s: %w", n.Name, err)
			}
		}
		_, err := tx.Exec(ctx, `
			UPDATE nodes SET removed_at = now()
			 WHERE removed_at IS NULL AND NOT (name = ANY($1))`, names)
		return err
	})
}

// WorkerEvent is one row of worker_events.
type WorkerEvent struct {
	OrgID        uuid.UUID
	DeploymentID uuid.UUID
	Pod          string
	Node         string
	Type         string
	Detail       map[string]any
	OccurredAt   time.Time
}

// RecordWorkerEvent appends a worker event. Append-only; never updated.
func (s *Store) RecordWorkerEvent(ctx context.Context, e WorkerEvent) error {
	id, err := db.NewID()
	if err != nil {
		return err
	}
	if e.Detail == nil {
		e.Detail = map[string]any{}
	}
	var node *string
	if e.Node != "" {
		node = &e.Node
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO worker_events (id, org_id, deployment_id, pod_name, node_name, event_type, detail, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id, e.OrgID, e.DeploymentID, e.Pod, node, e.Type, e.Detail, e.OccurredAt)
	return err
}
