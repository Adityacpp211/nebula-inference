package models

import (
	"encoding/json"
	"net"
	"time"

	"github.com/google/uuid"
)

// Conventions in this file:
//
//   - A nullable column is a pointer, so "absent" is distinguishable from "zero".
//     This matters: a deployment with 0 ready replicas and one whose readiness is
//     unknown are different situations.
//   - jsonb columns are json.RawMessage. Typed shapes are introduced by the phase
//     that consumes them rather than guessed at now; a wrong typed shape is worse
//     than a raw one because it silently drops fields on round-trip.
//   - Money is int64 micros. Sizes are int64 bytes. Units are in the field name.
//   - Each struct notes whether the table is AUTHORITATIVE, OBSERVED or
//     APPEND-ONLY, because that distinction is what keeps a control plane from
//     acting on stale copies of itself (axiom A1).

// Organization is a tenant. AUTHORITATIVE.
type Organization struct {
	ID               uuid.UUID       `json:"id"`
	Slug             string          `json:"slug"`
	Name             string          `json:"name"`
	PricingProfileID *uuid.UUID      `json:"pricing_profile_id,omitempty"`
	DefaultNamespace string          `json:"default_namespace"`
	Quota            json.RawMessage `json:"quota"`
	CreatedAt        time.Time       `json:"created_at"`
	UpdatedAt        time.Time       `json:"updated_at"`
	DeletedAt        *time.Time      `json:"deleted_at,omitempty"`
}

// User is a human identity. AUTHORITATIVE.
type User struct {
	ID    uuid.UUID `json:"id"`
	OrgID uuid.UUID `json:"org_id"`
	Email string    `json:"email"`
	Name  *string   `json:"name,omitempty"`
	Role  UserRole  `json:"role"`
	// PasswordHash is argon2id, or nil for an externally-authenticated user. It is
	// json-tagged "-" so a User can never be serialised into a response.
	PasswordHash *string    `json:"-"`
	LastLoginAt  *time.Time `json:"last_login_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	DeletedAt    *time.Time `json:"deleted_at,omitempty"`
}

// APIKey is a machine identity. AUTHORITATIVE.
//
// The plaintext key exists only in the response to its own creation and is never
// stored, so it is absent from this struct by design (ADR-0011).
type APIKey struct {
	ID     uuid.UUID  `json:"id"`
	OrgID  uuid.UUID  `json:"org_id"`
	UserID *uuid.UUID `json:"user_id,omitempty"`
	Name   string     `json:"name"`
	// Prefix is 'nbk_' plus 7 characters: safe to display, log and audit.
	Prefix string `json:"prefix"`
	// KeyHash is HMAC-SHA256 with a server-side pepper. Never serialised.
	KeyHash           []byte     `json:"-"`
	Scopes            []string   `json:"scopes"`
	Priority          Priority   `json:"priority"`
	RateLimitPolicyID *uuid.UUID `json:"rate_limit_policy_id,omitempty"`
	LastUsedAt        *time.Time `json:"last_used_at,omitempty"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
	RevokedAt         *time.Time `json:"revoked_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

// Usable reports whether the key may authenticate at the given instant.
func (k APIKey) Usable(now time.Time) bool {
	if k.RevokedAt != nil {
		return false
	}
	if k.ExpiresAt != nil && !now.Before(*k.ExpiresAt) {
		return false
	}
	return true
}

// Model is a named model family within an organization. AUTHORITATIVE.
type Model struct {
	ID          uuid.UUID  `json:"id"`
	OrgID       uuid.UUID  `json:"org_id"`
	Name        string     `json:"name"`
	Family      *string    `json:"family,omitempty"`
	Task        ModelTask  `json:"task"`
	Description *string    `json:"description,omitempty"`
	CreatedBy   *uuid.UUID `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	DeletedAt   *time.Time `json:"deleted_at,omitempty"`
}

// ModelVersion is an immutable-once-ready model artifact. AUTHORITATIVE.
type ModelVersion struct {
	ID              uuid.UUID          `json:"id"`
	ModelID         uuid.UUID          `json:"model_id"`
	Version         string             `json:"version"`
	Format          ModelFormat        `json:"format"`
	Runtime         Runtime            `json:"runtime"`
	Quantization    *string            `json:"quantization,omitempty"`
	ParameterCount  *int64             `json:"parameter_count,omitempty"`
	SizeBytes       int64              `json:"size_bytes"`
	ChecksumSHA256  []byte             `json:"checksum_sha256"`
	ContextWindow   int32              `json:"context_window"`
	ArtifactURI     string             `json:"artifact_uri"`
	HardwareProfile json.RawMessage    `json:"hardware_profile"`
	RuntimeConfig   json.RawMessage    `json:"runtime_config"`
	Status          ModelVersionStatus `json:"status"`
	FailureReason   *string            `json:"failure_reason,omitempty"`
	CreatedBy       *uuid.UUID         `json:"created_by,omitempty"`
	CreatedAt       time.Time          `json:"created_at"`
	ReadyAt         *time.Time         `json:"ready_at,omitempty"`
}

// Deployable reports whether this version may back a deployment.
func (v ModelVersion) Deployable() bool { return v.Status == VersionReady }

// ModelArtifact is one file of a possibly multi-file model. AUTHORITATIVE.
type ModelArtifact struct {
	ID             uuid.UUID `json:"id"`
	ModelVersionID uuid.UUID `json:"model_version_id"`
	Path           string    `json:"path"`
	SizeBytes      int64     `json:"size_bytes"`
	ChecksumSHA256 []byte    `json:"checksum_sha256"`
}

// Deployment is desired state plus controller-written observed status.
//
// The desired fields are AUTHORITATIVE. The Observed* fields are OBSERVED: they
// are a read convenience derived from Kubernetes, never a decision input, and the
// database grants stop the control plane from writing them.
type Deployment struct {
	ID               uuid.UUID       `json:"id"`
	OrgID            uuid.UUID       `json:"org_id"`
	ModelVersionID   uuid.UUID       `json:"model_version_id"`
	Name             string          `json:"name"`
	Namespace        string          `json:"namespace"`
	DesiredReplicas  int32           `json:"desired_replicas"`
	MinReplicas      int32           `json:"min_replicas"`
	MaxReplicas      int32           `json:"max_replicas"`
	Resources        json.RawMessage `json:"resources"`
	RuntimeOverrides json.RawMessage `json:"runtime_overrides"`
	Autoscaling      json.RawMessage `json:"autoscaling"`
	QueueConfig      json.RawMessage `json:"queue_config"`
	RoutingPolicyID  *uuid.UUID      `json:"routing_policy_id,omitempty"`

	// Generation increments on SPEC changes only, never on a status write. That
	// single rule is what makes Generation != ObservedGeneration a trustworthy
	// "work to do" predicate.
	Generation      int64 `json:"generation"`
	CurrentRevision int32 `json:"current_revision"`

	ObservedGeneration int64           `json:"observed_generation"`
	State              DeploymentState `json:"state"`
	// StateEnteredAt is maintained by a database trigger, so "stuck in starting
	// for 20 minutes" is a query rather than a guess.
	StateEnteredAt  time.Time       `json:"state_entered_at"`
	StateReason     *string         `json:"state_reason,omitempty"`
	StateMessage    *string         `json:"state_message,omitempty"`
	ReadyReplicas   int32           `json:"ready_replicas"`
	UpdatedReplicas int32           `json:"updated_replicas"`
	Conditions      json.RawMessage `json:"conditions"`
	LastError       *string         `json:"last_error,omitempty"`
	LastSyncedAt    *time.Time      `json:"last_synced_at,omitempty"`

	CreatedBy *uuid.UUID `json:"created_by,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

// NeedsReconcile reports whether the controller has work to do.
func (d Deployment) NeedsReconcile() bool { return d.Generation != d.ObservedGeneration }

// Converged reports whether observed state matches desired state.
func (d Deployment) Converged() bool {
	return !d.NeedsReconcile() && d.ReadyReplicas == d.DesiredReplicas
}

// DeploymentRevision is an immutable spec snapshot. APPEND-ONLY.
type DeploymentRevision struct {
	ID             uuid.UUID       `json:"id"`
	DeploymentID   uuid.UUID       `json:"deployment_id"`
	Revision       int32           `json:"revision"`
	ModelVersionID uuid.UUID       `json:"model_version_id"`
	Spec           json.RawMessage `json:"spec"`
	SpecHash       []byte          `json:"spec_hash"`
	Reason         RevisionReason  `json:"reason"`
	CreatedBy      *uuid.UUID      `json:"created_by,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// DeploymentStateTransition is one recorded state change. APPEND-ONLY, written by
// a database trigger so a transition cannot happen without being recorded —
// including one made by hand in psql during an incident.
type DeploymentStateTransition struct {
	ID           uuid.UUID        `json:"id"`
	OrgID        uuid.UUID        `json:"org_id"`
	DeploymentID uuid.UUID        `json:"deployment_id"`
	FromState    *DeploymentState `json:"from_state,omitempty"` // nil for the creation row
	ToState      DeploymentState  `json:"to_state"`
	Reason       *string          `json:"reason,omitempty"`
	Message      *string          `json:"message,omitempty"`
	Generation   int64            `json:"generation"`
	ActorType    ActorType        `json:"actor_type"`
	ActorID      *uuid.UUID       `json:"actor_id,omitempty"`
	OccurredAt   time.Time        `json:"occurred_at"`
}

// Route is the stable public identity clients address. AUTHORITATIVE.
type Route struct {
	ID              uuid.UUID  `json:"id"`
	OrgID           uuid.UUID  `json:"org_id"`
	ModelName       string     `json:"model_name"`
	RoutingPolicyID *uuid.UUID `json:"routing_policy_id,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// RouteTarget is one weighted destination of a route. AUTHORITATIVE.
//
// Weights across a route must total 100, enforced by a deferred constraint
// trigger: a route summing to 90 would silently drop 10% of traffic.
type RouteTarget struct {
	ID           uuid.UUID `json:"id"`
	RouteID      uuid.UUID `json:"route_id"`
	DeploymentID uuid.UUID `json:"deployment_id"`
	Weight       int32     `json:"weight"`
	IsBaseline   bool      `json:"is_baseline"`
	Label        *string   `json:"label,omitempty"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// RoutingPolicy names a strategy from the packages/routing registry.
// AUTHORITATIVE. A nil OrgID means a built-in shared policy.
type RoutingPolicy struct {
	ID        uuid.UUID       `json:"id"`
	OrgID     *uuid.UUID      `json:"org_id,omitempty"`
	Name      string          `json:"name"`
	Strategy  string          `json:"strategy"`
	Config    json.RawMessage `json:"config"`
	IsDefault bool            `json:"is_default"`
	CreatedAt time.Time       `json:"created_at"`
}

// RateLimitPolicy bounds a key's consumption. AUTHORITATIVE. A nil field means
// that dimension is unlimited, which is why the seeded default sets all of them.
type RateLimitPolicy struct {
	ID                uuid.UUID  `json:"id"`
	OrgID             *uuid.UUID `json:"org_id,omitempty"`
	Name              string     `json:"name"`
	RequestsPerMinute *int32     `json:"requests_per_minute,omitempty"`
	TokensPerMinute   *int32     `json:"tokens_per_minute,omitempty"`
	MaxConcurrency    *int32     `json:"max_concurrency,omitempty"`
	MaxQueueDepth     *int32     `json:"max_queue_depth,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
}

// CanaryRollout is a progressive-delivery state machine. AUTHORITATIVE.
type CanaryRollout struct {
	ID                   uuid.UUID       `json:"id"`
	OrgID                uuid.UUID       `json:"org_id"`
	RouteID              uuid.UUID       `json:"route_id"`
	BaselineDeploymentID uuid.UUID       `json:"baseline_deployment_id"`
	CanaryDeploymentID   uuid.UUID       `json:"canary_deployment_id"`
	Steps                json.RawMessage `json:"steps"`
	CurrentStep          int32           `json:"current_step"`
	Analysis             json.RawMessage `json:"analysis"`
	State                RolloutState    `json:"state"`
	LastAnalysis         json.RawMessage `json:"last_analysis,omitempty"`
	AbortReason          *string         `json:"abort_reason,omitempty"`
	CreatedBy            *uuid.UUID      `json:"created_by,omitempty"`
	StartedAt            *time.Time      `json:"started_at,omitempty"`
	StepEnteredAt        *time.Time      `json:"step_entered_at,omitempty"`
	FinishedAt           *time.Time      `json:"finished_at,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
}

// Experiment is a fixed traffic split for comparison. AUTHORITATIVE.
type Experiment struct {
	ID         uuid.UUID       `json:"id"`
	OrgID      uuid.UUID       `json:"org_id"`
	RouteID    uuid.UUID       `json:"route_id"`
	Name       string          `json:"name"`
	Hypothesis *string         `json:"hypothesis,omitempty"`
	Bucketing  Bucketing       `json:"bucketing"`
	State      ExperimentState `json:"state"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	StoppedAt  *time.Time      `json:"stopped_at,omitempty"`
	Conclusion *string         `json:"conclusion,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

// PricingProfile is an immutable, versioned price list. IMMUTABLE.
//
// All amounts are integer micros of Currency: no floating-point money anywhere
// (ADR-0018).
type PricingProfile struct {
	ID                    uuid.UUID       `json:"id"`
	OrgID                 *uuid.UUID      `json:"org_id,omitempty"`
	Name                  string          `json:"name"`
	Version               int32           `json:"version"`
	Currency              string          `json:"currency"`
	CPUCoreHourMicros     int64           `json:"cpu_core_hour_micros"`
	MemoryGiBHourMicros   int64           `json:"memory_gib_hour_micros"`
	GPUHourMicros         json.RawMessage `json:"gpu_hour_micros"`
	StorageGiBMonthMicros int64           `json:"storage_gib_month_micros"`
	EgressGiBMicros       int64           `json:"egress_gib_micros"`
	MarkupBasisPoints     int32           `json:"markup_basis_points"`
	EffectiveFrom         time.Time       `json:"effective_from"`
	CreatedAt             time.Time       `json:"created_at"`
}

// Zeroed reports whether this profile prices everything at zero, which is the
// seeded default. Cost responses must say so rather than reporting a confident 0.
func (p PricingProfile) Zeroed() bool {
	return p.CPUCoreHourMicros == 0 && p.MemoryGiBHourMicros == 0 &&
		p.StorageGiBMonthMicros == 0 && p.EgressGiBMicros == 0
}

// Node is a cached Kubernetes Node. OBSERVED: rebuildable, never a decision input
// that the live informer could answer.
type Node struct {
	ID             uuid.UUID       `json:"id"`
	Name           string          `json:"name"`
	ProviderID     *string         `json:"provider_id,omitempty"`
	Labels         json.RawMessage `json:"labels"`
	Taints         json.RawMessage `json:"taints"`
	Capacity       json.RawMessage `json:"capacity"`
	Allocatable    json.RawMessage `json:"allocatable"`
	Requested      json.RawMessage `json:"requested"`
	Conditions     json.RawMessage `json:"conditions"`
	Schedulable    bool            `json:"schedulable"`
	KubeletVersion *string         `json:"kubelet_version,omitempty"`
	FirstSeenAt    time.Time       `json:"first_seen_at"`
	SyncedAt       time.Time       `json:"synced_at"`
	RemovedAt      *time.Time      `json:"removed_at,omitempty"`
}

// Stale reports whether the cache entry is older than maxAge and should be
// refreshed rather than trusted.
func (n Node) Stale(now time.Time, maxAge time.Duration) bool {
	return now.Sub(n.SyncedAt) > maxAge
}

// WorkerEvent is a pod lifecycle fact. APPEND-ONLY.
//
// This is what makes "why was this degraded yesterday" answerable: Kubernetes
// Events expire in about an hour.
type WorkerEvent struct {
	ID           uuid.UUID       `json:"id"`
	OrgID        uuid.UUID       `json:"org_id"`
	DeploymentID uuid.UUID       `json:"deployment_id"`
	PodName      string          `json:"pod_name"`
	NodeName     *string         `json:"node_name,omitempty"`
	EventType    WorkerEventType `json:"event_type"`
	Detail       json.RawMessage `json:"detail"`
	OccurredAt   time.Time       `json:"occurred_at"`
}

// AutoscalingEvent records one scaling decision, including a decision not to act.
// APPEND-ONLY.
type AutoscalingEvent struct {
	ID           uuid.UUID          `json:"id"`
	OrgID        uuid.UUID          `json:"org_id"`
	DeploymentID uuid.UUID          `json:"deployment_id"`
	FromReplicas int32              `json:"from_replicas"`
	ToReplicas   int32              `json:"to_replicas"`
	Direction    AutoscaleDirection `json:"direction"`
	Signals      json.RawMessage    `json:"signals"`
	Decision     AutoscaleDecision  `json:"decision"`
	Reason       string             `json:"reason"`
	CreatedAt    time.Time          `json:"created_at"`
}

// Request is one inference request. APPEND-ONLY, partitioned, sampled.
//
// Has no foreign keys by design: FK checks cost throughput on a high-rate insert
// path, and the referenced columns are historical snapshots that must survive the
// deletion of what they reference (ADR-0017).
type Request struct {
	ID                  uuid.UUID      `json:"id"`
	OrgID               uuid.UUID      `json:"org_id"`
	CreatedAt           time.Time      `json:"created_at"`
	RouteID             *uuid.UUID     `json:"route_id,omitempty"`
	DeploymentID        *uuid.UUID     `json:"deployment_id,omitempty"`
	ModelVersionID      *uuid.UUID     `json:"model_version_id,omitempty"`
	APIKeyID            *uuid.UUID     `json:"api_key_id,omitempty"`
	ExperimentID        *uuid.UUID     `json:"experiment_id,omitempty"`
	Variant             *string        `json:"variant,omitempty"`
	Endpoint            string         `json:"endpoint"`
	Priority            Priority       `json:"priority"`
	StatusCode          int16          `json:"status_code"`
	ErrorClass          *string        `json:"error_class,omitempty"`
	Outcome             RequestOutcome `json:"outcome"`
	Streamed            bool           `json:"streamed"`
	QueueWaitMS         int32          `json:"queue_wait_ms"`
	TTFTMS              *int32         `json:"ttft_ms,omitempty"`
	DurationMS          int32          `json:"duration_ms"`
	ComputeMS           int32          `json:"compute_ms"`
	Attempts            int16          `json:"attempts"`
	PromptTokens        int32          `json:"prompt_tokens"`
	CompletionTokens    int32          `json:"completion_tokens"`
	WorkerPod           *string        `json:"worker_pod,omitempty"`
	NodeName            *string        `json:"node_name,omitempty"`
	TraceID             *string        `json:"trace_id,omitempty"`
	SampleRate          float32        `json:"sample_rate"`
	EstimatedCostMicros int64          `json:"estimated_cost_micros"`
}

// TotalTokens is prompt plus completion.
func (r Request) TotalTokens() int32 { return r.PromptTokens + r.CompletionTokens }

// UsageRecord is an hourly rollup. APPEND-ONLY, upserted idempotently, retained
// indefinitely. This, not Request, is what the cost and usage APIs read.
type UsageRecord struct {
	ID                  uuid.UUID  `json:"id"`
	OrgID               uuid.UUID  `json:"org_id"`
	BucketHour          time.Time  `json:"bucket_hour"`
	DeploymentID        *uuid.UUID `json:"deployment_id,omitempty"`
	ModelVersionID      *uuid.UUID `json:"model_version_id,omitempty"`
	APIKeyID            *uuid.UUID `json:"api_key_id,omitempty"`
	RequestCount        int64      `json:"request_count"`
	ErrorCount          int64      `json:"error_count"`
	PromptTokens        int64      `json:"prompt_tokens"`
	CompletionTokens    int64      `json:"completion_tokens"`
	ComputeSeconds      string     `json:"compute_seconds"` // numeric(14,3), kept exact as text
	GPUSeconds          string     `json:"gpu_seconds"`
	SampleRate          float32    `json:"sample_rate"`
	PricingProfileID    uuid.UUID  `json:"pricing_profile_id"`
	EstimatedCostMicros int64      `json:"estimated_cost_micros"`
	ComputedAt          time.Time  `json:"computed_at"`
}

// AuditLog is one recorded state change. APPEND-ONLY, enforced by grants as well
// as a trigger.
type AuditLog struct {
	ID           uuid.UUID       `json:"id"`
	OrgID        uuid.UUID       `json:"org_id"`
	CreatedAt    time.Time       `json:"created_at"`
	ActorType    ActorType       `json:"actor_type"`
	ActorID      *uuid.UUID      `json:"actor_id,omitempty"`
	ActorLabel   *string         `json:"actor_label,omitempty"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   *uuid.UUID      `json:"resource_id,omitempty"`
	Before       json.RawMessage `json:"before,omitempty"`
	After        json.RawMessage `json:"after,omitempty"`
	RequestID    *uuid.UUID      `json:"request_id,omitempty"`
	IP           *net.IP         `json:"ip,omitempty"`
	UserAgent    *string         `json:"user_agent,omitempty"`
}

// SchemaMigration is one row of the migration bookkeeping table.
type SchemaMigration struct {
	Version     int64     `json:"version"`
	Name        string    `json:"name"`
	Checksum    []byte    `json:"checksum"`
	AppliedAt   time.Time `json:"applied_at"`
	ExecutionMS int32     `json:"execution_ms"`
}
