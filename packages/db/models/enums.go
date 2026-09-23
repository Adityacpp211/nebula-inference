// Package models holds the Go representation of NEBULA's database schema.
//
// These are hand-written row types, not an ORM and not generated code. Phase 1
// defines the types and their enum vocabularies so the schema has one
// authoritative Go mirror; the queries that use them arrive with the phase that
// needs them. Query code will be generated from reviewed SQL by sqlc (ADR-0019),
// which produces structs compatible with these.
//
// Every enum here mirrors either a PostgreSQL enum type or a CHECK constraint in
// migrations/. When the two disagree the database wins, and a test asserts they
// do not disagree.
package models

// UserRole mirrors the user_role PostgreSQL enum.
type UserRole string

// User roles, in descending order of privilege.
const (
	RoleOwner     UserRole = "owner"     // billing and organization deletion
	RoleAdmin     UserRole = "admin"     // all resources, all API keys, pricing, policies
	RoleDeveloper UserRole = "developer" // deploy, scale, rollback; own keys only
	RoleViewer    UserRole = "viewer"    // read resources, metrics, usage
)

// Valid reports whether r is a known role.
func (r UserRole) Valid() bool {
	switch r {
	case RoleOwner, RoleAdmin, RoleDeveloper, RoleViewer:
		return true
	}
	return false
}

// UserRoles lists every role.
func UserRoles() []UserRole { return []UserRole{RoleOwner, RoleAdmin, RoleDeveloper, RoleViewer} }

// ModelVersionStatus mirrors the model_version_status PostgreSQL enum.
//
// The lifecycle is uploading -> verifying -> ready, with failure possible from
// either of the first two. A version becomes IMMUTABLE at ready, and from there
// the only legal transition is to archived (ADR-0010).
type ModelVersionStatus string

// Model version statuses.
const (
	VersionUploading ModelVersionStatus = "uploading"
	VersionVerifying ModelVersionStatus = "verifying"
	VersionReady     ModelVersionStatus = "ready"
	VersionFailed    ModelVersionStatus = "failed"
	VersionArchived  ModelVersionStatus = "archived"
)

// Valid reports whether s is a known status.
func (s ModelVersionStatus) Valid() bool {
	switch s {
	case VersionUploading, VersionVerifying, VersionReady, VersionFailed, VersionArchived:
		return true
	}
	return false
}

// CanTransitionTo encodes the legal state machine. The database trigger enforces
// the immutability half of this; keeping the rule here too means the API can
// reject an illegal transition with a clear message rather than surfacing a
// constraint violation.
func (s ModelVersionStatus) CanTransitionTo(next ModelVersionStatus) bool {
	switch s {
	case VersionUploading:
		return next == VersionVerifying || next == VersionFailed
	case VersionVerifying:
		return next == VersionReady || next == VersionFailed
	case VersionReady:
		return next == VersionArchived
	case VersionFailed, VersionArchived:
		return false
	}
	return false
}

// Immutable reports whether the version's defining fields are frozen.
func (s ModelVersionStatus) Immutable() bool {
	return s == VersionReady || s == VersionArchived
}

// DeploymentState mirrors the deployment_state PostgreSQL enum.
//
// The transition graph, the reason vocabulary and the predicates that depend on
// more than one state live in packages/lifecycle, which imports this package.
// Only the values and the two single-state predicates are here, so the mirror of
// the database enum stays dependency-free.
type DeploymentState string

// Deployment states. See packages/lifecycle for what may follow what, and
// docs/architecture-decisions/0027-deployment-state-machine.md for why these
// eight.
const (
	DeploymentPending      DeploymentState = "pending"
	DeploymentProvisioning DeploymentState = "provisioning"
	DeploymentStarting     DeploymentState = "starting"
	DeploymentReady        DeploymentState = "ready"
	DeploymentDegraded     DeploymentState = "degraded"
	DeploymentFailed       DeploymentState = "failed"
	DeploymentStopping     DeploymentState = "stopping"
	DeploymentStopped      DeploymentState = "stopped"
)

// DeploymentStates lists every state, in lifecycle order.
func DeploymentStates() []DeploymentState {
	return []DeploymentState{
		DeploymentPending, DeploymentProvisioning, DeploymentStarting, DeploymentReady,
		DeploymentDegraded, DeploymentFailed, DeploymentStopping, DeploymentStopped,
	}
}

// Valid reports whether s is a known state.
func (s DeploymentState) Valid() bool {
	for _, v := range DeploymentStates() {
		if v == s {
			return true
		}
	}
	return false
}

// Serving reports whether a deployment in this state may receive traffic.
//
// Degraded still serves: some replicas are healthy, and removing the whole
// deployment from rotation because one pod died would turn a partial failure into
// a total one.
func (s DeploymentState) Serving() bool {
	return s == DeploymentReady || s == DeploymentDegraded
}

// Resting reports whether nothing is expected to change without an operator or a
// new revision. A deployment may only be soft-deleted from a resting state, which
// is also a CHECK constraint on the table.
func (s DeploymentState) Resting() bool {
	return s == DeploymentPending || s == DeploymentFailed || s == DeploymentStopped
}

// RolloutState mirrors the rollout_state PostgreSQL enum.
type RolloutState string

// Rollout states.
const (
	RolloutPending     RolloutState = "pending"
	RolloutProgressing RolloutState = "progressing"
	RolloutPaused      RolloutState = "paused"
	RolloutPromoted    RolloutState = "promoted"
	RolloutAborted     RolloutState = "aborted"
	RolloutRolledBack  RolloutState = "rolled_back"
	RolloutFailed      RolloutState = "failed"
)

// Valid reports whether s is a known state.
func (s RolloutState) Valid() bool {
	switch s {
	case RolloutPending, RolloutProgressing, RolloutPaused, RolloutPromoted,
		RolloutAborted, RolloutRolledBack, RolloutFailed:
		return true
	}
	return false
}

// Active reports whether the rollout holds the per-route exclusivity lock. The
// database enforces one active rollout per route with a partial unique index over
// exactly this set.
func (s RolloutState) Active() bool {
	return s == RolloutPending || s == RolloutProgressing || s == RolloutPaused
}

// ExperimentState mirrors the experiment_state PostgreSQL enum.
type ExperimentState string

// Experiment states.
const (
	ExperimentDraft     ExperimentState = "draft"
	ExperimentRunning   ExperimentState = "running"
	ExperimentStopped   ExperimentState = "stopped"
	ExperimentConcluded ExperimentState = "concluded"
)

// Valid reports whether s is a known state.
func (s ExperimentState) Valid() bool {
	switch s {
	case ExperimentDraft, ExperimentRunning, ExperimentStopped, ExperimentConcluded:
		return true
	}
	return false
}

// Priority mirrors the priority CHECK constraint on api_keys and requests.
//
// Priority is a property of the API key, never of the request body, so a caller
// cannot promote itself.
type Priority string

// Request priorities.
const (
	PriorityLow    Priority = "LOW"
	PriorityNormal Priority = "NORMAL"
	PriorityHigh   Priority = "HIGH"
)

// Valid reports whether p is a known priority.
func (p Priority) Valid() bool {
	switch p {
	case PriorityLow, PriorityNormal, PriorityHigh:
		return true
	}
	return false
}

// Rank orders priorities for queue comparison; higher is served first.
func (p Priority) Rank() int {
	switch p {
	case PriorityHigh:
		return 2
	case PriorityNormal:
		return 1
	case PriorityLow:
		return 0
	}
	return 0
}

// ModelFormat mirrors the format CHECK constraint on model_versions.
type ModelFormat string

// Model artifact formats.
const (
	FormatGGUF        ModelFormat = "gguf"
	FormatSafetensors ModelFormat = "safetensors"
	// FormatMock is a declared development stub, refused when NEBULA_ENV=production.
	FormatMock ModelFormat = "mock"
)

// Valid reports whether f is a known format.
func (f ModelFormat) Valid() bool {
	switch f {
	case FormatGGUF, FormatSafetensors, FormatMock:
		return true
	}
	return false
}

// Runtime mirrors the runtime CHECK constraint on model_versions.
type Runtime string

// Inference runtimes. Implemented in the phase named against each.
const (
	RuntimeLlamaCPP Runtime = "llamacpp" // Phase 3
	RuntimeVLLM     Runtime = "vllm"     // post-v1, needs a GPU
	// RuntimeMock is a declared development stub (axiom A9).
	RuntimeMock Runtime = "mock" // Phase 3
)

// Valid reports whether r is a known runtime.
func (r Runtime) Valid() bool {
	switch r {
	case RuntimeLlamaCPP, RuntimeVLLM, RuntimeMock:
		return true
	}
	return false
}

// DevelopmentOnly reports whether this runtime must be refused in production.
func (r Runtime) DevelopmentOnly() bool { return r == RuntimeMock }

// ModelTask mirrors the task CHECK constraint on models.
type ModelTask string

// Model tasks.
const (
	TaskChat       ModelTask = "chat"
	TaskCompletion ModelTask = "completion"
	TaskEmbedding  ModelTask = "embedding"
)

// Valid reports whether t is a known task.
func (t ModelTask) Valid() bool {
	switch t {
	case TaskChat, TaskCompletion, TaskEmbedding:
		return true
	}
	return false
}

// RevisionReason mirrors the reason CHECK constraint on deployment_revisions.
type RevisionReason string

// Reasons a deployment revision was created.
const (
	ReasonCreate   RevisionReason = "create"
	ReasonUpdate   RevisionReason = "update"
	ReasonScale    RevisionReason = "scale"
	ReasonRollback RevisionReason = "rollback"
	ReasonRollout  RevisionReason = "rollout"
)

// Valid reports whether r is a known reason.
func (r RevisionReason) Valid() bool {
	switch r {
	case ReasonCreate, ReasonUpdate, ReasonScale, ReasonRollback, ReasonRollout:
		return true
	}
	return false
}

// WorkerEventType mirrors the event_type CHECK constraint on worker_events.
type WorkerEventType string

// Worker lifecycle events.
const (
	WorkerScheduled       WorkerEventType = "scheduled"
	WorkerArtifactPulling WorkerEventType = "artifact_pulling"
	WorkerArtifactCached  WorkerEventType = "artifact_cached"
	WorkerModelLoading    WorkerEventType = "model_loading"
	WorkerModelLoaded     WorkerEventType = "model_loaded"
	WorkerModelLoadFailed WorkerEventType = "model_load_failed"
	WorkerReady           WorkerEventType = "ready"
	WorkerUnhealthy       WorkerEventType = "unhealthy"
	WorkerEngineCrashed   WorkerEventType = "engine_crashed"
	WorkerCrashed         WorkerEventType = "crashed"
	WorkerOOMKilled       WorkerEventType = "oom_killed"
	WorkerEvicted         WorkerEventType = "evicted"
	WorkerDraining        WorkerEventType = "draining"
	WorkerTerminated      WorkerEventType = "terminated"
)

// WorkerEventTypes lists every worker event type, in rough lifecycle order.
func WorkerEventTypes() []WorkerEventType {
	return []WorkerEventType{
		WorkerScheduled, WorkerArtifactPulling, WorkerArtifactCached, WorkerModelLoading,
		WorkerModelLoaded, WorkerModelLoadFailed, WorkerReady, WorkerUnhealthy,
		WorkerEngineCrashed, WorkerCrashed, WorkerOOMKilled, WorkerEvicted,
		WorkerDraining, WorkerTerminated,
	}
}

// Valid reports whether t is a known event type.
func (t WorkerEventType) Valid() bool {
	for _, v := range WorkerEventTypes() {
		if v == t {
			return true
		}
	}
	return false
}

// Fatal reports whether the event means this replica will not recover without
// intervention. A rollout aborts on a fatal event rather than waiting out its
// analysis window.
func (t WorkerEventType) Fatal() bool {
	return t == WorkerModelLoadFailed
}

// AutoscaleDirection mirrors the direction CHECK constraint.
type AutoscaleDirection string

// Autoscaling directions. "hold" exists so a decision NOT to change replica count
// is still recorded, which is what makes "why didn't it scale?" answerable.
const (
	ScaleUp   AutoscaleDirection = "up"
	ScaleDown AutoscaleDirection = "down"
	ScaleHold AutoscaleDirection = "hold"
)

// Valid reports whether d is a known direction.
func (d AutoscaleDirection) Valid() bool {
	switch d {
	case ScaleUp, ScaleDown, ScaleHold:
		return true
	}
	return false
}

// AutoscaleDecision mirrors the decision CHECK constraint on autoscaling_events.
type AutoscaleDecision string

// Autoscaling decisions, including every reason an action was suppressed.
const (
	DecisionScaled                  AutoscaleDecision = "scaled"
	DecisionSuppressedCooldown      AutoscaleDecision = "suppressed_cooldown"
	DecisionSuppressedStabilization AutoscaleDecision = "suppressed_stabilization"
	DecisionSuppressedRollout       AutoscaleDecision = "suppressed_rollout"
	DecisionSuppressedNoSignal      AutoscaleDecision = "suppressed_no_signal"
	DecisionClampedMax              AutoscaleDecision = "clamped_max"
	DecisionClampedMin              AutoscaleDecision = "clamped_min"
	DecisionDisabled                AutoscaleDecision = "disabled"
)

// AutoscaleDecisions lists every decision.
func AutoscaleDecisions() []AutoscaleDecision {
	return []AutoscaleDecision{
		DecisionScaled, DecisionSuppressedCooldown, DecisionSuppressedStabilization,
		DecisionSuppressedRollout, DecisionSuppressedNoSignal, DecisionClampedMax,
		DecisionClampedMin, DecisionDisabled,
	}
}

// Valid reports whether d is a known decision.
func (d AutoscaleDecision) Valid() bool {
	for _, v := range AutoscaleDecisions() {
		if v == d {
			return true
		}
	}
	return false
}

// RequestOutcome mirrors the outcome CHECK constraint on requests.
//
// Deliberately separate from the HTTP status code: a cancelled stream that
// produced 60 tokens is billable work AND an availability non-event, and one
// field cannot carry both facts.
type RequestOutcome string

// Request outcomes.
const (
	OutcomeCompleted         RequestOutcome = "completed"
	OutcomeClientCancelled   RequestOutcome = "client_cancelled"
	OutcomeStreamInterrupted RequestOutcome = "stream_interrupted"
	OutcomeDeadlineExceeded  RequestOutcome = "deadline_exceeded"
	OutcomeQueueTimeout      RequestOutcome = "queue_timeout"
	OutcomeRejected          RequestOutcome = "rejected"
)

// RequestOutcomes lists every outcome.
func RequestOutcomes() []RequestOutcome {
	return []RequestOutcome{
		OutcomeCompleted, OutcomeClientCancelled, OutcomeStreamInterrupted,
		OutcomeDeadlineExceeded, OutcomeQueueTimeout, OutcomeRejected,
	}
}

// Valid reports whether o is a known outcome.
func (o RequestOutcome) Valid() bool {
	for _, v := range RequestOutcomes() {
		if v == o {
			return true
		}
	}
	return false
}

// CountsAgainstAvailability reports whether this outcome should count as an error
// in the availability SLO. A client hanging up has not experienced an outage, and
// counting it as one trains operators to ignore the alert
// (docs/observability.md §7.1).
func (o RequestOutcome) CountsAgainstAvailability() bool {
	switch o {
	case OutcomeClientCancelled:
		return false
	default:
		return o != OutcomeCompleted
	}
}

// Bucketing mirrors the bucketing CHECK constraint on experiments.
type Bucketing string

// Experiment bucketing strategies.
const (
	BucketPerRequest Bucketing = "request"
	BucketStickyUser Bucketing = "sticky_user"
	BucketStickyOrg  Bucketing = "sticky_org"
)

// Valid reports whether b is a known bucketing strategy.
func (b Bucketing) Valid() bool {
	switch b {
	case BucketPerRequest, BucketStickyUser, BucketStickyOrg:
		return true
	}
	return false
}

// ActorType mirrors the actor_type CHECK constraint on audit_logs.
type ActorType string

// Audit actor types.
const (
	ActorUser   ActorType = "user"
	ActorAPIKey ActorType = "api_key"
	ActorSystem ActorType = "system"
)

// Valid reports whether a is a known actor type.
func (a ActorType) Valid() bool {
	switch a {
	case ActorUser, ActorAPIKey, ActorSystem:
		return true
	}
	return false
}
