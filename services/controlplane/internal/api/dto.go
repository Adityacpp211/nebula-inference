package api

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
)

// Wire types live here rather than being the row structs, for one reason: a row
// type serialised directly is a promise that the schema is the API. Renaming a
// column would then be a breaking API change, and adding one would leak it. The
// mapping functions below are the seam that makes those independent.
//
// TODO(NEB-131): these types are hand-written in Phase 2 and become generated from
// packages/api/openapi.yaml (ADR-0018). A drift test asserts the spec and the
// mounted routes agree in the meantime.

// ---------------------------------------------------------------------------
// identity
// ---------------------------------------------------------------------------

type meResponse struct {
	OrgID            uuid.UUID `json:"org_id"`
	OrgSlug          string    `json:"org_slug"`
	OrgName          string    `json:"org_name"`
	DefaultNamespace string    `json:"default_namespace"`
	ActorType        string    `json:"actor_type"`
	ActorID          uuid.UUID `json:"actor_id"`
	ActorLabel       string    `json:"actor_label"`
	Role             string    `json:"role,omitempty"`
	Scopes           []string  `json:"scopes"`
	Priority         string    `json:"priority"`
}

type userResponse struct {
	ID          uuid.UUID  `json:"id"`
	Email       string     `json:"email"`
	Name        *string    `json:"name"`
	Role        string     `json:"role"`
	LastLoginAt *time.Time `json:"last_login_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func newUserResponse(u *models.User) userResponse {
	return userResponse{
		ID: u.ID, Email: u.Email, Name: u.Name, Role: string(u.Role),
		LastLoginAt: u.LastLoginAt, CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt,
	}
}

type createUserRequest struct {
	Email string  `json:"email"`
	Name  *string `json:"name"`
	Role  string  `json:"role"`
	// Password is optional: a user may be created for an external identity
	// provider and never hold one. It is write-only and never echoed.
	Password string `json:"password"`
}

type updateUserRequest struct {
	Role string `json:"role"`
}

type apiKeyResponse struct {
	ID         uuid.UUID  `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	UserID     *uuid.UUID `json:"user_id"`
	Scopes     []string   `json:"scopes"`
	Priority   string     `json:"priority"`
	LastUsedAt *time.Time `json:"last_used_at"`
	ExpiresAt  *time.Time `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	CreatedAt  time.Time  `json:"created_at"`
}

func newAPIKeyResponse(k *models.APIKey) apiKeyResponse {
	return apiKeyResponse{
		ID: k.ID, Name: k.Name, Prefix: k.Prefix, UserID: k.UserID,
		Scopes: k.Scopes, Priority: string(k.Priority), LastUsedAt: k.LastUsedAt,
		ExpiresAt: k.ExpiresAt, RevokedAt: k.RevokedAt, CreatedAt: k.CreatedAt,
	}
}

// createdAPIKeyResponse is the only response that ever carries a plaintext key.
//
// The field is named and documented as one-time because that is the whole
// contract: the server stores a keyed hash and cannot reproduce the secret, so a
// caller who loses it must create another key (ADR-0011).
type createdAPIKeyResponse struct {
	apiKeyResponse
	Key     string `json:"key"`
	KeyNote string `json:"key_note"`
}

type createAPIKeyRequest struct {
	Name      string     `json:"name"`
	UserID    *uuid.UUID `json:"user_id"`
	Scopes    []string   `json:"scopes"`
	Priority  string     `json:"priority"`
	ExpiresAt *time.Time `json:"expires_at"`
}

// ---------------------------------------------------------------------------
// registry
// ---------------------------------------------------------------------------

type modelResponse struct {
	ID          uuid.UUID  `json:"id"`
	Name        string     `json:"name"`
	Family      *string    `json:"family"`
	Task        string     `json:"task"`
	Description *string    `json:"description"`
	CreatedBy   *uuid.UUID `json:"created_by"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func newModelResponse(m *models.Model) modelResponse {
	return modelResponse{
		ID: m.ID, Name: m.Name, Family: m.Family, Task: string(m.Task),
		Description: m.Description, CreatedBy: m.CreatedBy,
		CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt,
	}
}

type createModelRequest struct {
	Name        string  `json:"name"`
	Task        string  `json:"task"`
	Family      *string `json:"family"`
	Description *string `json:"description"`
}

// updateModelRequest carries only the mutable metadata.
//
// Name is absent deliberately: it appears in Kubernetes object names and in every
// audit record referencing the model, so renaming it would make history unreadable.
// A model that needs a different name is a new model.
type updateModelRequest struct {
	Family      *string `json:"family"`
	Description *string `json:"description"`
}

type versionResponse struct {
	ID              uuid.UUID       `json:"id"`
	ModelID         uuid.UUID       `json:"model_id"`
	Version         string          `json:"version"`
	Format          string          `json:"format"`
	Runtime         string          `json:"runtime"`
	Quantization    *string         `json:"quantization"`
	ParameterCount  *int64          `json:"parameter_count"`
	SizeBytes       int64           `json:"size_bytes"`
	ChecksumSHA256  string          `json:"checksum_sha256"`
	ContextWindow   int32           `json:"context_window"`
	ArtifactURI     string          `json:"artifact_uri"`
	HardwareProfile json.RawMessage `json:"hardware_profile"`
	RuntimeConfig   json.RawMessage `json:"runtime_config"`
	Status          string          `json:"status"`
	FailureReason   *string         `json:"failure_reason"`
	Immutable       bool            `json:"immutable"`
	CreatedBy       *uuid.UUID      `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
	ReadyAt         *time.Time      `json:"ready_at"`
}

func newVersionResponse(v *models.ModelVersion) versionResponse {
	return versionResponse{
		ID: v.ID, ModelID: v.ModelID, Version: v.Version, Format: string(v.Format),
		Runtime: string(v.Runtime), Quantization: v.Quantization,
		ParameterCount: v.ParameterCount, SizeBytes: v.SizeBytes,
		ChecksumSHA256: hexString(v.ChecksumSHA256),
		ContextWindow:  v.ContextWindow, ArtifactURI: v.ArtifactURI,
		HardwareProfile: v.HardwareProfile, RuntimeConfig: v.RuntimeConfig,
		Status: string(v.Status), FailureReason: v.FailureReason,
		Immutable: v.Status.Immutable(), CreatedBy: v.CreatedBy,
		CreatedAt: v.CreatedAt, ReadyAt: v.ReadyAt,
	}
}

type createVersionRequest struct {
	Version         string          `json:"version"`
	Format          string          `json:"format"`
	Runtime         string          `json:"runtime"`
	ArtifactURI     string          `json:"artifact_uri"`
	Quantization    *string         `json:"quantization"`
	ParameterCount  *int64          `json:"parameter_count"`
	SizeBytes       *int64          `json:"size_bytes"`
	ChecksumSHA256  *string         `json:"checksum_sha256"`
	ContextWindow   *int32          `json:"context_window"`
	HardwareProfile json.RawMessage `json:"hardware_profile"`
	RuntimeConfig   json.RawMessage `json:"runtime_config"`
}

// createdVersionResponse carries the version and, once object storage exists, the
// upload target for its weights.
//
// Upload is nil in Phase 2 and Note says why. This is a DECLARED STUB in the sense
// the specification requires: the field exists in the contract, the value is
// honestly absent, and no caller is told an upload succeeded when none did
// (docs/api.md §4 describes the two-phase flow that Phase 3 completes).
type createdVersionResponse struct {
	versionResponse
	Upload *uploadTarget `json:"upload"`
	Note   string        `json:"note,omitempty"`
}

type uploadTarget struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
	MaxBytes  int64             `json:"max_bytes"`
}

type finalizeVersionRequest struct {
	ChecksumSHA256 string  `json:"checksum_sha256"`
	SizeBytes      *int64  `json:"size_bytes"`
	ContextWindow  *int32  `json:"context_window"`
	Quantization   *string `json:"quantization"`
	ParameterCount *int64  `json:"parameter_count"`
}

type failVersionRequest struct {
	Reason string `json:"reason"`
}

// ---------------------------------------------------------------------------
// deployments
// ---------------------------------------------------------------------------

// deploymentResponse separates spec from status, mirroring the schema's split
// between AUTHORITATIVE desired state and OBSERVED actual state (axiom A1). A flat
// object would invite a client to treat ready_replicas as something it can set.
type deploymentResponse struct {
	ID              uuid.UUID           `json:"id"`
	Name            string              `json:"name"`
	Namespace       string              `json:"namespace"`
	ModelVersionID  uuid.UUID           `json:"model_version_id"`
	Spec            deploymentSpecDTO   `json:"spec"`
	Status          deploymentStatusDTO `json:"status"`
	Generation      int64               `json:"generation"`
	CurrentRevision int32               `json:"current_revision"`
	CreatedBy       *uuid.UUID          `json:"created_by"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
}

type deploymentSpecDTO struct {
	DesiredReplicas  int32           `json:"desired_replicas"`
	MinReplicas      int32           `json:"min_replicas"`
	MaxReplicas      int32           `json:"max_replicas"`
	Resources        json.RawMessage `json:"resources"`
	RuntimeOverrides json.RawMessage `json:"runtime_overrides"`
	Autoscaling      json.RawMessage `json:"autoscaling"`
	Queue            json.RawMessage `json:"queue"`
	RoutingPolicyID  *uuid.UUID      `json:"routing_policy_id"`
}

// deploymentStatusDTO is what the control plane has OBSERVED.
//
// Every field here is either written by the controller or derived from the state
// column. reconciled is false and observed_generation is 0 for every deployment in
// Phase 2, because no controller runs yet: that is reported rather than papered
// over (axiom A6).
type deploymentStatusDTO struct {
	State              string          `json:"state"`
	StateReason        *string         `json:"state_reason"`
	StateMessage       *string         `json:"state_message"`
	StateEnteredAt     time.Time       `json:"state_entered_at"`
	StateAgeSeconds    int64           `json:"state_age_seconds"`
	Serving            bool            `json:"serving"`
	NextStates         []string        `json:"next_states"`
	ObservedGeneration int64           `json:"observed_generation"`
	Reconciled         bool            `json:"reconciled"`
	ReadyReplicas      int32           `json:"ready_replicas"`
	UpdatedReplicas    int32           `json:"updated_replicas"`
	Conditions         json.RawMessage `json:"conditions"`
	LastError          *string         `json:"last_error"`
	LastSyncedAt       *time.Time      `json:"last_synced_at"`
}

func newDeploymentResponse(d *models.Deployment, now time.Time) deploymentResponse {
	next := lifecycle.NextStates(d.State)
	nextStrings := make([]string, 0, len(next))
	for _, s := range next {
		nextStrings = append(nextStrings, string(s))
	}

	return deploymentResponse{
		ID: d.ID, Name: d.Name, Namespace: d.Namespace, ModelVersionID: d.ModelVersionID,
		Spec: deploymentSpecDTO{
			DesiredReplicas: d.DesiredReplicas, MinReplicas: d.MinReplicas,
			MaxReplicas: d.MaxReplicas, Resources: d.Resources,
			RuntimeOverrides: d.RuntimeOverrides, Autoscaling: d.Autoscaling,
			Queue: d.QueueConfig, RoutingPolicyID: d.RoutingPolicyID,
		},
		Status: deploymentStatusDTO{
			State: string(d.State), StateReason: d.StateReason, StateMessage: d.StateMessage,
			StateEnteredAt:     d.StateEnteredAt,
			StateAgeSeconds:    int64(now.Sub(d.StateEnteredAt).Seconds()),
			Serving:            lifecycle.Serving(d.State),
			NextStates:         nextStrings,
			ObservedGeneration: d.ObservedGeneration,
			Reconciled:         d.ObservedGeneration == d.Generation,
			ReadyReplicas:      d.ReadyReplicas, UpdatedReplicas: d.UpdatedReplicas,
			Conditions: d.Conditions, LastError: d.LastError, LastSyncedAt: d.LastSyncedAt,
		},
		Generation: d.Generation, CurrentRevision: d.CurrentRevision,
		CreatedBy: d.CreatedBy, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

// createdDeploymentResponse is returned with 202 Accepted, not 201 Created.
//
// Deliberate, and the Note says so: a deployment is a reconciled intent. Returning
// "created" while nothing has been provisioned would be a claim about the cluster
// that the control plane has not verified.
type createdDeploymentResponse struct {
	deploymentResponse
	Note string `json:"note,omitempty"`
}

type createDeploymentRequest struct {
	Name             string          `json:"name"`
	ModelVersionID   uuid.UUID       `json:"model_version_id"`
	Namespace        *string         `json:"namespace"`
	Replicas         *int32          `json:"replicas"`
	MinReplicas      *int32          `json:"min_replicas"`
	MaxReplicas      *int32          `json:"max_replicas"`
	Resources        json.RawMessage `json:"resources"`
	RuntimeOverrides json.RawMessage `json:"runtime_overrides"`
	Autoscaling      json.RawMessage `json:"autoscaling"`
	Queue            json.RawMessage `json:"queue"`
	RoutingPolicyID  *uuid.UUID      `json:"routing_policy_id"`
}

// updateDeploymentRequest is a patch: an omitted field is unchanged.
//
// Generation is required, not optional. A spec update without the generation the
// caller read is a lost-update waiting to happen, and making the client state what
// it believed is cheaper than reconciling two conflicting edits afterwards.
type updateDeploymentRequest struct {
	Generation       int64           `json:"generation"`
	ModelVersionID   *uuid.UUID      `json:"model_version_id"`
	Replicas         *int32          `json:"replicas"`
	MinReplicas      *int32          `json:"min_replicas"`
	MaxReplicas      *int32          `json:"max_replicas"`
	Resources        json.RawMessage `json:"resources"`
	RuntimeOverrides json.RawMessage `json:"runtime_overrides"`
	Autoscaling      json.RawMessage `json:"autoscaling"`
	Queue            json.RawMessage `json:"queue"`
	RoutingPolicyID  *uuid.UUID      `json:"routing_policy_id"`
}

type scaleRequest struct {
	Replicas    int32  `json:"replicas"`
	MinReplicas *int32 `json:"min_replicas"`
	MaxReplicas *int32 `json:"max_replicas"`
}

type rollbackRequest struct {
	// Revision may be omitted to mean "the one before the current".
	Revision *int32 `json:"revision"`
}

type transitionRequest struct {
	From    string  `json:"from"`
	To      string  `json:"to"`
	Reason  string  `json:"reason"`
	Message *string `json:"message"`
}

type revisionResponse struct {
	ID             uuid.UUID       `json:"id"`
	Revision       int32           `json:"revision"`
	ModelVersionID uuid.UUID       `json:"model_version_id"`
	Spec           json.RawMessage `json:"spec"`
	SpecHash       string          `json:"spec_hash"`
	Reason         string          `json:"reason"`
	CreatedBy      *uuid.UUID      `json:"created_by"`
	CreatedAt      time.Time       `json:"created_at"`
}

func newRevisionResponse(rev *models.DeploymentRevision) revisionResponse {
	return revisionResponse{
		ID: rev.ID, Revision: rev.Revision, ModelVersionID: rev.ModelVersionID,
		Spec: rev.Spec, SpecHash: hexString(rev.SpecHash), Reason: string(rev.Reason),
		CreatedBy: rev.CreatedBy, CreatedAt: rev.CreatedAt,
	}
}

type transitionResponse struct {
	ID         uuid.UUID  `json:"id"`
	FromState  *string    `json:"from_state"`
	ToState    string     `json:"to_state"`
	Reason     *string    `json:"reason"`
	Message    *string    `json:"message"`
	Generation int64      `json:"generation"`
	ActorType  string     `json:"actor_type"`
	ActorID    *uuid.UUID `json:"actor_id"`
	OccurredAt time.Time  `json:"occurred_at"`
}

func newTransitionResponse(t *models.DeploymentStateTransition) transitionResponse {
	out := transitionResponse{
		ID: t.ID, ToState: string(t.ToState), Reason: t.Reason, Message: t.Message,
		Generation: t.Generation, ActorType: string(t.ActorType), ActorID: t.ActorID,
		OccurredAt: t.OccurredAt,
	}
	if t.FromState != nil {
		s := string(*t.FromState)
		out.FromState = &s
	}
	return out
}

// ---------------------------------------------------------------------------
// lifecycle description
// ---------------------------------------------------------------------------

type lifecycleResponse struct {
	States      []lifecycleState `json:"states"`
	Transitions []lifecycleEdge  `json:"transitions"`
	Reasons     []string         `json:"reasons"`
}

type lifecycleState struct {
	Name     string   `json:"name"`
	Serving  bool     `json:"serving"`
	Resting  bool     `json:"resting"`
	InFlight bool     `json:"in_flight"`
	Next     []string `json:"next"`
}

type lifecycleEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Note string `json:"note"`
}

func newLifecycleResponse() lifecycleResponse {
	out := lifecycleResponse{}
	for _, s := range lifecycle.States() {
		next := lifecycle.NextStates(s)
		names := make([]string, 0, len(next))
		for _, n := range next {
			names = append(names, string(n))
		}
		out.States = append(out.States, lifecycleState{
			Name:     string(s),
			Serving:  lifecycle.Serving(s),
			Resting:  lifecycle.Resting(s),
			InFlight: lifecycle.InFlight(s),
			Next:     names,
		})
	}
	for _, e := range lifecycle.Edges() {
		out.Transitions = append(out.Transitions, lifecycleEdge{
			From: string(e.From), To: string(e.To), Note: e.Note,
		})
	}
	for _, r := range lifecycle.Reasons() {
		out.Reasons = append(out.Reasons, string(r))
	}
	return out
}

// ---------------------------------------------------------------------------
// audit
// ---------------------------------------------------------------------------

type auditResponse struct {
	ID           uuid.UUID       `json:"id"`
	CreatedAt    time.Time       `json:"created_at"`
	ActorType    string          `json:"actor_type"`
	ActorID      *uuid.UUID      `json:"actor_id"`
	ActorLabel   *string         `json:"actor_label"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   *uuid.UUID      `json:"resource_id"`
	Before       json.RawMessage `json:"before"`
	After        json.RawMessage `json:"after"`
	RequestID    *uuid.UUID      `json:"request_id"`
	IP           *string         `json:"ip"`
	UserAgent    *string         `json:"user_agent"`
}

func newAuditResponse(a *models.AuditLog) auditResponse {
	out := auditResponse{
		ID: a.ID, CreatedAt: a.CreatedAt, ActorType: string(a.ActorType),
		ActorID: a.ActorID, ActorLabel: a.ActorLabel, Action: a.Action,
		ResourceType: a.ResourceType, ResourceID: a.ResourceID,
		Before: a.Before, After: a.After, RequestID: a.RequestID,
		UserAgent: a.UserAgent,
	}
	if a.IP != nil {
		s := a.IP.String()
		out.IP = &s
	}
	return out
}

// hexString renders a byte slice as lowercase hex.
func hexString(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, digits[c>>4], digits[c&0x0f])
	}
	return string(out)
}
