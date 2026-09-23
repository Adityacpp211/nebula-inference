// Package lifecycle holds NEBULA's deployment state machine.
//
// It is deliberately I/O-free: the transition graph, the predicates and the reason
// vocabulary are pure data and pure functions, so they are exhaustively testable
// without a database or a cluster (docs/repository-structure.md §4, rule 5).
//
// The same graph is encoded in SQL in migrations/000009 as deployment_state_edges,
// and enforced there by a trigger. That duplication is intentional — defence in
// depth — and a test asserts the two agree. If they ever diverge the database
// wins and writes start failing, so catching it in CI is the point.
package lifecycle

import (
	"fmt"
	"sort"

	"github.com/adityasatwar321/nebula/packages/db/models"
)

// State is a deployment's lifecycle state. Alias of the database enum mirror so
// callers do not have to convert.
type State = models.DeploymentState

// The eight states of a deployment.
//
// provisioning and starting are separate because they fail differently and on
// different timescales: provisioning is NEBULA applying Kubernetes objects and
// fails fast (admission, RBAC, quota), while starting is a pod pulling a
// multi-gigabyte artifact and loading a model, where several minutes is normal.
// A single "progressing" state cannot express "stuck" usefully for both.
const (
	// Pending: the record exists and nothing has been provisioned yet.
	Pending State = "pending"
	// Provisioning: the controller is creating or updating Kubernetes objects.
	Provisioning State = "provisioning"
	// Starting: objects exist; pods are pulling artifacts and loading models.
	Starting State = "starting"
	// Ready: the desired number of replicas are ready and serving.
	Ready State = "ready"
	// Degraded: serving, but below the desired replica count.
	Degraded State = "degraded"
	// Failed: cannot proceed without intervention.
	Failed State = "failed"
	// Stopping: draining and scaling to zero.
	Stopping State = "stopping"
	// Stopped: no replicas running. The record, its revisions and its history are
	// retained, and it can be started again.
	Stopped State = "stopped"
)

// States returns every state, in lifecycle order.
func States() []State {
	return []State{Pending, Provisioning, Starting, Ready, Degraded, Failed, Stopping, Stopped}
}

// Edge is one legal transition and why it exists.
type Edge struct {
	From State
	To   State
	Note string
}

// edges is the authoritative transition graph. It must match
// deployment_state_edges in migrations/000009.
var edges = []Edge{
	// forward path
	{Pending, Provisioning, "controller claimed the deployment and began applying objects"},
	{Provisioning, Starting, "objects applied; pods are starting"},
	{Starting, Ready, "all desired replicas became ready"},
	{Starting, Degraded, "some replicas became ready; others have not"},
	{Ready, Degraded, "a replica was lost"},
	{Degraded, Ready, "replicas recovered"},

	// re-reconciliation after a spec change goes back to provisioning, never
	// straight to starting: new objects have to be applied first
	{Ready, Provisioning, "spec changed; reconciling the new revision"},
	{Degraded, Provisioning, "spec changed; reconciling the new revision"},
	{Starting, Provisioning, "spec changed while starting; reconciling the new revision"},

	// failure, from anywhere work can fail
	{Pending, Failed, "admission rejected the deployment"},
	{Provisioning, Failed, "applying objects failed"},
	{Starting, Failed, "no replica could start"},
	{Ready, Failed, "every replica was lost"},
	{Degraded, Failed, "the remaining replicas were lost"},
	{Stopping, Failed, "the deployment could not be stopped"},

	// recovery
	{Failed, Provisioning, "a new revision was applied after the failure"},

	// stopping, from anywhere
	{Pending, Stopping, "stopped before provisioning began"},
	{Provisioning, Stopping, "stop requested during provisioning"},
	{Starting, Stopping, "stop requested while starting"},
	{Ready, Stopping, "stop requested"},
	{Degraded, Stopping, "stop requested"},
	{Failed, Stopping, "stop requested after failure"},
	{Stopping, Stopped, "all replicas are gone"},

	// restart
	{Stopped, Pending, "start requested"},
}

// transitions indexes edges for O(1) lookup.
var transitions = func() map[State]map[State]string {
	m := make(map[State]map[State]string, len(States()))
	for _, e := range edges {
		if m[e.From] == nil {
			m[e.From] = map[State]string{}
		}
		m[e.From][e.To] = e.Note
	}
	return m
}()

// Edges returns every legal transition, sorted, for documentation and for the
// test that compares this graph with the database's.
func Edges() []Edge {
	out := make([]Edge, len(edges))
	copy(out, edges)
	sort.Slice(out, func(i, j int) bool {
		if out[i].From != out[j].From {
			return out[i].From < out[j].From
		}
		return out[i].To < out[j].To
	})
	return out
}

// ValidState reports whether s is one of the eight states.
func ValidState(s State) bool {
	switch s {
	case Pending, Provisioning, Starting, Ready, Degraded, Failed, Stopping, Stopped:
		return true
	}
	return false
}

// CanTransition reports whether from -> to is legal.
//
// A transition to the same state is NOT legal: it is not a transition. Callers
// that recompute state on every reconcile should use Transition, which reports
// "no change" separately from "illegal".
func CanTransition(from, to State) bool {
	_, ok := transitions[from][to]
	return ok
}

// NextStates returns the states reachable from s, sorted.
func NextStates(s State) []State {
	out := make([]State, 0, len(transitions[s]))
	for to := range transitions[s] {
		out = append(out, to)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Serving reports whether a deployment in this state may receive traffic.
//
// Degraded serves: some replicas are healthy, and removing the whole deployment
// from rotation because one pod died would turn a partial failure into a total one.
func Serving(s State) bool { return s == Ready || s == Degraded }

// Resting reports whether the state is one where nothing is expected to change
// without an operator or a new revision. A deployment may only be deleted from a
// resting state, which is also a CHECK constraint on the table.
func Resting(s State) bool { return s == Pending || s == Failed || s == Stopped }

// InFlight reports whether the controller has work in progress.
func InFlight(s State) bool { return s == Provisioning || s == Starting || s == Stopping }

// Deletable reports whether the record may be soft-deleted from this state.
// Deleting a running deployment would orphan its Kubernetes objects.
func Deletable(s State) bool { return Resting(s) }

// Reason is a machine-matchable transition reason.
//
// The CamelCase convention matches Kubernetes condition reasons so operators read
// one vocabulary across both systems, and so alerting can match on it. The human
// sentence goes in the message beside it.
type Reason string

// Transition reasons. A closed set: the dashboard and the alert rules branch on
// these, and an open set means a new value silently renders as nothing.
const (
	ReasonCreated               Reason = "Created"
	ReasonClaimed               Reason = "Claimed"
	ReasonObjectsApplied        Reason = "ObjectsApplied"
	ReasonReplicasReady         Reason = "ReplicasReady"
	ReasonReplicaLost           Reason = "ReplicaLost"
	ReasonReplicasRecovered     Reason = "ReplicasRecovered"
	ReasonSpecChanged           Reason = "SpecChanged"
	ReasonScaled                Reason = "Scaled"
	ReasonRolledBack            Reason = "RolledBack"
	ReasonInsufficientCapacity  Reason = "InsufficientCapacity"
	ReasonApplyFailed           Reason = "ApplyFailed"
	ReasonModelLoadFailed       Reason = "ModelLoadFailed"
	ReasonArtifactChecksumError Reason = "ArtifactChecksumMismatch"
	ReasonAllReplicasLost       Reason = "AllReplicasLost"
	ReasonStopRequested         Reason = "StopRequested"
	ReasonStartRequested        Reason = "StartRequested"
	ReasonDrained               Reason = "Drained"
	ReasonStopFailed            Reason = "StopFailed"
	ReasonQuotaExceeded         Reason = "QuotaExceeded"
)

// Reasons returns every reason, for validation and documentation.
func Reasons() []Reason {
	return []Reason{
		ReasonCreated, ReasonClaimed, ReasonObjectsApplied, ReasonReplicasReady,
		ReasonReplicaLost, ReasonReplicasRecovered, ReasonSpecChanged, ReasonScaled,
		ReasonRolledBack, ReasonInsufficientCapacity, ReasonApplyFailed,
		ReasonModelLoadFailed, ReasonArtifactChecksumError, ReasonAllReplicasLost,
		ReasonStopRequested, ReasonStartRequested, ReasonDrained, ReasonStopFailed,
		ReasonQuotaExceeded,
	}
}

// ValidReason reports whether r is a known reason.
func ValidReason(r Reason) bool {
	for _, v := range Reasons() {
		if v == r {
			return true
		}
	}
	return false
}

// Terminal reports whether a reason means the state will not improve on its own.
// A rollout aborts on a terminal reason rather than waiting out its analysis window.
func (r Reason) Terminal() bool {
	switch r {
	case ReasonModelLoadFailed, ReasonArtifactChecksumError, ReasonInsufficientCapacity, ReasonQuotaExceeded:
		return true
	}
	return false
}

// TransitionError reports a rejected transition. Typed so a handler can map it to
// a 409 rather than a 500.
type TransitionError struct {
	From State
	To   State
	// Allowed lists what would have been legal from From.
	Allowed []State
}

func (e *TransitionError) Error() string {
	if !ValidState(e.To) {
		return fmt.Sprintf("%q is not a deployment state", e.To)
	}
	if len(e.Allowed) == 0 {
		return fmt.Sprintf("no transition out of %s is defined", e.From)
	}
	return fmt.Sprintf("cannot move a deployment from %s to %s; legal transitions from %s are %v",
		e.From, e.To, e.From, e.Allowed)
}

// Outcome describes what a requested transition means.
type Outcome int

// Transition outcomes.
const (
	// OutcomeChanged: the transition is legal and changes the state.
	OutcomeChanged Outcome = iota
	// OutcomeUnchanged: already in the target state. Not an error — a controller
	// that recomputes desired state on every reconcile will ask for this
	// constantly, and treating it as a failure would fill the logs with noise.
	OutcomeUnchanged
)

// Transition validates a requested state change.
//
// It separates "no change needed" from "illegal", because a level-triggered
// controller asks for the state it has computed regardless of the current one.
func Transition(from, to State) (Outcome, error) {
	if !ValidState(from) {
		return 0, &TransitionError{From: from, To: to}
	}
	if !ValidState(to) {
		return 0, &TransitionError{From: from, To: to}
	}
	if from == to {
		return OutcomeUnchanged, nil
	}
	if !CanTransition(from, to) {
		return 0, &TransitionError{From: from, To: to, Allowed: NextStates(from)}
	}
	return OutcomeChanged, nil
}

// Reachable reports whether to is reachable from start by any sequence of legal
// transitions. Used by tests to prove no state is a dead end and every state is
// reachable from pending.
func Reachable(start, to State) bool {
	seen := map[State]bool{start: true}
	queue := []State{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == to {
			return true
		}
		for next := range transitions[cur] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}
