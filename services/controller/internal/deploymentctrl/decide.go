package deploymentctrl

import (
	"fmt"
	"strings"
	"time"

	"github.com/adityasatwar321/nebula/packages/db/models"
	"github.com/adityasatwar321/nebula/packages/lifecycle"
	"github.com/adityasatwar321/nebula/services/controller/internal/store"
)

// Observed is what Kubernetes reports about one deployment's objects.
type Observed struct {
	// Exists is false when the Deployment object is absent.
	Exists bool
	// Generation checks: a status is only trusted when Kubernetes has seen the
	// latest spec (ObservedGeneration >= Generation of the object).
	Current   bool
	Replicas  int32
	Ready     int32
	Updated   int32
	Available int32
	// Pods is how many pods exist for the deployment, terminating ones included.
	Pods int
	// ArtifactFailure is the artifact puller's reported failure, when any pod's
	// init container terminated with one.
	ArtifactFailure *ArtifactFailure
	// Problems are pod-level explanations: image pull errors, crash loops,
	// unschedulable pods. Used for the condition message and the failure reason.
	Problems []string
}

// ArtifactFailure is the puller's FetchResult for a failed pull.
type ArtifactFailure struct {
	Code    string
	Message string
}

// Decision is the next lifecycle step.
type Decision struct {
	// To is the next state; empty means stay.
	To      models.DeploymentState
	Reason  string
	Message string
}

// Reasons written to deployments.state_reason come from the lifecycle vocabulary
// (packages/lifecycle), the same closed set the API validates and documents, so a
// dashboard never meets a reason it has no explanation for.
var (
	ReasonClaimed         = string(lifecycle.ReasonClaimed)
	ReasonSpecChanged     = string(lifecycle.ReasonSpecChanged)
	ReasonObjectsApplied  = string(lifecycle.ReasonObjectsApplied)
	ReasonReplicasReady   = string(lifecycle.ReasonReplicasReady)
	ReasonPartiallyReady  = string(lifecycle.ReasonPartiallyReady)
	ReasonReplicaLost     = string(lifecycle.ReasonReplicaLost)
	ReasonRecovered       = string(lifecycle.ReasonReplicasRecovered)
	ReasonStartTimeout    = string(lifecycle.ReasonStartTimeout)
	ReasonAllReplicasLost = string(lifecycle.ReasonAllReplicasLost)
	ReasonScaledToZero    = string(lifecycle.ReasonDrained)
	ReasonApplyFailed     = string(lifecycle.ReasonApplyFailed)
	ReasonInvalidSpec     = string(lifecycle.ReasonApplyFailed)
	ReasonNewRevision     = string(lifecycle.ReasonSpecChanged)
	// ReasonObjectsMissing is a condition reason, not a state reason.
	ReasonObjectsMissing = "ObjectsMissing"
)

// artifactReason maps a puller error code onto the vocabulary: a checksum
// mismatch has its own reason; anything else about the artifact is a load failure,
// with the specifics in the message.
func artifactReason(code string) string {
	if code == "checksum_mismatch" {
		return string(lifecycle.ReasonArtifactChecksumError)
	}
	return string(lifecycle.ReasonModelLoadFailed)
}

// Decide chooses the next state after objects have been applied. It covers the
// states the controller advances by observation: starting, ready, degraded and
// stopping. The transitions that precede an apply (pending -> provisioning,
// spec changes -> provisioning) are the reconciler's, because they happen before
// there is anything to observe.
func Decide(d *store.Deployment, obs Observed, now time.Time, startingTimeout time.Duration) Decision {
	want := Replicas(d)
	age := now.Sub(d.StateEnteredAt)

	switch d.State {
	case lifecycle.Provisioning:
		return Decision{To: lifecycle.Starting, Reason: ReasonObjectsApplied,
			Message: fmt.Sprintf("objects applied for generation %d; waiting for %d replica(s)", d.Generation, want)}

	case lifecycle.Starting:
		if obs.ArtifactFailure != nil {
			// A checksum mismatch will not fix itself by waiting: the bytes in the store
			// are wrong. Failing now, with the reason, beats a start timeout later.
			return Decision{To: lifecycle.Failed, Reason: artifactReason(obs.ArtifactFailure.Code),
				Message: obs.ArtifactFailure.Message}
		}
		if !obs.Current {
			return Decision{}
		}
		switch {
		case obs.Ready >= want:
			return Decision{To: lifecycle.Ready, Reason: ReasonReplicasReady,
				Message: fmt.Sprintf("%d/%d replicas ready", obs.Ready, want)}
		case obs.Ready > 0:
			return Decision{To: lifecycle.Degraded, Reason: ReasonPartiallyReady,
				Message: fmt.Sprintf("%d/%d replicas ready%s", obs.Ready, want, problems(obs))}
		case age > startingTimeout:
			return Decision{To: lifecycle.Failed, Reason: ReasonStartTimeout,
				Message: fmt.Sprintf("no replica became ready within %s%s", startingTimeout, problems(obs))}
		}
		return Decision{}

	case lifecycle.Ready:
		if obs.Current && obs.Ready < want {
			return Decision{To: lifecycle.Degraded, Reason: ReasonReplicaLost,
				Message: fmt.Sprintf("%d/%d replicas ready%s", obs.Ready, want, problems(obs))}
		}
		return Decision{}

	case lifecycle.Degraded:
		if !obs.Current {
			return Decision{}
		}
		switch {
		case obs.Ready >= want:
			return Decision{To: lifecycle.Ready, Reason: ReasonRecovered,
				Message: fmt.Sprintf("%d/%d replicas ready", obs.Ready, want)}
		case obs.Ready == 0 && age > startingTimeout:
			// Degraded with nothing serving for as long as a start is allowed to take:
			// the replicas are not coming back on their own.
			return Decision{To: lifecycle.Failed, Reason: ReasonAllReplicasLost,
				Message: fmt.Sprintf("no replica has been ready for %s%s", age.Round(time.Second), problems(obs))}
		}
		return Decision{}

	case lifecycle.Stopping:
		if obs.Replicas == 0 && obs.Pods == 0 {
			return Decision{To: lifecycle.Stopped, Reason: ReasonScaledToZero, Message: "all replicas are gone"}
		}
		return Decision{}
	}
	return Decision{}
}

// Conditions summarizes the observation in the Kubernetes condition style.
func Conditions(d *store.Deployment, obs Observed, prev []store.Condition, now time.Time) []store.Condition {
	want := Replicas(d)
	set := func(t, status, reason, msg string) store.Condition {
		c := store.Condition{Type: t, Status: status, Reason: reason, Message: msg, LastTransitionTime: now}
		for _, p := range prev {
			if p.Type == t && p.Status == status {
				// The time a condition last CHANGED, not the time it was last written.
				c.LastTransitionTime = p.LastTransitionTime
			}
		}
		return c
	}
	var out []store.Condition
	switch {
	case !obs.Exists:
		out = append(out, set("Available", "False", ReasonObjectsMissing, "the worker Deployment does not exist yet"))
	case want == 0:
		out = append(out, set("Available", "False", "ScaledToZero", "the deployment runs no replicas"))
	case obs.Ready >= want:
		out = append(out, set("Available", "True", ReasonReplicasReady, fmt.Sprintf("%d/%d replicas ready", obs.Ready, want)))
	default:
		out = append(out, set("Available", "False", "InsufficientReplicas",
			fmt.Sprintf("%d/%d replicas ready%s", obs.Ready, want, problems(obs))))
	}
	if obs.ArtifactFailure != nil {
		out = append(out, set("ArtifactReady", "False", "Artifact"+camel(obs.ArtifactFailure.Code), obs.ArtifactFailure.Message))
	}
	reconciled := "True"
	if d.ObservedGeneration < d.Generation {
		reconciled = "False"
	}
	out = append(out, set("Reconciled", reconciled, "GenerationObserved",
		fmt.Sprintf("generation %d applied", d.Generation)))
	return out
}

func problems(obs Observed) string {
	if len(obs.Problems) == 0 {
		return ""
	}
	p := obs.Problems
	if len(p) > 3 {
		p = append(p[:3:3], fmt.Sprintf("and %d more", len(obs.Problems)-3))
	}
	return ": " + strings.Join(p, "; ")
}

// camel turns snake_case into CamelCase: checksum_mismatch -> ChecksumMismatch.
func camel(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		if r == '_' || r == '-' {
			up = true
			continue
		}
		if up && r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
		}
		up = false
		b.WriteRune(r)
	}
	return b.String()
}
