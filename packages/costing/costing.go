// Package costing turns measured usage into money (docs/architecture.md §9,
// ADR-0018). Money is integer micros of the profile's currency; there is no
// floating point anywhere a cost is computed, and a cost is rounded exactly once,
// at the end, half away from zero.
//
// The attribution model, stated in full by Assumptions:
//
//	a request is charged its share of one replica for the time it generated:
//	the replica's resources, divided by its parallel slots, for the request's
//	compute time, at the prices in force when it ran, plus the markup.
//
// Idle capacity — a replica nobody is using — is not attributed to anyone's
// requests. That is a choice, not an oversight: an estimate that charged idle time
// would make a tenant's cost depend on other tenants' traffic.
package costing

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// Profile is one immutable version of a pricing profile.
type Profile struct {
	ID                  uuid.UUID
	Name                string
	Version             int32
	Currency            string
	CPUCoreHourMicros   int64
	MemoryGiBHourMicros int64
	// GPUHourMicros prices a GPU by its type, e.g. {"nvidia-l4": 800000}.
	GPUHourMicros     map[string]int64
	MarkupBasisPoints int32
	EffectiveFrom     time.Time
}

// ParseGPUPrices reads the gpu_hour_micros column.
func ParseGPUPrices(raw json.RawMessage) (map[string]int64, error) {
	out := map[string]int64{}
	if len(raw) == 0 || string(raw) == "null" {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("gpu_hour_micros must map GPU types to integer micros: %w", err)
	}
	for k, v := range out {
		if v < 0 {
			return nil, fmt.Errorf("gpu_hour_micros[%s] must not be negative", k)
		}
	}
	return out, nil
}

// Resources is what one replica of a deployment requests.
type Resources struct {
	CPUMilli  int64  `json:"cpu_milli"`
	MemoryMiB int64  `json:"memory_mib"`
	GPUCount  int64  `json:"gpu_count"`
	GPUType   string `json:"gpu_type"`
}

// ParseResources reads deployments.resources.
func ParseResources(raw json.RawMessage) (Resources, error) {
	var r Resources
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &r); err != nil {
			return Resources{}, err
		}
	}
	if r.CPUMilli < 0 || r.MemoryMiB < 0 || r.GPUCount < 0 {
		return Resources{}, errors.New("resources must not be negative")
	}
	return r, nil
}

// ReplicaHourMicros is what one replica costs per hour, before markup, as an
// exact rational: CPU cores × core-hour + GiB × GiB-hour + GPUs × GPU-hour.
func ReplicaHourMicros(p Profile, r Resources) *big.Rat {
	total := new(big.Rat)
	// cpu_milli/1000 cores.
	total.Add(total, new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(r.CPUMilli), big.NewInt(p.CPUCoreHourMicros)),
		big.NewInt(1000)))
	// memory_mib/1024 GiB.
	total.Add(total, new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(r.MemoryMiB), big.NewInt(p.MemoryGiBHourMicros)),
		big.NewInt(1024)))
	if r.GPUCount > 0 {
		total.Add(total, new(big.Rat).SetInt(new(big.Int).Mul(big.NewInt(r.GPUCount),
			big.NewInt(p.GPUHourMicros[r.GPUType]))))
	}
	return total
}

// RequestCostMicros is one request's cost: its share of a replica (one of slots)
// for computeMS milliseconds, plus the markup, rounded once, half away from zero.
// slots below 1 is treated as 1.
func RequestCostMicros(p Profile, r Resources, slots, computeMS int64) int64 {
	if computeMS <= 0 {
		return 0
	}
	if slots < 1 {
		slots = 1
	}
	cost := ReplicaHourMicros(p, r)
	cost.Mul(cost, new(big.Rat).SetFrac(big.NewInt(computeMS), new(big.Int).Mul(big.NewInt(3_600_000), big.NewInt(slots))))
	cost.Mul(cost, new(big.Rat).SetFrac(big.NewInt(10_000+int64(p.MarkupBasisPoints)), big.NewInt(10_000)))
	return roundHalfAway(cost)
}

// roundHalfAway rounds a non-negative rational to the nearest integer, halves up.
func roundHalfAway(r *big.Rat) int64 {
	num, den := new(big.Int).Set(r.Num()), new(big.Int).Set(r.Denom())
	// floor((2·num + den) / (2·den))
	num.Mul(num, big.NewInt(2))
	num.Add(num, den)
	den.Mul(den, big.NewInt(2))
	return new(big.Int).Quo(num, den).Int64()
}

// Corrected scales a sampled count back to an estimate of the whole: a record
// kept at sample rate s stands for 1/s requests. The estimate is rounded to the
// nearest whole request; a rate outside (0, 1] is treated as 1.
func Corrected(sampled int64, sampleRate float64) int64 {
	if sampleRate <= 0 || sampleRate > 1 {
		return sampled
	}
	// Rates are stored as real; resolve to millionths so the arithmetic is exact.
	ppm := int64(sampleRate*1_000_000 + 0.5)
	if ppm < 1 {
		ppm = 1
	}
	return roundHalfAway(new(big.Rat).SetFrac(big.NewInt(sampled*1_000_000), big.NewInt(ppm)))
}

// Assumptions is what every cost figure NEBULA reports assumes, in the words the
// API returns alongside it.
func Assumptions() []string {
	return []string{
		"Costs are estimates, in integer micros of the pricing profile's currency.",
		"A request is charged its share of one replica for the time it generated: the replica's CPU, memory and GPU requests divided by its parallel slots, for the request's compute time.",
		"Prices are the organization's pricing profile, or the global default, in the version in force when the request ran: a price change applies from its effective time and never alters a cost already computed.",
		"Idle capacity — replicas running with no requests — is not attributed to any request.",
		"Queueing and network time are not charged; only generation is.",
		"The global default profile prices everything at zero until an operator configures real prices.",
	}
}
