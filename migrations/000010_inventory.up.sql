-- 000010 inventory: what capacity admission needs from the node cache.
--
-- Phase 5 turns `nodes` from a designed table into a used one. Capacity admission
-- (packages/scheduler) needs, per node, how much is already requested as well as
-- how much is allocatable, and it needs both in one normalized unit system so the
-- control plane never parses a Kubernetes quantity string.
--
-- Shapes, written by the controller's inventory reconciler:
--   allocatable, requested: {"cpu_milli": int, "memory_mib": int, "gpu": int}
--   taints:                 [{"key": text, "effect": text}]
--
-- `requested` is OBSERVED like the rest of the row: rebuilt from the Pod informer
-- on every sync, never a decision input the live cluster could contradict for long.

ALTER TABLE nodes ADD COLUMN requested jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE nodes ADD CONSTRAINT ck_nodes__requested_object CHECK (jsonb_typeof(requested) = 'object');

COMMENT ON COLUMN nodes.requested IS
    'Sum of pod resource requests on the node, normalized: {"cpu_milli","memory_mib","gpu"}. OBSERVED.';
