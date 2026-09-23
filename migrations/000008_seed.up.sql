-- 000008 seed: the built-in rows the system cannot function without
--
-- Production-safe only. There is no organization, user or API key here: a
-- migration must never create a usable credential. Development seeding is a
-- separate, explicitly gated step (NEBULA_DEV_SEED) introduced in Phase 2 once
-- API-key generation exists.
--
-- Identifiers are fixed so the rows are referenceable and the inserts are
-- idempotent. They are deliberately recognisable as seed data.

-- The global default pricing profile prices EVERYTHING AT ZERO.
--
-- This is the mechanism behind "no fabricated costs" (axiom A6): until an
-- operator configures real prices, every cost figure NEBULA reports is exactly
-- zero and visibly so, rather than a plausible-looking invented number.
INSERT INTO pricing_profiles (
    id, org_id, name, version, currency,
    cpu_core_hour_micros, memory_gib_hour_micros, gpu_hour_micros,
    storage_gib_month_micros, egress_gib_micros, markup_basis_points, effective_from
) VALUES (
    '00000000-0000-7000-8000-000000000001', NULL, 'default', 1, 'USD',
    0, 0, '{}'::jsonb,
    0, 0, 0, '1970-01-01T00:00:00Z'
) ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE pricing_profiles IS
    'Immutable, versioned infrastructure prices in integer micros. The seeded '
    'default prices everything at zero so no cost is ever fabricated before an '
    'operator configures real numbers.';

-- Built-in routing strategies. Rows rather than code constants so an operator can
-- point a route at a different strategy without a deploy; the strategy NAME is
-- resolved against the registry in packages/routing (ADR-0004).
--
-- least_loaded is the default: inference request cost varies by orders of
-- magnitude, so round-robin distributes requests evenly and load unevenly.
INSERT INTO routing_policies (id, org_id, name, strategy, config, is_default) VALUES
    ('00000000-0000-7000-8000-000000000011', NULL, 'least-loaded', 'least_loaded',
     '{"description":"Fewest in-flight requests plus queue depth. Default."}'::jsonb, true),
    ('00000000-0000-7000-8000-000000000012', NULL, 'round-robin', 'round_robin',
     '{"description":"Even request distribution. Baseline and benchmark control arm."}'::jsonb, false),
    ('00000000-0000-7000-8000-000000000013', NULL, 'latency-aware', 'latency_aware',
     '{"description":"EWMA of time-to-first-token per endpoint. For heterogeneous hardware.","half_life_seconds":30}'::jsonb, false),
    ('00000000-0000-7000-8000-000000000014', NULL, 'cost-aware', 'cost_aware',
     '{"description":"Prefer cheaper hardware within a latency budget.","max_p95_latency_ms":2000}'::jsonb, false),
    ('00000000-0000-7000-8000-000000000015', NULL, 'capability-based', 'capability_based',
     '{"description":"Match required context window and features to the model version."}'::jsonb, false),
    ('00000000-0000-7000-8000-000000000016', NULL, 'failover', 'failover',
     '{"description":"Wrapper: fall back to a declared secondary route when no primary endpoint is eligible."}'::jsonb, false)
ON CONFLICT (id) DO NOTHING;

-- A conservative default rate-limit policy. Deliberately finite: an unset limit
-- is an unbounded one, and "no limit by default" is how a single client takes down
-- a shared deployment.
INSERT INTO rate_limit_policies (
    id, org_id, name, requests_per_minute, tokens_per_minute, max_concurrency, max_queue_depth
) VALUES (
    '00000000-0000-7000-8000-000000000021', NULL, 'default', 600, 200000, 32, 128
) ON CONFLICT (id) DO NOTHING;
