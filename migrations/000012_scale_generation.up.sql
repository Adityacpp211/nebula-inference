-- 000012 a desired_replicas change is a spec change.
--
-- generation changes on SPEC changes only (000004), and the controller's work
-- predicate is `generation <> observed_generation`. The control plane advances
-- generation itself when a user scales. The autoscaler connects as
-- nebula_controller, whose grants allow desired_replicas and not generation
-- (000007), so without this trigger its decisions would wait for the controller's
-- periodic resync instead of being reconciled at once.
--
-- The trigger advances generation only when the statement did not, so a control-
-- plane update that already moved it is not counted twice. Column privileges are
-- checked against the statement's SET list, not the trigger's NEW row, so the
-- autoscaler still cannot write generation directly.

CREATE FUNCTION nebula_bump_generation_on_scale() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.desired_replicas IS DISTINCT FROM OLD.desired_replicas
       AND NEW.generation = OLD.generation THEN
        NEW.generation := OLD.generation + 1;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER trg_deployments__scale_generation
    BEFORE UPDATE OF desired_replicas ON deployments
    FOR EACH ROW EXECUTE FUNCTION nebula_bump_generation_on_scale();
