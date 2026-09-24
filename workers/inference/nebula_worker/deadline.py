"""Deadlines: absolute, caller-supplied, and enforced rather than advised.

``X-Nebula-Deadline`` is an absolute Unix millisecond timestamp. Absolute on purpose:
a duration has to be re-derived at every hop, and each re-derivation quietly grants a
fresh budget, so a request with a "5 second timeout" can take fifteen. An absolute
instant means every component agrees on when to stop without talking to each other.

The rule this module exists to enforce is admission-side: the worker never accepts
work it cannot start before the deadline. Accepting and then timing out spends the
caller's budget on a queue slot and returns nothing, when the gateway could have
spent it on a replica that was free.
"""

from __future__ import annotations

import time
from dataclasses import dataclass


class DeadlineError(ValueError):
    """The deadline header was malformed or already past."""


@dataclass(frozen=True, slots=True)
class Budget:
    """What is left of a request's allowance."""

    #: Absolute deadline in Unix milliseconds, or None when the caller gave none.
    deadline_ms: int | None
    #: Seconds remaining at the moment this was computed.
    remaining_s: float
    #: True when the worker supplied the deadline from its default rather than the
    #: caller supplying one. Tracked so a response can be honest about whose timeout
    #: applied, and so strict mode can refuse it.
    defaulted: bool = False

    @property
    def expired(self) -> bool:
        return self.remaining_s <= 0


def parse_deadline(raw: str | None, *, now_ms: int | None = None) -> int | None:
    """Parse the header. Absent is None; malformed is an error, never a default.

    A malformed deadline is rejected rather than replaced with a default because the
    caller clearly intended *some* bound, and silently substituting ours would run
    their request for a length they never agreed to.
    """
    if raw is None or not raw.strip():
        return None
    try:
        deadline_ms = int(raw.strip())
    except ValueError as exc:
        raise DeadlineError(
            f"X-Nebula-Deadline must be absolute Unix milliseconds, got {raw!r}"
        ) from exc
    if deadline_ms <= 0:
        raise DeadlineError("X-Nebula-Deadline must be a positive timestamp")

    current = now_ms if now_ms is not None else int(time.time() * 1000)

    # Both directions of the seconds/milliseconds mistake are caught here, and both
    # are reported as what they are rather than as their symptom.
    #
    # Too small: a Unix *seconds* timestamp is around 1.7e9, where a millisecond one is
    # around 1.7e12. Left alone, a seconds value looks like a deadline in 1970 and the
    # caller is told "your deadline has already passed" — true, and useless. The
    # threshold is 1e11 ms (about 1973), which no real deadline will ever be below.
    if deadline_ms < 100_000_000_000:
        raise DeadlineError(
            "X-Nebula-Deadline must be absolute Unix MILLISECONDS; "
            f"{deadline_ms} looks like seconds"
        )
    # Too large: more than a day out is either seconds-scaled the other way or a clock
    # problem, and serving it as written would pin a slot for a day.
    if deadline_ms > current + 86_400_000:
        raise DeadlineError(
            "X-Nebula-Deadline is more than 24h in the future; check the units and the clock"
        )
    return deadline_ms


def budget_for(
    deadline_ms: int | None,
    *,
    default_timeout_s: float,
    strict: bool,
    now: float | None = None,
) -> Budget:
    """Turn a parsed deadline into a budget, applying the default where allowed."""
    current = now if now is not None else time.time()
    if deadline_ms is None:
        if strict:
            raise DeadlineError(
                "X-Nebula-Deadline is required: a request with no deadline cannot be "
                "admitted, because nothing would ever stop it"
            )
        return Budget(
            deadline_ms=int((current + default_timeout_s) * 1000),
            remaining_s=default_timeout_s,
            defaulted=True,
        )
    return Budget(deadline_ms=deadline_ms, remaining_s=deadline_ms / 1000 - current)


__all__ = ["Budget", "DeadlineError", "budget_for", "parse_deadline"]
