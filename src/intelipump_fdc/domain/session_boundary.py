"""Physical dispensing session boundaries — never high-water across RESET.

A verified new hose session always gets a new stable identity even when the
previous session is still ACTIVE / UNCERTAIN / provisionally settled.
"""

from __future__ import annotations

# Minimum absolute drop (centilitres with volume_decimals=2) that implies RESET.
_MIN_ABSOLUTE_DROP = 10  # 0.10 L
# Relative drop when prior face was non-trivial.
_RELATIVE_DROP = 0.45
_MIN_PRIOR_FOR_RELATIVE = 20  # 0.20 L


def is_new_physical_session(
    *,
    prior_volume: int | None,
    prior_amount: int | None,
    new_volume: int,
    new_amount: int,
) -> bool:
    """True when DC2 face looks like a fresh authorize after a prior fill.

    Typical glue bug: prior 1476 cl → new 74 cl on the same UUID with
    ``max(prior, new)`` → one merged sale. Detect the drop and force a new
    identity instead.
    """
    if prior_volume is None:
        return False
    prior_v = int(prior_volume)
    new_v = int(new_volume)
    if prior_v <= 0:
        return False
    if new_v >= prior_v:
        return False
    drop = prior_v - new_v
    if drop >= _MIN_ABSOLUTE_DROP:
        return True
    if prior_v >= _MIN_PRIOR_FOR_RELATIVE and new_v <= int(prior_v * (1.0 - _RELATIVE_DROP)):
        return True
    # Amount drop with flat/near-zero new volume also signals RESET.
    prior_a = int(prior_amount or 0)
    new_a = int(new_amount)
    if prior_a > 0 and new_a < prior_a and drop >= 5 and new_v < prior_v:
        return True
    return False


def session_boundary_reason(
    *,
    prior_volume: int | None,
    prior_amount: int | None,
    new_volume: int,
    new_amount: int,
) -> str | None:
    if not is_new_physical_session(
        prior_volume=prior_volume,
        prior_amount=prior_amount,
        new_volume=new_volume,
        new_amount=new_amount,
    ):
        return None
    return (
        f"meter_reset_face prior_v={prior_volume} prior_a={prior_amount} "
        f"new_v={new_volume} new_a={new_amount}"
    )
