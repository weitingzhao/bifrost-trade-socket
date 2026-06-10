"""Patch ib_insync timezone parsing for IB legacy zone names (e.g. US/Eastern).

Python zoneinfo does not resolve US/* aliases even when tzdata is installed.
IB execDetails timestamps use ``20260609 12:24:16 US/Eastern`` which breaks
reqExecutions decoding unless mapped to IANA names (America/New_York).
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

_PATCHED = False

# IB / TWS legacy names → IANA (zoneinfo)
_IB_TZ_ALIASES: Dict[str, str] = {
    "US/Eastern": "America/New_York",
    "US/Central": "America/Chicago",
    "US/Mountain": "America/Denver",
    "US/Pacific": "America/Los_Angeles",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
}


def _normalize_ib_datetime_string(s: str) -> str:
    if s.count(" ") >= 2 and "  " not in s:
        s0, s1, s2 = s.split(" ", 2)
        mapped = _IB_TZ_ALIASES.get(s2.strip())
        if mapped:
            return f"{s0} {s1} {mapped}"
    return s


def apply_ib_timezone_patch() -> None:
    """Idempotent: wrap ib_insync.util.parseIBDatetime with IB legacy TZ aliases."""
    global _PATCHED
    if _PATCHED:
        return

    import ib_insync.util as util

    original: Callable[[str], Any] = util.parseIBDatetime

    def patched(s: str) -> Any:
        normalized = _normalize_ib_datetime_string(s)
        return original(normalized)

    util.parseIBDatetime = patched  # type: ignore[method-assign]

    # decoder binds parseIBDatetime at import time; patch that reference too if loaded.
    try:
        import ib_insync.decoder as decoder_mod

        decoder_mod.parseIBDatetime = patched  # type: ignore[attr-defined]
    except ImportError:
        pass

    _PATCHED = True
    logger.info("IB timezone patch applied (legacy US/* → IANA, e.g. US/Eastern→America/New_York)")
