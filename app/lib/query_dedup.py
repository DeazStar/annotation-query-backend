"""
Signature-based query deduplication using Redis: single-flight for concurrent
identical queries and a short-lived result pointer for recent completions.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Optional, Tuple

from app import redis_client
from app.constants import TaskStatus

_LOG = logging.getLogger(__name__)

PREFIX = "qdedup"
EXP = int(os.getenv("REDIS_EXPIRATION", "3600"))
ACTIVE_TTL = int(os.getenv("QUERY_DEDUP_ACTIVE_TTL", "7200"))


def is_enabled() -> bool:
    return os.getenv("QUERY_DEDUP_ENABLED", "true").lower() in ("1", "true", "yes")


def _decode(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return str(val)


def _structural_request(request: dict) -> dict:
    """Identity for the graph query: ignore annotation_id and incidental fields."""
    nodes = request.get("nodes") or []
    predicates = request.get("predicates") or []
    return {"nodes": nodes, "predicates": predicates}


def compute_signature(
    query: Tuple[str, str, str],
    request: dict,
    species: str,
    data_source: str,
    db_type: str,
) -> str:
    payload = {
        "db": db_type,
        "q0": query[0],
        "q1": query[1],
        "q2": query[2],
        "species": species or "human",
        "data_source": data_source or "all",
        "req": _structural_request(request),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _cache_still_valid(ref_id: str) -> bool:
    raw = redis_client.get(str(ref_id))
    if not raw:
        return False
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if data.get("status") != TaskStatus.COMPLETE.value:
        return False
    g = data.get("graph")
    return g is not None


def resolve(
    annotation_id,
    query: Tuple[str, str, str],
    request: dict,
    species: str,
    data_source: str,
    db_type: str,
) -> Tuple[str, Optional[str]]:
    """
    Decide how this annotation should obtain results.

    Returns:
        ("disabled", None) — run normal pipeline
        ("leader", None) — this task executes the DB queries
        ("follower", leader_id) — wait for leader and replicate
        ("cache", ref_id) — replicate from a recent completed annotation (no DB)
    """
    if not is_enabled():
        return ("disabled", None)

    sig = compute_signature(query, request, species, data_source, db_type)
    active_key = f"{PREFIX}:active:{sig}"
    aid = str(annotation_id)

    # In-flight leader must win over a stale result_ref, or followers would
    # attach to an old completed annotation while a new run is executing.
    leader_raw = redis_client.get(active_key)
    leader_id = _decode(leader_raw) if leader_raw else ""
    if leader_id and leader_id != aid:
        followers_key = f"{PREFIX}:followers:{sig}"
        redis_client.sadd(followers_key, aid)
        redis_client.expire(followers_key, ACTIVE_TTL)
        redis_client.setex(f"{PREFIX}:sig_for:{aid}", ACTIVE_TTL, sig)
        return ("follower", leader_id)

    # Warm path: no in-flight leader; reuse recent identical result if Redis still holds it.
    ref_key = f"{PREFIX}:result_ref:{sig}"
    cached = redis_client.get(ref_key)
    if cached:
        ref_id = _decode(cached)
        if ref_id and _cache_still_valid(ref_id):
            return ("cache", ref_id)
        redis_client.delete(ref_key)

    if redis_client.set(active_key, aid, nx=True, ex=ACTIVE_TTL):
        redis_client.setex(f"{PREFIX}:sig_for:{aid}", ACTIVE_TTL, sig)
        return ("leader", None)

    leader_raw = redis_client.get(active_key)
    leader_id = _decode(leader_raw) if leader_raw else ""
    if leader_id and leader_id != aid:
        followers_key = f"{PREFIX}:followers:{sig}"
        redis_client.sadd(followers_key, aid)
        redis_client.expire(followers_key, ACTIVE_TTL)
        redis_client.setex(f"{PREFIX}:sig_for:{aid}", ACTIVE_TTL, sig)
        return ("follower", leader_id)

    if redis_client.set(active_key, aid, nx=True, ex=ACTIVE_TTL):
        redis_client.setex(f"{PREFIX}:sig_for:{aid}", ACTIVE_TTL, sig)
        return ("leader", None)

    _LOG.warning("query_dedup: ambiguous leader race for sig=%s; claiming active slot", sig[:12])
    redis_client.set(active_key, aid, ex=ACTIVE_TTL)
    redis_client.setex(f"{PREFIX}:sig_for:{aid}", ACTIVE_TTL, sig)
    return ("leader", None)


def notify_leader_ended(annotation_id, status: str) -> None:
    """
    Release single-flight lock and optionally register result pointer for cache hits.
    Call when a dedup leader reaches a terminal state (COMPLETE, FAILED, CANCELLED).
    """
    if not is_enabled():
        return

    aid = str(annotation_id)
    sig_raw = redis_client.get(f"{PREFIX}:sig_for:{aid}")
    if not sig_raw:
        return

    sig = _decode(sig_raw)
    # Publish result pointer before releasing the active lock so concurrent
    # requests never see neither active nor result_ref (which would start a duplicate run).
    if status == TaskStatus.COMPLETE.value:
        redis_client.setex(f"{PREFIX}:result_ref:{sig}", EXP, aid)
    redis_client.delete(f"{PREFIX}:active:{sig}")
    redis_client.delete(f"{PREFIX}:sig_for:{aid}")


def cleanup_follower(annotation_id) -> None:
    """Remove follower from the waiting set after replication or timeout."""
    if not is_enabled():
        return

    aid = str(annotation_id)
    sig_raw = redis_client.get(f"{PREFIX}:sig_for:{aid}")
    if not sig_raw:
        return
    sig = _decode(sig_raw)
    redis_client.srem(f"{PREFIX}:followers:{sig}", aid)
    redis_client.delete(f"{PREFIX}:sig_for:{aid}")
