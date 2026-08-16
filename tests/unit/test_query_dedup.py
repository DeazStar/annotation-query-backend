"""Unit tests for signature-based query deduplication (Redis layer)."""
import pytest
from unittest.mock import MagicMock, patch


QUERY = ("MATCH (n) RETURN n LIMIT 1", "COUNT 1", "COUNT 2")
REQ = {"nodes": [{"id": "a", "type": "Gene", "node_id": "x"}], "predicates": []}


def test_compute_signature_stable():
    from app.lib.query_dedup import compute_signature

    s1 = compute_signature(QUERY, REQ, "human", "all", "cypher")
    s2 = compute_signature(QUERY, REQ, "human", "all", "cypher")
    assert s1 == s2
    assert len(s1) == 64


def test_compute_signature_differs_by_species():
    from app.lib.query_dedup import compute_signature

    a = compute_signature(QUERY, REQ, "human", "all", "cypher")
    b = compute_signature(QUERY, REQ, "fly", "all", "cypher")
    assert a != b


def test_resolve_leader_on_first_acquire():
    mock_redis = MagicMock()
    mock_redis.get.return_value = None
    mock_redis.set.return_value = True  # NX wins

    with patch("app.lib.query_dedup.redis_client", mock_redis):
        from app.lib import query_dedup

        with patch.object(query_dedup, "is_enabled", return_value=True):
            mode, leader = query_dedup.resolve(
                "ann1", QUERY, REQ, "human", "all", "cypher"
            )
    assert mode == "leader"
    assert leader is None
    mock_redis.set.assert_called()


def test_resolve_follower_when_active_exists():
    mock_redis = MagicMock()
    # First GET is qdedup:active:{sig} — another annotation holds the lock
    mock_redis.get.side_effect = [b"leader99"]
    mock_redis.set.return_value = False

    with patch("app.lib.query_dedup.redis_client", mock_redis):
        from app.lib import query_dedup

        with patch.object(query_dedup, "is_enabled", return_value=True):
            mode, leader = query_dedup.resolve(
                "ann2", QUERY, REQ, "human", "all", "cypher"
            )
    assert mode == "follower"
    assert leader == "leader99"


def test_resolve_disabled():
    from app.lib import query_dedup

    with patch.object(query_dedup, "is_enabled", return_value=False):
        mode, leader = query_dedup.resolve(
            "ann1", QUERY, REQ, "human", "all", "cypher"
        )
    assert mode == "disabled"
    assert leader is None


def test_notify_leader_releases_active_and_sets_result_ref():
    mock_redis = MagicMock()
    mock_redis.get.return_value = b"abc_sig_hex"

    with patch("app.lib.query_dedup.redis_client", mock_redis):
        from app.lib import query_dedup
        from app.constants import TaskStatus

        with patch.object(query_dedup, "is_enabled", return_value=True):
            query_dedup.notify_leader_ended("ann1", TaskStatus.COMPLETE.value)

    mock_redis.delete.assert_any_call(f"{query_dedup.PREFIX}:active:abc_sig_hex")
    mock_redis.setex.assert_called()
