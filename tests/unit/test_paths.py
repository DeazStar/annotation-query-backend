from copy import deepcopy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.api_v1.endpoints import paths as paths_endpoint
from app.lib.db_resilience import QueryTimeoutError, QueryType
from app.services.path_search import (
    PathSearchResourceError,
    normalize_request,
    process_candidates,
)
from tests.fixtures.path_graph import CANDIDATES, THREE_PATH_CANDIDATES


NODE_TYPES = {"gene": {}, "protein": {}, "pathway": {}}
EDGE_TYPES = {"interacts with": {}, "participates in": {}, "regulates": {}}
SCHEMA = SimpleNamespace(
    full_schema_representation={"human": {"nodes": NODE_TYPES, "edges": EDGE_TYPES}}
)


class FakeBackend:
    def __init__(self, candidates=None, unresolved_ids=None):
        self.candidates = candidates or []
        self.unresolved_ids = unresolved_ids or []
        self.requests = []

    def find_paths(self, request):
        self.requests.append(request)
        return {
            "candidates": deepcopy(self.candidates),
            "unresolved_ids": list(self.unresolved_ids),
            "truncated": False,
        }


class TimeoutBackend:
    def find_paths(self, request):
        raise QueryTimeoutError(QueryType.PATHS, 10.0)


def request(**changes):
    value = {
        "sources": [{"type": "gene", "id": "gene-a"}],
        "targets": [{"type": "gene", "id": "gene-b"}],
    }
    value.update(changes)
    return normalize_request(value)


def endpoint_request(**changes):
    value = {
        "sources": [{"type": "gene", "id": "gene-a"}],
        "targets": [{"type": "gene", "id": "gene-b"}],
    }
    value.update(changes)
    return value


def call_endpoint(backend, data=None):
    return paths_endpoint.find_paths(
        data=data or endpoint_request(),
        current_user_id="user-1",
        db_instance=backend,
        schema_manager=SCHEMA,
    )


def test_paths_rank_and_avoid_hubs_by_default():
    result = process_candidates({"candidates": deepcopy(CANDIDATES)}, request())
    assert len(result["paths"]) == 1
    assert result["paths"][0]["hop_count"] == 3


def test_hub_can_be_allowed_and_never_replaces_endpoints():
    result = process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(hub_avoidance={"enabled": False}),
    )
    assert [node["id"] for node in result["paths"][0]["nodes"]] == ["gene-a", "hub", "gene-b"]


def test_filters_and_no_repeated_nodes():
    normalized = request(
        max_hops=3,
        evidence_filters=[{"field": "score", "operator": ">=", "value": 700}],
        intermediate_node_types={"allow": ["protein", "pathway"]},
    )
    result = process_candidates({"candidates": deepcopy(CANDIDATES)}, normalized)
    assert result["paths"][0]["hop_count"] == 3
    repeated = {"nodes": [CANDIDATES[0]["nodes"][0], CANDIDATES[0]["nodes"][1],
                           CANDIDATES[0]["nodes"][1]], "edges": CANDIDATES[0]["edges"][:2]}
    assert not process_candidates({"candidates": [repeated]}, normalized)["paths"]


def test_max_hops_and_no_path():
    result = process_candidates({"candidates": deepcopy(CANDIDATES)}, request(max_hops=2))
    assert result["paths"] == []
    assert "no path" in result["reason"]


def test_deterministic_order_and_validation():
    first = process_candidates({"candidates": deepcopy(CANDIDATES)}, request(hub_avoidance={"enabled": False}))
    second = process_candidates({"candidates": list(reversed(deepcopy(CANDIDATES)))}, request(hub_avoidance={"enabled": False}))
    assert first["paths"] == second["paths"]
    for hostile in ['"', "`", "(", ")"]:
        try:
            request(sources=[{"type": "gene", "id": hostile}])
        except ValueError:
            pass
        else:
            raise AssertionError("hostile id accepted")


def test_endpoint_missing_auth_is_401():
    with pytest.raises(HTTPException) as raised:
        paths_endpoint._paths_user(None)
    assert raised.value.status_code == 401


def test_endpoint_invalid_auth_is_401(monkeypatch):
    def reject(_token):
        raise HTTPException(status_code=403, detail="invalid")

    monkeypatch.setattr(paths_endpoint, "get_current_user", reject)
    with pytest.raises(HTTPException) as raised:
        paths_endpoint._paths_user("invalid")
    assert raised.value.status_code == 401


def test_endpoint_all_unknown_ids_is_400_naming_ids():
    backend = FakeBackend(unresolved_ids=["unknown-source", "unknown-target"])
    data = endpoint_request(
        sources=[{"type": "gene", "id": "unknown-source"}],
        targets=[{"type": "gene", "id": "unknown-target"}],
    )
    with pytest.raises(HTTPException) as raised:
        call_endpoint(backend, data)
    assert raised.value.status_code == 400
    assert "unknown-source" in raised.value.detail
    assert "unknown-target" in raised.value.detail


def test_endpoint_partly_unknown_ids_returns_unresolved_ids():
    backend = FakeBackend(CANDIDATES, unresolved_ids=["unknown-target"])
    response = call_endpoint(backend, endpoint_request(
        targets=[
            {"type": "gene", "id": "gene-b"},
            {"type": "gene", "id": "unknown-target"},
        ]
    ))
    assert response["unresolved_ids"] == ["unknown-target"]
    assert backend.requests[0]["targets"][0]["id"] == "gene-b"


def test_endpoint_no_path_returns_reason():
    response = call_endpoint(FakeBackend())
    assert response["paths"] == []
    assert response["reason"] == "no path within 3 hops"


def test_endpoint_path_timeout_is_504():
    with pytest.raises(HTTPException) as raised:
        call_endpoint(TimeoutBackend())
    assert raised.value.status_code == 504


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"max_hops": 5}, "max_hops"),
        ({"max_hops": 0}, "max_hops"),
        ({"k": 101}, "k"),
        ({"sources": [{"type": "gene", "id": str(i)} for i in range(51)]}, "sources"),
        ({"targets": [{"type": "gene", "id": str(i)} for i in range(51)]}, "targets"),
    ],
)
def test_endpoint_invalid_request_limits_are_400(change, expected):
    with pytest.raises(HTTPException) as raised:
        call_endpoint(FakeBackend(), endpoint_request(**change))
    assert raised.value.status_code == 400
    assert expected in raised.value.detail


def test_endpoint_unknown_node_type_is_400():
    with pytest.raises(HTTPException) as raised:
        call_endpoint(FakeBackend(), endpoint_request(
            sources=[{"type": "not-a-node", "id": "gene-a"}]
        ))
    assert raised.value.status_code == 400


def test_endpoint_unknown_edge_type_is_400():
    with pytest.raises(HTTPException) as raised:
        call_endpoint(FakeBackend(), endpoint_request(
            edge_types={"allow": ["not-an-edge"]}
        ))
    assert raised.value.status_code == 400


@pytest.mark.parametrize(
    "evidence_filter",
    [
        {"field": "not_allowed", "operator": ">=", "value": 1},
        {"field": "score", "operator": "LIKE", "value": 1},
        {"field": "score", "operator": ">=", "value": "700"},
    ],
)
def test_endpoint_malformed_evidence_filters_are_400(evidence_filter):
    with pytest.raises(HTTPException) as raised:
        call_endpoint(FakeBackend(), endpoint_request(evidence_filters=[evidence_filter]))
    assert raised.value.status_code == 400


@pytest.mark.parametrize("hostile", ['"', "`", "(", ")", ";", "{", "}"])
def test_hostile_ids_and_filters_never_reach_backend(hostile):
    backend = FakeBackend()
    with pytest.raises(HTTPException):
        call_endpoint(backend, endpoint_request(
            sources=[{"type": "gene", "id": hostile}]
        ))
    with pytest.raises(HTTPException):
        call_endpoint(backend, endpoint_request(
            edge_types={"allow": [hostile]}
        ))
    assert backend.requests == []


@pytest.mark.parametrize(
    "node_id",
    ["ENSG00000136244", "GO:0008150", "rs12345", "HGNC:6018", "FBgn0000008"],
)
def test_realistic_ids_are_accepted(node_id):
    normalized = normalize_request({
        "sources": [{"type": "gene", "id": node_id}],
        "targets": [{"type": "gene", "id": "gene-b"}],
    })
    assert normalized["sources"][0]["id"] == node_id


def test_resource_limit_rejects_oversized_candidate_set(monkeypatch):
    import app.services.path_search as path_search

    monkeypatch.setattr(path_search, "MAX_RESULT_ROWS", 1)
    normalized = request(hub_avoidance=False)
    with pytest.raises(PathSearchResourceError, match="MAX_RESULT_ROWS"):
        path_search.process_candidates(
            {"candidates": deepcopy(THREE_PATH_CANDIDATES)},
            normalized,
        )


def test_resource_limit_allows_normal_candidate_set(monkeypatch):
    import app.services.path_search as path_search

    monkeypatch.setattr(path_search, "MAX_RESULT_ROWS", 100)
    result = path_search.process_candidates(
        {"candidates": deepcopy(THREE_PATH_CANDIDATES)},
        request(hub_avoidance=False),
    )
    assert len(result["paths"]) == 3


def test_resource_limit_maps_to_clean_503(monkeypatch):
    backend = FakeBackend(THREE_PATH_CANDIDATES)
    monkeypatch.setattr(
        "app.api.api_v1.endpoints.paths.process_candidates",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            PathSearchResourceError("MAX_EXPANDED_PATHS exceeded")
        ),
    )
    with pytest.raises(HTTPException) as raised:
        call_endpoint(backend)
    assert raised.value.status_code == 503
    assert "MAX_EXPANDED_PATHS" in raised.value.detail


def test_ranking_uses_hops_then_evidence_then_id_tie_break():
    def two_hop_via(mid_id, score):
        return {
            "nodes": [
                {"id": "gene-a", "type": "gene", "properties": {"degree": 1}},
                {"id": mid_id, "type": "protein", "properties": {"degree": 1}},
                {"id": "gene-b", "type": "gene", "properties": {"degree": 1}},
            ],
            "edges": [
                {
                    "source": "gene-a", "target": mid_id,
                    "source_type": "gene", "target_type": "protein",
                    "type": "interacts with", "evidence": {"score": score},
                },
                {
                    "source": mid_id, "target": "gene-b",
                    "source_type": "protein", "target_type": "gene",
                    "type": "regulates", "evidence": {"score": score},
                },
            ],
        }

    candidates = [
        two_hop_via("protein-z", 800),
        two_hop_via("protein-m", 800),
        two_hop_via("protein-high", 950),
        *deepcopy(THREE_PATH_CANDIDATES[:2]),
    ]
    result = process_candidates({"candidates": candidates}, request())
    assert [path["hop_count"] for path in result["paths"]] == [2, 2, 2, 3, 3]
    assert [path["nodes"][1]["id"] for path in result["paths"][:3]] == [
        "protein-high", "protein-m", "protein-z"
    ]


def test_top_k_sets_has_more():
    result = process_candidates(
        {"candidates": deepcopy(THREE_PATH_CANDIDATES)},
        request(k=2, hub_avoidance=False),
    )
    assert len(result["paths"]) == 2
    assert result["has_more"] is True


def test_subgraph_is_union_of_returned_paths_in_query_format():
    result = process_candidates(
        {"candidates": deepcopy(THREE_PATH_CANDIDATES[:2])},
        request(hub_avoidance=False),
    )
    assert set(node["data"]["id"] for node in result["subgraph"]["nodes"]) == {
        node["id"] for path in result["paths"] for node in path["nodes"]
    }
    assert all("data" in edge for edge in result["subgraph"]["edges"])


def test_returned_edges_carry_stored_evidence():
    result = process_candidates({"candidates": deepcopy(CANDIDATES)}, request())
    assert all(edge["evidence"] for edge in result["paths"][0]["edges"])


def test_edge_allow_and_exclude_filters():
    assert process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(edge_types={"allow": ["interacts with", "participates in"]}),
    )["paths"] == []
    assert process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(edge_types={"exclude": ["regulates"]}),
    )["paths"] == []


def test_intermediate_exclude_and_exclude_ids_filters():
    assert process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(intermediate_node_types={"exclude": ["protein"]}),
    )["paths"] == []
    assert process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(hub_avoidance={"enabled": False, "exclude_ids": ["protein-a", "hub"]}),
    )["paths"] == []
    result = process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(hub_avoidance={"enabled": False, "exclude_ids": ["protein-a"]}),
    )
    assert [[node["id"] for node in path["nodes"]] for path in result["paths"]] == [
        ["gene-a", "hub", "gene-b"]
    ]
    assert process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(hub_avoidance=False, exclude_ids=["gene-a", "gene-b"]),
    )["paths"]


def test_p_value_less_than_filter():
    result = process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(evidence_filters=[{"field": "p_value", "operator": "<", "value": 0.05}]),
    )
    assert len(result["paths"]) == 1


def test_endpoint_degree_limit_does_not_remove_source_or_target():
    candidates = deepcopy(CANDIDATES[:1])
    candidates[0]["nodes"][0]["properties"]["degree"] = 999
    candidates[0]["nodes"][-1]["properties"]["degree"] = 999
    data = endpoint_request(
        sources=[{"type": "gene", "id": "gene-a"}],
        targets=[{"type": "gene", "id": "gene-b"}],
        max_degree=1,
        hub_avoidance=True,
    )
    response = call_endpoint(FakeBackend(candidates), data)
    assert len(response["paths"]) == 1


def test_exclude_ids_never_removes_source_or_target():
    result = process_candidates(
        {"candidates": deepcopy(CANDIDATES)},
        request(hub_avoidance={"enabled": True, "exclude_ids": ["gene-a", "gene-b"]}),
    )
    assert len(result["paths"]) == 1


def test_disconnected_candidate_is_rejected():
    candidate = deepcopy(CANDIDATES[0])
    candidate["edges"][1]["source"] = "disconnected"
    result = process_candidates({"candidates": [candidate]}, request())
    assert result["paths"] == []


def test_zero_hop_candidate_is_rejected():
    candidate = {
        "nodes": [{"id": "gene-a", "type": "gene", "properties": {}}],
        "edges": [],
    }
    normalized = request(
        sources=[{"type": "gene", "id": "gene-a"}],
        targets=[{"type": "gene", "id": "gene-a"}],
    )
    assert process_candidates({"candidates": [candidate]}, normalized)["paths"] == []
