import os
import re
import logging
import time
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.api.api_v1.endpoints import paths as paths_endpoint
from app.lib.db_resilience import (
    QueryType,
    QueryTimeoutConfig,
    ResilientDriver,
    RetryPolicy,
)
from app.services.cypher_generator import (
    CypherQueryGenerator,
    _path_schema_whitelist,
)
from app.services.path_search import normalize_request, process_candidates
from tests.fixtures.path_graph import FULL_EDGES, FULL_NODES


PREFIX = "TESTPATHS_"
EDGE_TYPE_MAP = {
    "interacts with": "interacts_with",
    "participates in": "participates_in",
    "regulates": "regulates",
}
NODE_TYPES = {"gene": {}, "protein": {}, "pathway": {}}
EDGE_TYPES = {value: {} for value in EDGE_TYPE_MAP.values()}
SCHEMA = SimpleNamespace(
    full_schema_representation={"human": {"nodes": NODE_TYPES, "edges": EDGE_TYPES}}
)
logger = logging.getLogger(__name__)


@dataclass
class Neo4jPathContext:
    generator: CypherQueryGenerator
    candidates: list[dict]
    source_id: str
    target_id: str
    run_id: str


def _record_value(record, key, default=None):
    return record.get(key, default) if hasattr(record, "get") else record[key]


def _ident(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise AssertionError(f"Invalid test identifier: {value}")
    return f"`{value}`"


def _prefixed_id(node_id):
    return f"{PREFIX}{node_id.replace(' ', '_')}"


def _build_test_graph(run_id):
    nodes_by_id = {}
    edges_by_key = {}

    def add_node(node):
        node_id = _prefixed_id(node["id"])
        nodes_by_id.setdefault(
            node_id,
            {
                "id": node_id,
                "type": node["type"],
                "properties": {"test_run_id": run_id},
            },
        )

    def add_edge(edge):
        source = _prefixed_id(edge["source"])
        target = _prefixed_id(edge["target"])
        edge_type = EDGE_TYPE_MAP[edge["type"]]
        key = (source, edge_type, target)
        edges_by_key.setdefault(
            key,
            {
                "source": source,
                "target": target,
                "source_type": edge["source_type"],
                "target_type": edge["target_type"],
                "type": edge_type,
                "evidence": {**edge["evidence"], "test_run_id": run_id},
            },
        )

    for node in FULL_NODES:
        add_node(node)
    for edge in FULL_EDGES:
        add_edge(edge)

    degrees = {node_id: 0 for node_id in nodes_by_id}
    for source, _, target in edges_by_key:
        degrees[source] += 1
        degrees[target] += 1
    for node_id, node in nodes_by_id.items():
        node["properties"]["degree"] = degrees[node_id]

    source_id = _prefixed_id("gene-a")
    target_id = _prefixed_id("gene-b")
    adjacency = {}
    for (source, edge_type, target), edge in edges_by_key.items():
        adjacency.setdefault(source, []).append((target, edge_type, edge))

    def enumerate_simple_paths():
        max_hops = max(1, len(nodes_by_id) - 1)
        found = []

        def visit(node_id, node_path, edge_path):
            if node_id == target_id:
                found.append({"nodes": [nodes_by_id[node] for node in node_path],
                              "edges": [edge for edge in edge_path]})
                return
            if len(edge_path) >= max_hops:
                return
            for neighbor, edge_type, edge in adjacency.get(node_id, []):
                if neighbor in node_path:
                    continue
                visit(
                    neighbor,
                    node_path + [neighbor],
                    edge_path + [edge],
                )

        visit(source_id, [source_id], [])
        return found

    candidates = enumerate_simple_paths()
    return list(nodes_by_id.values()), list(edges_by_key.values()), candidates


def _create_graph(driver, nodes, edges, run_id):
    nodes_by_type = {}
    for node in nodes:
        nodes_by_type.setdefault(node["type"], []).append(
            {"id": node["id"], "properties": node["properties"]}
        )
    for node_type, rows in nodes_by_type.items():
        query = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{_ident(node_type)} {{id: row.id}}) "
            "SET n += row.properties, n.test_run_id = $run"
        )
        driver.run_with_retry(
            query,
            query_type=QueryType.PATHS,
            params={"rows": rows, "run": run_id},
        )
    edges_by_type = {}
    for edge in edges:
        edges_by_type.setdefault(edge["type"], []).append(edge)
    for edge_type, type_edges in edges_by_type.items():
        endpoint_matches = []
        for node_type in NODE_TYPES:
            endpoint_matches.extend(
                [
                    f"OPTIONAL MATCH (source_{node_type}:{_ident(node_type)} {{id: row.source}})",
                    f"OPTIONAL MATCH (target_{node_type}:{_ident(node_type)} {{id: row.target}})",
                ]
            )
        source_nodes = ", ".join(f"source_{node_type}" for node_type in NODE_TYPES)
        target_nodes = ", ".join(f"target_{node_type}" for node_type in NODE_TYPES)
        query = (
            "UNWIND $rows AS row "
            + " ".join(endpoint_matches)
            + f" WITH row, coalesce({source_nodes}) AS source, coalesce({target_nodes}) AS target "
            f"WHERE source IS NOT NULL AND target IS NOT NULL "
            f"MERGE (source)-[r:{_ident(edge_type)}]->(target) "
            "SET r += row.evidence, r.test_run_id = $run"
        )
        driver.run_with_retry(
            query,
            query_type=QueryType.PATHS,
            params={
                "rows": [
                    {
                        "source": edge["source"],
                        "target": edge["target"],
                        "evidence": edge["evidence"],
                    }
                    for edge in type_edges
                ],
                "run": run_id,
            },
        )


def _request(context, **changes):
    value = {
        "sources": [{"type": "gene", "id": context.source_id}],
        "targets": [{"type": "gene", "id": context.target_id}],
    }
    value.update(changes)
    return normalize_request(value)


def _run_both(context, request):
    real = process_candidates(context.generator.find_paths(request), request)
    memory = process_candidates(
        {"candidates": deepcopy(context.candidates), "unresolved_ids": [], "truncated": False},
        request,
    )
    assert real == memory
    return real


@pytest.fixture(scope="module")
def neo4j_paths():
    required = ("HUMAN_NEO4J_URI", "HUMAN_NEO4J_USERNAME", "HUMAN_NEO4J_PASSWORD")
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.skip(f"Missing Neo4j configuration: {', '.join(missing)}")

    run_id = uuid4().hex
    driver = None
    try:
        driver = ResilientDriver(
            uri=os.environ["HUMAN_NEO4J_URI"],
            auth=(
                os.environ["HUMAN_NEO4J_USERNAME"],
                os.environ["HUMAN_NEO4J_PASSWORD"],
            ),
            retry_policy=RetryPolicy(max_attempts=1, retry_on_unknown=False),
            timeout_config=QueryTimeoutConfig(
                timeouts={QueryType.PATHS: float(os.getenv("PATHS_TEST_TIMEOUT", "30"))},
                fallback_results={},
            ),
        )
        driver.run_with_retry("RETURN 1 AS ok", query_type=QueryType.PATHS)
    except Exception as error:
        if driver is not None:
            driver.close()
        pytest.skip(f"Neo4j is unreachable: {error}")

    try:
        collisions = driver.run_with_retry(
            "MATCH (n) WHERE n.id STARTS WITH $prefix "
            "RETURN n.id AS id, n.test_run_id AS test_run_id",
            query_type=QueryType.PATHS,
            params={"prefix": PREFIX},
        )
        conflicting = [
            _record_value(record, "id")
            for record in collisions
            if _record_value(record, "test_run_id") != run_id
        ]
        if conflicting:
            pytest.fail(f"Existing TESTPATHS_ nodes belong to another run: {conflicting}")

        node_types, edge_types = _path_schema_whitelist()
        missing_node_types = set(NODE_TYPES) - node_types
        missing_edge_types = set(EDGE_TYPES) - edge_types
        if missing_node_types or missing_edge_types:
            pytest.fail(
                "Fixture types are missing from the Cypher whitelist: "
                f"nodes={sorted(missing_node_types)}, edges={sorted(missing_edge_types)}"
            )
        nodes, edges, candidates = _build_test_graph(run_id)
        _create_graph(driver, nodes, edges, run_id)

        generator = CypherQueryGenerator.__new__(CypherQueryGenerator)
        generator.human_driver = driver

        original_run_query = generator.run_query

        def timed_run_query(*args, **kwargs):
            started_at = time.perf_counter()
            try:
                return original_run_query(*args, **kwargs)
            finally:
                logger.info(
                    "Neo4j run_query elapsed=%.3fs query=%s",
                    time.perf_counter() - started_at,
                    args[0] if args else kwargs.get("query_code"),
                )

        generator.run_query = timed_run_query
        yield Neo4jPathContext(
            generator=generator,
            candidates=candidates,
            source_id=_prefixed_id("gene-a"),
            target_id=_prefixed_id("gene-b"),
            run_id=run_id,
        )
    finally:
        if driver is not None:
            cleanup_query = (
                "MATCH (n) WHERE n.id STARTS WITH $prefix "
                "AND n.test_run_id = $run DETACH DELETE n"
            )
            cleanup_error = None
            for attempt in range(3):
                try:
                    with driver.session() as session:
                        session.run(
                            cleanup_query,
                            parameters={"prefix": PREFIX, "run": run_id},
                        ).consume()
                    cleanup_error = None
                    break
                except Exception as error:
                    cleanup_error = error
                    logger.warning(
                        "Neo4j fixture cleanup attempt %d failed",
                        attempt + 1,
                        exc_info=True,
                    )
            driver.close()
            if cleanup_error is not None:
                raise cleanup_error


def test_real_paths_match_in_memory_fixture(neo4j_paths):
    result = _run_both(neo4j_paths, _request(neo4j_paths))
    assert result["paths"]
    assert [path["hop_count"] for path in result["paths"]] == [2, 2, 3]
    assert [path["score"] for path in result["paths"]] == [950.0, 600.0, 800.0]
    assert [
        [node["id"] for node in path["nodes"]]
        for path in result["paths"]
    ] == [
        ["TESTPATHS_gene-a", "TESTPATHS_hub", "TESTPATHS_gene-b"],
        ["TESTPATHS_gene-a", "TESTPATHS_protein-b", "TESTPATHS_gene-b"],
        [
            "TESTPATHS_gene-a",
            "TESTPATHS_protein-a",
            "TESTPATHS_pathway-a",
            "TESTPATHS_gene-b",
        ],
    ]
    assert all(edge["evidence"]["score"] for edge in result["paths"][0]["edges"])


@pytest.mark.parametrize(
    "changes",
    [
        {"edge_types": {"allow": ["interacts_with", "regulates"]}},
        {"edge_types": {"exclude": ["participates_in"]}},
        {"intermediate_node_types": {"exclude": ["pathway"]}},
        {"evidence_filters": [{"field": "p_value", "operator": "<", "value": 0.05}]},
        {"evidence_filters": [{"field": "score", "operator": ">=", "value": 700}]},
        {"hub_avoidance": {"enabled": False, "exclude_ids": ["TESTPATHS_protein-a"]}},
        {"hub_avoidance": {"enabled": True, "max_degree": 1, "exclude_ids": []}},
    ],
)
def test_real_paths_match_for_every_filter(neo4j_paths, changes):
    _run_both(neo4j_paths, _request(neo4j_paths, **changes))


def test_hub_avoidance_changes_results_without_removing_endpoints(neo4j_paths):
    avoided = _run_both(
        neo4j_paths,
        _request(neo4j_paths, hub_avoidance={"enabled": True, "max_degree": 3, "exclude_ids": []}),
    )
    allowed = _run_both(
        neo4j_paths,
        _request(neo4j_paths, hub_avoidance={"enabled": False, "max_degree": 100, "exclude_ids": []}),
    )
    assert avoided["paths"] != allowed["paths"]
    assert [path["score"] for path in avoided["paths"]] == [600.0, 800.0]
    assert [path["hop_count"] for path in avoided["paths"]] == [2, 3]
    assert [path["score"] for path in allowed["paths"]] == [950.0, 600.0, 800.0]
    assert [path["hop_count"] for path in allowed["paths"]] == [2, 2, 3]
    assert any(
        path["nodes"][1]["id"] == "TESTPATHS_hub" for path in allowed["paths"]
    )
    for path in allowed["paths"]:
        assert path["nodes"][0]["id"] == neo4j_paths.source_id
        assert path["nodes"][-1]["id"] == neo4j_paths.target_id


def test_no_path_and_partly_unknown_ids(neo4j_paths):
    no_path = _run_both(
        neo4j_paths,
        _request(
            neo4j_paths,
            targets=[{"type": "gene", "id": _prefixed_id("gene-c")}],
        ),
    )
    assert no_path["paths"] == []
    assert no_path["reason"]

    request = _request(
        neo4j_paths,
        targets=[
            {"type": "gene", "id": neo4j_paths.target_id},
            {"type": "gene", "id": f"{PREFIX}unknown"},
        ],
    )
    result = process_candidates(neo4j_paths.generator.find_paths(request), request)
    assert result["unresolved_ids"] == [f"{PREFIX}unknown"]


def test_all_unknown_ids_are_400(neo4j_paths):
    data = {
        "sources": [{"type": "gene", "id": f"{PREFIX}unknown-source"}],
        "targets": [{"type": "gene", "id": f"{PREFIX}unknown-target"}],
    }
    with pytest.raises(HTTPException) as raised:
        paths_endpoint.find_paths(
            data=data,
            current_user_id="integration-user",
            db_instance=neo4j_paths.generator,
            schema_manager=SCHEMA,
        )
    assert raised.value.status_code == 400
    assert f"{PREFIX}unknown-source" in raised.value.detail
    assert f"{PREFIX}unknown-target" in raised.value.detail


def test_real_path_search_is_deterministic(neo4j_paths):
    request = _request(neo4j_paths, k=10)
    results = [
        process_candidates(neo4j_paths.generator.find_paths(request), request)
        for _ in range(3)
    ]
    assert results[0] == results[1] == results[2]


@pytest.mark.parametrize("hostile", ['"', "`", "(", ")", "{", "}", ";"])
def test_hostile_ids_are_parameters_and_do_not_change_database(neo4j_paths, hostile):
    before = neo4j_paths.generator.run_query(
        "MATCH (n) WHERE n.id STARTS WITH $prefix RETURN count(n) AS count",
        species="human",
        query_type=QueryType.PATHS,
        params={"prefix": PREFIX},
    )[0]["count"]
    request = _request(neo4j_paths)
    request["sources"] = [{"type": "gene", "id": hostile}]
    result = neo4j_paths.generator.find_paths(request)
    after = neo4j_paths.generator.run_query(
        "MATCH (n) WHERE n.id STARTS WITH $prefix RETURN count(n) AS count",
        species="human",
        query_type=QueryType.PATHS,
        params={"prefix": PREFIX},
    )[0]["count"]
    assert result["unresolved_ids"] == [hostile]
    assert before == after


def test_truncation_sets_shared_has_more(neo4j_paths):
    driver = neo4j_paths.generator.human_driver
    rows = [{"id": f"{PREFIX}trunc-{index}"} for index in range(8)]
    create_query = (
        "UNWIND $rows AS row "
        "MERGE (middle:protein {id: row.id}) "
        "SET middle.test_run_id = $run "
        "WITH collect(middle) AS middles "
        "MATCH (source:gene {id: $source}), (target:gene {id: $target}) "
        "UNWIND middles AS middle "
        "MERGE (source)-[first:interacts_with]->(middle) "
        "SET first.score = 1, first.p_value = 1.0, first.test_run_id = $run "
        "MERGE (middle)-[second:regulates]->(target) "
        "SET second.score = 1, second.p_value = 1.0, second.test_run_id = $run"
    )
    cleanup_query = (
        "MATCH (n) WHERE n.id STARTS WITH $prefix "
        "AND n.test_run_id = $run DETACH DELETE n"
    )
    params = {
        "rows": rows,
        "run": neo4j_paths.run_id,
        "source": neo4j_paths.source_id,
        "target": neo4j_paths.target_id,
    }

    try:
        driver.run_with_retry(
            create_query,
            query_type=QueryType.PATHS,
            params=params,
        )

        request = _request(
            neo4j_paths,
            k=1,
            hub_avoidance={"enabled": True, "max_degree": 100, "exclude_ids": []},
        )
        backend_result = neo4j_paths.generator.find_paths(request)
        assert backend_result["truncated"] is True
        assert len(backend_result["candidates"]) == 5
        result = process_candidates(backend_result, request)
        assert len(result["paths"]) == 1
        assert result["has_more"] is True

        full_request = _request(
            neo4j_paths,
            k=100,
            hub_avoidance={"enabled": True, "max_degree": 100, "exclude_ids": []},
        )
        full_backend_result = neo4j_paths.generator.find_paths(full_request)
        assert full_backend_result["truncated"] is False
        full_result = process_candidates(full_backend_result, full_request)
        assert full_result["has_more"] is False
    finally:
        driver.run_with_retry(
            cleanup_query,
            query_type=QueryType.PATHS,
            params={"prefix": f"{PREFIX}trunc-", "run": neo4j_paths.run_id},
        )


def test_real_results_respect_hop_and_repeat_constraints(neo4j_paths):
    request = _request(neo4j_paths, max_hops=2, k=100)
    result = process_candidates(neo4j_paths.generator.find_paths(request), request)
    assert [path["score"] for path in result["paths"]] == [950.0, 600.0]
    assert [
        [node["id"] for node in path["nodes"]]
        for path in result["paths"]
    ] == [
        ["TESTPATHS_gene-a", "TESTPATHS_hub", "TESTPATHS_gene-b"],
        ["TESTPATHS_gene-a", "TESTPATHS_protein-b", "TESTPATHS_gene-b"],
    ]
    for path in result["paths"]:
        ids = [node["id"] for node in path["nodes"]]
        assert path["hop_count"] <= 2
        assert len(ids) == len(set(ids))
