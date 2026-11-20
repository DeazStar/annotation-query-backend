import re
from copy import deepcopy
from types import SimpleNamespace

import pytest

from app.lib.db_resilience import QueryTimeoutError, QueryType
from app.services import mork_paths
from app.services.mork_paths import MorkPathMixin
from app.services.path_search import (
    PathRequestError,
    normalize_request,
    process_candidates,
)
from tests.fixtures.path_graph import (
    CANDIDATES,
    FULL_CANDIDATES,
    FULL_EDGES,
    FULL_NODES,
    HUB_LEAF_IDS,
)
from tests.fixtures.path_graph_to_metta import write_fixture


NODE_TYPES = {"gene", "protein", "pathway"}
EDGE_TYPES = {"interacts_with", "participates_in", "regulates"}


class FakeMorkPaths(MorkPathMixin):
    path_node_types = NODE_TYPES
    path_edge_types = EDGE_TYPES

    def __init__(self, duplicate=False):
        self.nodes = {(node["type"], node["id"]): node for node in FULL_NODES}
        self.edges = []
        for edge in FULL_EDGES:
            self.edges.append({
                "predicate": edge["type"].replace(" ", "_"),
                "source": (edge["source_type"], edge["source"]),
                "target": (edge["target_type"], edge["target"]),
                "evidence": edge["evidence"],
            })
        if duplicate:
            self.edges.append(deepcopy(self.edges[0]))
        self.patterns = []

    def _run_pattern(self, pattern_str, template_str):
        self.patterns.append(pattern_str)
        resolve = re.fullmatch(r"\((\w+) ([A-Za-z0-9_.:/-]+)\)", pattern_str)
        if resolve:
            return [f"(resolved ({resolve.group(1)} {resolve.group(2)}))"] if (
                resolve.group(1), resolve.group(2)
            ) in self.nodes else []

        property_match = re.fullmatch(
            r"\(\$property \((\w+) ([^)]+)\) \$value\)", pattern_str
        )
        if property_match:
            node = self.nodes.get((property_match.group(1), property_match.group(2)))
            return [
                f"(node_property {key} ({property_match.group(1)} {property_match.group(2)}) {value})"
                for key, value in (node or {}).get("properties", {}).items()
            ]

        evidence_match = re.fullmatch(
            r"\(\$property \((\w+) \((\w+) ([^)]+)\) \((\w+) ([^)]+)\)\) \$value\)",
            pattern_str,
        )
        if evidence_match:
            predicate, source_type, source_id, target_type, target_id = evidence_match.groups()
            for edge in self.edges:
                if (
                    edge["predicate"], edge["source"], edge["target"]
                ) == (predicate, (source_type, source_id), (target_type, target_id)):
                    return [
                        f"(ev {key} {value})"
                        for key, value in edge["evidence"].items()
                    ]
            return []

        current_match = re.search(r"\((\w+) ([A-Za-z0-9_.:/-]+)\)", pattern_str)
        wildcard = "$predicate" in pattern_str
        if wildcard:
            outgoing_batch = re.search(
                r"^\(\$predicate \((\w+) \$id\) \(\$neighbor_type \$neighbor_id\)\)$",
                pattern_str,
            )
            incoming_batch = re.search(
                r"^\(\$predicate \(\$neighbor_type \$neighbor_id\) \((\w+) \$id\)\)$",
                pattern_str,
            )
            if outgoing_batch:
                node_type = outgoing_batch.group(1)
                return [
                    f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                    f"({edge['target'][0]} {edge['target'][1]}))"
                    for edge in self.edges
                    if edge["source"][0] == node_type
                ]
            if incoming_batch:
                node_type = incoming_batch.group(1)
                return [
                    f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                    f"({edge['target'][0]} {edge['target'][1]}))"
                    for edge in self.edges
                    if edge["target"][0] == node_type
                ]

            outgoing = re.search(
                r"^\(\$predicate \((\w+) ([A-Za-z0-9_.:/-]+)\) "
                r"\(\$neighbor_type \$neighbor_id\)\)$",
                pattern_str,
            )
            incoming = re.search(
                r"^\(\$predicate \(\$neighbor_type \$neighbor_id\) "
                r"\((\w+) ([A-Za-z0-9_.:/-]+)\)\)$",
                pattern_str,
            )
            if outgoing:
                current = (outgoing.group(1), outgoing.group(2))
                return [
                    f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                    f"({edge['target'][0]} {edge['target'][1]}))"
                    for edge in self.edges
                    if edge["source"] == current
                ]
            if incoming:
                current = (incoming.group(1), incoming.group(2))
                return [
                    f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                    f"({edge['target'][0]} {edge['target'][1]}))"
                    for edge in self.edges
                    if edge["target"] == current
                ]
        if current_match:
            current = (current_match.group(1), current_match.group(2))
            reverse = pattern_str.find(f"({current[0]} {current[1]})") > pattern_str.find(
                "($neighbor_type $neighbor_id)"
            )
            result = []
            for edge in self.edges:
                if reverse and edge["target"] == current:
                    result.append(
                        f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                        f"({edge['target'][0]} {edge['target'][1]}))"
                    )
                elif not reverse and edge["source"] == current:
                    result.append(
                        f"(path_edge {edge['predicate']} ({edge['source'][0]} {edge['source'][1]})"
                        f"({edge['target'][0]} {edge['target'][1]}))"
                    )
            if wildcard:
                return result
            predicate = pattern_str.split("(", 2)[1].split()[0]
            parsed = MorkPathMixin._atom_tuples(result)
            return [
                f"(path_edge {atom[1]} ({atom[2][0]} {atom[2][1]})"
                f"({atom[3][0]} {atom[3][1]}))"
                for atom in parsed
                if len(atom) == 4 and atom[1] == predicate
            ]
        return []


def request(**changes):
    value = {
        "sources": [{"type": "gene", "id": "gene-a"}],
        "targets": [{"type": "gene", "id": "gene-b"}],
    }
    value.update(changes)
    return normalize_request(value)


def expected_candidates():
    candidates = deepcopy(FULL_CANDIDATES)
    degrees = {}
    for edge in FULL_EDGES:
        degrees[edge["source"]] = degrees.get(edge["source"], 0) + 1
        degrees[edge["target"]] = degrees.get(edge["target"], 0) + 1
    for candidate in candidates:
        for node in candidate["nodes"]:
            node["properties"]["degree"] = degrees[node["id"]]
    return candidates


def test_mork_paths_match_fixture_paths_and_dedupe_edges():
    backend = FakeMorkPaths(duplicate=True)
    normalized = request(hub_avoidance={"enabled": False})
    actual = process_candidates(backend.find_paths(normalized), normalized)
    expected = process_candidates({"candidates": expected_candidates()}, normalized)
    assert actual["paths"] == expected["paths"]
    assert all(
        len({
            (edge["source"], edge["target"], edge["type"])
            for edge in path["edges"]
        }) == len(path["edges"])
        for path in actual["paths"]
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"edge_types": {"allow": ["interacts with", "regulates"]}},
        {"edge_types": {"exclude": ["participates in"]}},
        {"intermediate_node_types": {"exclude": ["pathway"]}},
        {"evidence_filters": [{"field": "p_value", "operator": "<", "value": 0.05}]},
        {"hub_avoidance": {"enabled": False, "exclude_ids": ["protein-a"]}},
        {"max_hops": 2},
    ],
)
def test_mork_paths_apply_path_filters(changes):
    backend = FakeMorkPaths()
    normalized = request(**changes)
    actual = process_candidates(backend.find_paths(normalized), normalized)
    expected = process_candidates({"candidates": expected_candidates()}, normalized)
    assert actual == expected


def test_mork_paths_handle_unknown_and_hostile_ids():
    backend = FakeMorkPaths()
    unknown = request(sources=[{"type": "gene", "id": "unknown"}])
    result = backend.find_paths(unknown)
    assert result["unresolved_ids"] == ["unknown"]
    patterns = list(backend.patterns)
    with pytest.raises(ValueError):
        backend.find_paths(request(sources=[{"type": "gene", "id": '"'}]))
    assert backend.patterns == patterns


def test_empty_path_schema_whitelist_raises(monkeypatch):
    backend = FakeMorkPaths()
    backend.path_node_types = set()
    backend.path_edge_types = set()
    monkeypatch.setattr(
        "app.services.cypher_generator._path_schema_whitelist",
        lambda: (set(), set()),
    )
    with pytest.raises(ValueError, match="path schema whitelist is empty"):
        backend.find_paths(request())


def test_hostile_predicate_and_node_type_are_rejected_before_patterns():
    backend = FakeMorkPaths()
    backend.path_edge_types = {"bad)"}
    with pytest.raises(ValueError, match="Invalid predicate"):
        backend.find_paths(request())
    assert backend.patterns == []

    backend = FakeMorkPaths()
    normalized = request()
    normalized["sources"][0]["type"] = "bad)"
    with pytest.raises(ValueError, match="Invalid node type"):
        backend.find_paths(normalized)
    assert backend.patterns == []


@pytest.mark.parametrize("hostile_id", ['"', "`", "(", ")", "$", "bad id"])
def test_hostile_ids_are_rejected_before_patterns(hostile_id):
    backend = FakeMorkPaths()
    normalized = request()
    normalized["sources"][0]["id"] = hostile_id
    with pytest.raises(ValueError, match="Invalid node id"):
        backend.find_paths(normalized)
    assert backend.patterns == []


@pytest.mark.parametrize(
    ("hostile", "field"),
    [
        ("'", "id"),
        ('"', "id"),
        ("`", "id"),
        ("(", "id"),
        (")", "id"),
        ("\\", "id"),
        (";", "id"),
        ("\n", "id"),
        ("x" * 300, "id"),
        ("bad)", "node_type"),
        ("interacts_with; injected", "edge_type"),
        ("score)\n(injected", "filter_value"),
    ],
)
def test_mork_hostile_query_values_are_rejected(hostile, field):
    backend = FakeMorkPaths()
    try:
        if field == "id":
            normalized = normalize_request({
                "sources": [{"type": "gene", "id": hostile}],
                "targets": [{"type": "gene", "id": "gene-b"}],
            })
        elif field == "node_type":
            normalized = normalize_request({
                "sources": [{"type": hostile, "id": "gene-a"}],
                "targets": [{"type": "gene", "id": "gene-b"}],
            })
        elif field == "edge_type":
            normalized = normalize_request({
                "sources": [{"type": "gene", "id": "gene-a"}],
                "targets": [{"type": "gene", "id": "gene-b"}],
                "edge_types": {"allow": [hostile]},
            })
        else:
            normalized = normalize_request({
                "sources": [{"type": "gene", "id": "gene-a"}],
                "targets": [{"type": "gene", "id": "gene-b"}],
                "evidence_filters": [
                    {"field": "score", "operator": ">=", "value": hostile}
                ],
            })
        backend.find_paths(normalized)
    except (ValueError, PathRequestError):
        pass
    else:
        raise AssertionError(f"hostile MORK {field} was accepted")
    assert not any(
        hostile in pattern for pattern in backend.patterns
    )


def test_atom_tuples_handles_inconsistent_mork_spacing():
    atoms = [
        "(path_edge interacts_with (gene gene-a) (protein hub))",
        "(path_edge interacts_with (gene gene-a)(protein protein-a))",
        "(ev score 900)",
    ]
    assert MorkPathMixin._atom_tuples(atoms) == [
        ("path_edge", "interacts_with", ("gene", "gene-a"), ("protein", "hub")),
        ("path_edge", "interacts_with", ("gene", "gene-a"), ("protein", "protein-a")),
        ("ev", "score", "900"),
    ]


def test_mork_evidence_numeric_strings_are_converted():
    assert MorkPathMixin._value("900") == 900
    assert MorkPathMixin._value("3.25") == 3.25
    assert MorkPathMixin._value("high") == "high"


def test_mork_space_edge_type_is_canonicalized_to_underscore():
    candidate = deepcopy(CANDIDATES[1])
    candidate["edges"][0]["type"] = "interacts with"
    candidate["edges"][1]["type"] = "regulates"
    normalized = normalize_request({
        "sources": [{"type": "gene", "id": "gene-a"}],
        "targets": [{"type": "gene", "id": "gene-b"}],
        "edge_types": {"allow": ["interacts_with", "regulates"]},
        "hub_avoidance": {"enabled": False, "max_degree": 100, "exclude_ids": []},
    })
    result = process_candidates({"candidates": [candidate]}, normalized)

    assert result["paths"]
    assert result["paths"][0]["edges"][0]["type"] == "interacts_with"
    assert all(
        edge["type"] in {"interacts_with", "regulates"}
        for edge in result["paths"][0]["edges"]
    )


def test_node_properties_ignore_edge_and_id_atoms():
    class PropertyAtomBackend(FakeMorkPaths):
        def _run_pattern(self, pattern_str, template_str):
            if pattern_str.startswith("($property (pathway pathway-a)"):
                return [
                    "(name (pathway pathway-a) neo4j)",
                    "(regulates (pathway pathway-a) (gene gene-b))",
                    "(id (pathway pathway-a) pathway-a)",
                ]
            return []

    backend = PropertyAtomBackend()
    deadline = mork_paths.time.monotonic() + mork_paths._PATH_TIMEOUT_SECONDS
    degree_cache = {("degree", ("pathway", "pathway-a")): 2}
    properties = backend._node_properties(
        ("pathway", "pathway-a"),
        deadline,
        {},
        degree_cache,
    )
    assert properties == {"name": "neo4j", "degree": 2}


def test_path_graph_metta_fixture_matches_full_graph(tmp_path):
    write_fixture(tmp_path)
    text = (tmp_path / "path_graph.metta").read_text(encoding="utf-8")
    lines = text.splitlines()

    for edge in FULL_EDGES:
        predicate = edge["type"].replace(" ", "_")
        edge_line = (
            f"({predicate} ({edge['source_type']} {edge['source']}) "
            f"({edge['target_type']} {edge['target']})"
        )
        matches = [line for line in lines if line.startswith(edge_line)]
        assert len(matches) == 1
        for key, value in edge["evidence"].items():
            evidence_matches = [
                line for line in lines
                if line.startswith(f"({key} ({predicate} ")
                and str(value) in line
                and edge["source"] in line
                and edge["target"] in line
            ]
            assert len(evidence_matches) == 1

    assert not any(line.startswith("(degree ") for line in lines)
    hub_leaf_edges = [
        edge for edge in FULL_EDGES
        if edge["source"] == "hub" and edge["target"] in HUB_LEAF_IDS
    ]
    assert len(hub_leaf_edges) >= 5


def test_mork_degree_uses_neighbor_edge_atoms():
    backend = FakeMorkPaths()
    normalized = request(hub_avoidance={"enabled": False})
    deadline = mork_paths.time.monotonic() + mork_paths._PATH_TIMEOUT_SECONDS
    cache = {}

    degree = backend._degree(("protein", "hub"), deadline, cache)
    expected = sum(
        1
        for edge in backend.edges
        if edge["source"] == ("protein", "hub") or edge["target"] == ("protein", "hub")
    )
    assert degree == expected


def test_mork_paths_bounded_traversal_call_count():
    backend = FakeMorkPaths()
    normalized = request(max_hops=3, hub_avoidance={"enabled": False})
    backend.find_paths(normalized)

    traversal_calls = [
        pattern for pattern in backend.patterns
        if "$predicate" in pattern
        and "$neighbor_type" in pattern
    ]
    visited_nodes = {
        (node["type"], node["id"])
        for node in FULL_NODES
    }
    evidence_calls = [
        pattern for pattern in backend.patterns
        if "$property" in pattern and "($property" in pattern
    ]
    assert len(traversal_calls) <= 2 * len(visited_nodes) + len(evidence_calls)


def test_mork_paths_timeout_and_truncation(monkeypatch):
    backend = FakeMorkPaths()
    original_monotonic = mork_paths.time.monotonic
    values = iter([0.0, 11.0])
    monkeypatch.setattr(mork_paths.time, "monotonic", lambda: next(values, 11.0))
    with pytest.raises(QueryTimeoutError):
        backend.find_paths(request())
    monkeypatch.setattr(mork_paths.time, "monotonic", original_monotonic)

    for index in range(6):
        middle = ("protein", f"protein-{index}")
        backend.nodes[middle] = {"id": middle[0], "type": middle[1], "properties": {}}
        backend.edges.extend([
            {"predicate": "interacts_with", "source": ("gene", "gene-a"),
             "target": middle, "evidence": {"score": 1, "p_value": 1.0}},
            {"predicate": "regulates", "source": middle, "target": ("gene", "gene-b"),
             "evidence": {"score": 1, "p_value": 1.0}},
        ])
    normalized = request(k=1, hub_avoidance={"enabled": False})
    result = backend.find_paths(normalized)
    assert result["truncated"] is True
    assert len(result["candidates"]) == 5
