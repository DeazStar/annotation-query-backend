import re
import os
from copy import deepcopy
from numbers import Number
from typing import Any, Iterable


_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")
_OPERATORS = {">", ">=", "<", "<=", "==", "!="}
_EVIDENCE_FIELDS = {
    "score",
    "p_value",
    "confidence",
    "evidence_score",
    "publication_count",
    "support",
}
_FILTER_KEYS = {"allow", "exclude"}


def _canonical_edge_type(value: Any) -> str:
    return str(value).strip().replace(" ", "_")


def _resource_limit(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        return value if value >= 0 else default
    except (TypeError, ValueError):
        return default


MAX_FRONTIER_SIZE = _resource_limit("MAX_FRONTIER_SIZE", 10_000)
MAX_EXPANDED_PATHS = _resource_limit("MAX_EXPANDED_PATHS", 10_000)
MAX_RESULT_ROWS = _resource_limit("MAX_RESULT_ROWS", 1_000)


class PathSearchResourceError(RuntimeError):
    """The path search exceeded a configured bounded-work limit."""


class PathRequestError(ValueError):
    """A client supplied an invalid path request."""


def _fail(message: str) -> None:
    raise PathRequestError(message)


def _validate_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        _fail(f"Invalid {label}")
    return value


def _normalise_type_filter(value: Any, label: str) -> dict:
    if value is None:
        return {"allow": [], "exclude": []}
    if not isinstance(value, dict) or set(value) - _FILTER_KEYS:
        _fail(f"Invalid {label}")
    result = {}
    for key in _FILTER_KEYS:
        raw = value.get(key, [])
        if not isinstance(raw, list) or not all(isinstance(item, str) and item for item in raw):
            _fail(f"Invalid {label}.{key}")
        if label == "edge_types":
            result[key] = sorted({_canonical_edge_type(item) for item in raw})
        else:
            result[key] = sorted(set(raw))
    if set(result["allow"]) & set(result["exclude"]):
        _fail(f"{label} allow and exclude overlap")
    return result


def _normalise_evidence_filters(value: Any) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        _fail("Invalid evidence_filters")
    filters = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"field", "operator", "value"}:
            _fail("Malformed evidence filter")
        field, operator, expected = item["field"], item["operator"], item["value"]
        if field not in _EVIDENCE_FIELDS or operator not in _OPERATORS:
            _fail("Malformed evidence filter")
        if not isinstance(expected, Number) or isinstance(expected, bool):
            _fail("Malformed evidence filter")
        filters.append({"field": field, "operator": operator, "value": expected})
    return filters


def _schema_types(schema_manager: Any, species: str) -> tuple[set[str], set[str]]:
    representation = getattr(schema_manager, "full_schema_representation", {}) or {}
    species_schema = representation.get(species, {})
    nodes = set((species_schema.get("nodes") or {}).keys())
    edges = set((species_schema.get("edges") or {}).keys())
    nodes.update(
        value.get("label")
        for value in (species_schema.get("nodes") or {}).values()
        if isinstance(value, dict) and value.get("label")
    )
    edges.update(
        value.get("label")
        for value in (species_schema.get("edges") or {}).values()
        if isinstance(value, dict) and value.get("label")
    )
    if not nodes and not edges:
        filtered = getattr(schema_manager, "filter_schema", {}) or {}
        for value in (filtered.get(species, {}) or {}).values():
            if value.get("source"):
                nodes.add(value["source"])
            if value.get("target"):
                nodes.add(value["target"])
            if value.get("id"):
                edges.add(value["id"])
            if value.get("label"):
                edges.add(value["label"])
    return nodes, edges


def normalize_request(request: dict, schema_manager: Any = None) -> dict:
    if not isinstance(request, dict):
        _fail("Request must be an object")
    required = {"sources", "targets"}
    if not required.issubset(request):
        _fail("sources and targets are required")
    species = request.get("species", "human")
    if species not in {"human", "fly"}:
        _fail("Invalid species")
    max_hops = request.get("max_hops", 3)
    k = request.get("k", 20)
    if not isinstance(max_hops, int) or isinstance(max_hops, bool) or not 1 <= max_hops <= 4:
        _fail("max_hops must be between 1 and 4")
    if not isinstance(k, int) or isinstance(k, bool) or not 1 <= k <= 100:
        _fail("k must be between 1 and 100")

    def endpoints(value: Any, label: str) -> list[dict]:
        if not isinstance(value, list) or not 1 <= len(value) <= 50:
            _fail(f"{label} must contain between 1 and 50 nodes")
        result = []
        for item in value:
            if not isinstance(item, dict) or set(item) != {"type", "id"}:
                _fail(f"Invalid {label} node")
            node_type = item["type"]
            if not isinstance(node_type, str) or not node_type.strip():
                _fail(f"Invalid {label} node type")
            result.append({"type": node_type, "id": _validate_id(item["id"], f"{label} node id")})
        return result

    sources, targets = endpoints(request["sources"], "sources"), endpoints(request["targets"], "targets")
    node_types, edge_types = _schema_types(schema_manager, species) if schema_manager else (set(), set())
    requested_node_types = {n["type"] for n in sources + targets}
    for node_type in requested_node_types:
        if node_types and node_type not in node_types:
            _fail(f"Unknown node type: {node_type}")

    edge_filter = _normalise_type_filter(request.get("edge_types"), "edge_types")
    intermediate_filter = _normalise_type_filter(
        request.get("intermediate_node_types"), "intermediate_node_types"
    )
    for edge_type in edge_filter["allow"] + edge_filter["exclude"]:
        if edge_types and edge_type not in edge_types:
            _fail(f"Unknown edge type: {edge_type}")
    for node_type in intermediate_filter["allow"] + intermediate_filter["exclude"]:
        if node_types and node_type not in node_types:
            _fail(f"Unknown node type: {node_type}")

    hub = request.get("hub_avoidance", True)
    if isinstance(hub, bool):
        hub = {
            "enabled": hub,
            "max_degree": request.get("max_degree", 100),
            "exclude_ids": request.get("exclude_ids", []),
        }
    if not isinstance(hub, dict) or set(hub) - {"enabled", "max_degree", "exclude_ids"}:
        _fail("Invalid hub_avoidance")
    enabled = hub.get("enabled", True)
    max_degree = hub.get("max_degree", 100)
    exclude_ids = hub.get("exclude_ids", [])
    if not isinstance(enabled, bool) or not isinstance(max_degree, int) or max_degree < 0:
        _fail("Invalid hub_avoidance")
    if not isinstance(exclude_ids, list) or not all(isinstance(i, str) and _ID_RE.fullmatch(i) for i in exclude_ids):
        _fail("Invalid hub_avoidance.exclude_ids")

    return {
        "sources": sources,
        "targets": targets,
        "species": species,
        "max_hops": max_hops,
        "k": k,
        "edge_types": edge_filter,
        "intermediate_node_types": intermediate_filter,
        "evidence_filters": _normalise_evidence_filters(request.get("evidence_filters")),
        "hub_avoidance": {
            "enabled": enabled,
            "max_degree": max_degree,
            "exclude_ids": sorted(set(exclude_ids)),
        },
    }


def _compare(actual: Any, operator: str, expected: Number) -> bool:
    if not isinstance(actual, Number):
        return False
    return {
        ">": actual > expected,
        ">=": actual >= expected,
        "<": actual < expected,
        "<=": actual <= expected,
        "==": actual == expected,
        "!=": actual != expected,
    }[operator]


def _candidate_score(path: dict) -> float:
    if isinstance(path.get("score"), Number):
        return float(path["score"])
    scores = [e.get("evidence", {}).get("score") for e in path.get("edges", [])]
    numeric = [float(s) for s in scores if isinstance(s, Number)]
    return sum(numeric) / len(numeric) if numeric else 0.0


def _matches(path: dict, request: dict) -> bool:
    nodes, edges = path.get("nodes", []), path.get("edges", [])
    if not nodes or not edges or len(edges) != len(nodes) - 1 or len(edges) > request["max_hops"]:
        return False
    ids = [node.get("id") for node in nodes]
    if len(ids) != len(set(ids)):
        return False
    source_ids = {n["id"] for n in request["sources"]}
    target_ids = {n["id"] for n in request["targets"]}
    if ids[0] not in source_ids or ids[-1] not in target_ids:
        return False
    source_types = {n["type"] for n in request["sources"] if n["id"] == ids[0]}
    target_types = {n["type"] for n in request["targets"] if n["id"] == ids[-1]}
    if nodes[0].get("type") not in source_types or nodes[-1].get("type") not in target_types:
        return False
    edge_filter = request["edge_types"]
    intermediate_filter = request["intermediate_node_types"]
    for index, edge in enumerate(edges):
        edge_type = _canonical_edge_type(edge.get("type"))
        if edge_type in edge_filter["exclude"] or (
            edge_filter["allow"] and edge_type not in edge_filter["allow"]
        ):
            return False
        if edge.get("source") != ids[index] or edge.get("target") != ids[index + 1]:
            # Candidate traversal may be reverse to stored edge direction.
            if {edge.get("source"), edge.get("target")} != {ids[index], ids[index + 1]}:
                return False
        evidence = edge.get("evidence") or {}
        if any(not _compare(evidence.get(f["field"]), f["operator"], f["value"])
               for f in request["evidence_filters"]):
            return False
    hub = request["hub_avoidance"]
    for node in nodes[1:-1]:
        if node.get("id") in hub["exclude_ids"]:
            return False
        if hub["enabled"] and isinstance(node.get("properties", {}).get("degree"), Number):
            if node["properties"]["degree"] > hub["max_degree"]:
                return False
        if node.get("type") in intermediate_filter["exclude"] or (
            intermediate_filter["allow"] and node.get("type") not in intermediate_filter["allow"]
        ):
            return False
    return True


def _path_key(path: dict) -> tuple:
    return (
        len(path["edges"]),
        -_candidate_score(path),
        tuple(str(n.get("id", "")) for n in path["nodes"]),
        tuple(_canonical_edge_type(e.get("type")) for e in path["edges"]),
    )


def _subgraph(paths: Iterable[dict]) -> dict:
    nodes, edges = {}, {}
    for path in paths:
        for node in path["nodes"]:
            nodes[node["id"]] = {
                "data": {
                    "id": node["id"],
                    "type": node.get("type"),
                    "properties": node.get("properties", {}),
                }
            }
        for edge in path["edges"]:
            normalized_edge = deepcopy(edge)
            normalized_edge["type"] = _canonical_edge_type(normalized_edge.get("type"))
            key = (
                normalized_edge.get("source"),
                normalized_edge.get("target"),
                normalized_edge["type"],
            )
            edges[key] = {"data": normalized_edge}
    return {"nodes": list(nodes.values()), "edges": list(edges.values())}


def process_candidates(result: dict, request: dict) -> dict:
    candidates = result.get("candidates", []) if isinstance(result, dict) else []
    if len(candidates) > MAX_RESULT_ROWS:
        raise PathSearchResourceError(
            f"Path search exceeded MAX_RESULT_ROWS ({MAX_RESULT_ROWS})"
        )
    valid = [deepcopy(path) for path in candidates if isinstance(path, dict) and _matches(path, request)]
    valid.sort(key=_path_key)
    selected, has_more = valid[: request["k"]], len(valid) > request["k"]
    paths = []
    for path in selected:
        score = _candidate_score(path)
        normalized_edges = deepcopy(path["edges"])
        for edge in normalized_edges:
            edge["type"] = _canonical_edge_type(edge.get("type"))
        paths.append({
            "nodes": path["nodes"],
            "edges": normalized_edges,
            "hop_count": len(path["edges"]),
            "score": score,
            "score_fields": sorted({
                key for edge in path["edges"] for key, value in (edge.get("evidence") or {}).items()
                if isinstance(value, Number)
            }),
        })
    return {
        "paths": paths,
        "subgraph": _subgraph(selected),
        "has_more": has_more or bool(result.get("truncated", False)),
        "unresolved_ids": sorted(set(result.get("unresolved_ids", []))),
        "truncated": bool(result.get("truncated", False)),
        "reason": None if paths else f"no path within {request['max_hops']} hops",
    }
