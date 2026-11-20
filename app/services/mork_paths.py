import re
import time
from copy import deepcopy
from numbers import Number

from app.lib.db_resilience import QueryTimeoutError, QueryType
from app.services.path_search import (
    MAX_EXPANDED_PATHS,
    MAX_FRONTIER_SIZE,
    MAX_RESULT_ROWS,
    PathSearchResourceError,
    _ID_RE,
    _compare,
)


_PATH_TIMEOUT_SECONDS = 10.0
_EVIDENCE_FIELDS = {
    "score",
    "p_value",
    "confidence",
    "evidence_score",
    "publication_count",
    "support",
}
_ATOM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$")


class MorkPathMixin:
    """Shared MORK path traversal for the server and CLI-backed generators."""

    def _run_pattern(self, pattern_str, template_str):
        raise NotImplementedError

    def _path_schema(self, request):
        node_types = set(request.get("_path_node_types", ()))
        edge_types = set(request.get("_path_edge_types", ()))
        node_types.update(getattr(self, "path_node_types", ()))
        edge_types.update(getattr(self, "path_edge_types", ()))
        if not node_types or not edge_types:
            from app.services.cypher_generator import _path_schema_whitelist

            schema_nodes, schema_edges = _path_schema_whitelist()
            node_types.update(schema_nodes)
            edge_types.update(schema_edges)
        if not node_types or not edge_types:
            raise ValueError("path schema whitelist is empty")
        return node_types, edge_types

    @staticmethod
    def _safe(value, kind, label, pattern=False):
        if kind not in {"id", "atom"}:
            raise ValueError(f"Invalid safety kind: {kind}")
        regex = _ID_RE if kind == "id" else _ATOM_RE
        if not isinstance(value, str) or not regex.fullmatch(value):
            raise ValueError(f"Invalid {label}")
        if pattern and any(char in value for char in ('"', "'", "`", "$", "(", ")", " ", "\t", "\n")):
            raise ValueError(f"Invalid {label}")
        return value

    @staticmethod
    def _atom_tuples(atoms):
        if not atoms:
            return []
        parsed = []
        for atom in atoms:
            if isinstance(atom, tuple):
                parsed.append(atom)
            else:
                parsed.extend(MorkPathMixin._parse_metta_text(str(atom)))
        return parsed

    @staticmethod
    def _parse_metta_text(text):
        index = 0

        def skip_space():
            nonlocal index
            while index < len(text) and text[index].isspace():
                index += 1

        def parse_expression():
            nonlocal index
            skip_space()
            if index >= len(text):
                return None
            if text[index] != "(":
                start = index
                while index < len(text) and not text[index].isspace() and text[index] not in "()":
                    index += 1
                return text[start:index]

            index += 1
            items = []
            while index < len(text):
                skip_space()
                if index < len(text) and text[index] == ")":
                    index += 1
                    return tuple(items)
                item = parse_expression()
                if item is None:
                    break
                items.append(item)
            return tuple(items)

        parsed = []
        while True:
            skip_space()
            if index >= len(text):
                break
            item = parse_expression()
            if item is not None:
                parsed.append(item)
        return parsed

    def _query(self, deadline, pattern, template):
        if time.monotonic() >= deadline:
            raise QueryTimeoutError(QueryType.PATHS, _PATH_TIMEOUT_SECONDS)
        return self._atom_tuples(self._run_pattern(pattern, template))

    @staticmethod
    def _value(value):
        if isinstance(value, Number) and not isinstance(value, bool):
            return value
        if not isinstance(value, str):
            return value
        value = value.strip('"')
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    @staticmethod
    def _node_key(node_type, node_id):
        return node_type, node_id

    def _resolve(self, request, deadline):
        resolved = set()
        unresolved = []
        for endpoint in request["sources"] + request["targets"]:
            node_type = self._safe(endpoint["type"], "atom", "node type", pattern=True)
            node_id = self._safe(endpoint["id"], "id", "node id", pattern=True)
            found = self._query(
                deadline,
                f"({node_type} {node_id})",
                f"(resolved ({node_type} {node_id}))",
            )
            key = self._node_key(node_type, node_id)
            if any(atom and atom[0] == "resolved" for atom in found):
                resolved.add(key)
            else:
                unresolved.append(node_id)
        return resolved, sorted(set(unresolved))

    def _neighbor_edges(self, node, reverse, deadline, cache):
        key = (node, reverse)
        if key in cache:
            return cache[key]

        node_type, node_id = node
        self._safe(node_type, "atom", "node type", pattern=True)
        self._safe(node_id, "id", "node id", pattern=True)
        if reverse:
            pattern = (
                f"($predicate ($neighbor_type $neighbor_id) "
                f"({node_type} {node_id}))"
            )
            template = (
                f"(path_edge $predicate ($neighbor_type $neighbor_id) "
                f"({node_type} {node_id}))"
            )
        else:
            pattern = (
                f"($predicate ({node_type} {node_id}) "
                f"($neighbor_type $neighbor_id))"
            )
            template = (
                f"(path_edge $predicate ({node_type} {node_id}) "
                f"($neighbor_type $neighbor_id))"
            )
        atoms = self._query(deadline, pattern, template)
        atoms = list(dict.fromkeys(
            atom for atom in atoms
            if len(atom) == 4 and atom[0] == "path_edge"
        ))
        cache[key] = atoms
        return atoms

    def _neighbors(self, node, predicate, reverse, deadline, cache=None):
        if cache is None:
            cache = {}
        atoms = self._neighbor_edges(node, reverse, deadline, cache)
        if predicate is None:
            return atoms
        if isinstance(predicate, str):
            allowed = {predicate}
        else:
            allowed = set(predicate)
        return [atom for atom in atoms if atom[1] in allowed]

    def _batch_neighbors(self, nodes, reverse, deadline, cache):
        uncached = [node for node in nodes if (node, reverse) not in cache]
        if not uncached:
            return
        by_type = {}
        for node in uncached:
            by_type.setdefault(node[0], set()).add(node)

        for node_type, type_nodes in by_type.items():
            self._safe(node_type, "atom", "node type", pattern=True)
            if reverse:
                pattern = (
                    f"($predicate ($neighbor_type $neighbor_id) "
                    f"({node_type} $id))"
                )
                template = (
                    f"(path_edge $predicate ($neighbor_type $neighbor_id) "
                    f"({node_type} $id))"
                )
            else:
                pattern = (
                    f"($predicate ({node_type} $id) "
                    f"($neighbor_type $neighbor_id))"
                )
                template = (
                    f"(path_edge $predicate ({node_type} $id) "
                    f"($neighbor_type $neighbor_id))"
                )
            atoms = self._query(deadline, pattern, template)
            atoms = list(dict.fromkeys(
                atom for atom in atoms
                if len(atom) == 4 and atom[0] == "path_edge"
            ))
            for node in type_nodes:
                matching = []
                for atom in atoms:
                    source = tuple(str(value) for value in atom[2])
                    target = tuple(str(value) for value in atom[3])
                    if (reverse and target == node) or (not reverse and source == node):
                        matching.append(atom)
                cache[(node, reverse)] = matching

    def _degree(self, node, deadline, cache):
        degree_key = ("degree", node)
        if degree_key in cache:
            return cache[degree_key]
        edges = set()
        for reverse in (False, True):
            for atom in self._neighbor_edges(node, reverse, deadline, cache):
                edges.add((atom[1], tuple(atom[2]), tuple(atom[3])))
        cache[degree_key] = len(edges)
        return cache[degree_key]

    def _node_properties(self, node, deadline, cache, degree_cache):
        if node not in cache:
            node_type, node_id = node
            pattern = f"($property ({node_type} {node_id}) $value)"
            template = f"(node_property $property ({node_type} {node_id}) $value)"
            properties = {}
            edge_names = {
                str(value).replace(" ", "_")
                for value in getattr(self, "path_edge_types", ())
            }
            for atom in self._query(deadline, pattern, template):
                if len(atom) == 4 and atom[0] == "node_property":
                    name = str(atom[1])
                    value = atom[3]
                    if (
                        name not in {"id", "degree"}
                        and name not in edge_names
                        and not isinstance(value, tuple)
                        and not isinstance(value, list)
                    ):
                        properties[name] = self._value(value)
                elif len(atom) == 3:
                    name = str(atom[0])
                    node_pair = atom[1]
                    value = atom[2]
                    if (
                        name not in {"id", "degree"}
                        and name not in edge_names
                        and isinstance(node_pair, tuple)
                        and len(node_pair) == 2
                        and str(node_pair[0]) == node_type
                        and str(node_pair[1]) == node_id
                        and not isinstance(value, tuple)
                        and not isinstance(value, list)
                    ):
                        properties[name] = self._value(value)
            cache[node] = properties
        result = dict(cache[node])
        result["degree"] = int(self._degree(node, deadline, degree_cache))
        return result

    def _edge_evidence(self, edge, deadline, cache):
        key = (edge["predicate"], edge["source"], edge["target"])
        if key in cache:
            return cache[key]
        predicate = self._safe(edge["predicate"], "atom", "predicate", pattern=True)
        source_type, source_id = edge["source"]
        target_type, target_id = edge["target"]
        pattern = (
            f"($property ({predicate} ({source_type} {source_id}) "
            f"({target_type} {target_id})) $value)"
        )
        template = (
            f"(edge_evidence $property ({predicate} ({source_type} {source_id}) "
            f"({target_type} {target_id})) $value)"
        )
        evidence = {}
        for atom in self._query(deadline, pattern, template):
            if len(atom) == 4 and atom[0] == "edge_evidence":
                evidence[str(atom[1])] = self._value(atom[3])
            elif len(atom) == 3 and atom[0] == "ev":
                evidence[str(atom[1])] = self._value(atom[2])
        cache[key] = evidence
        return evidence

    @staticmethod
    def _output_predicate(predicate, request):
        requested = (
            request["edge_types"]["allow"] + request["edge_types"]["exclude"]
        )
        return predicate.replace("_", " ") if not requested or any(" " in value for value in requested) else predicate

    @staticmethod
    def _score(edges):
        scores = [
            edge["evidence"].get("score")
            for edge in edges
            if isinstance(edge["evidence"].get("score"), Number)
        ]
        return sum(float(score) for score in scores) / len(scores) if scores else 0.0

    def find_paths(self, request):
        deadline = time.monotonic() + _PATH_TIMEOUT_SECONDS
        node_types, edge_types = self._path_schema(request)
        endpoint_types = {endpoint["type"] for endpoint in request["sources"] + request["targets"]}
        intermediate_types = set(request["intermediate_node_types"]["allow"])
        intermediate_types.update(request["intermediate_node_types"]["exclude"])
        for node_type in endpoint_types | intermediate_types:
            self._safe(node_type, "atom", "node type", pattern=True)
            if node_types and node_type not in node_types:
                raise ValueError(f"Unknown node type: {node_type}")

        predicates = {value.replace(" ", "_") for value in edge_types}
        allowed = request["edge_types"]["allow"]
        excluded = set(request["edge_types"]["exclude"])
        for requested_predicate in allowed + request["edge_types"]["exclude"]:
            canonical = requested_predicate.replace(" ", "_")
            self._safe(canonical, "atom", "predicate", pattern=True)
            if edge_types and canonical not in predicates:
                raise ValueError(f"Unknown predicate: {requested_predicate}")
        if allowed:
            predicates &= {value.replace(" ", "_") for value in allowed}
        predicates -= {value.replace(" ", "_") for value in excluded}
        for predicate in predicates:
            self._safe(predicate, "atom", "predicate", pattern=True)
        if not predicates:
            return {"candidates": [], "unresolved_ids": [], "truncated": False}

        resolved, unresolved = self._resolve(request, deadline)
        source_nodes = [
            (endpoint["type"], endpoint["id"])
            for endpoint in request["sources"]
            if (endpoint["type"], endpoint["id"]) in resolved
        ]
        target_nodes = {
            (endpoint["type"], endpoint["id"])
            for endpoint in request["targets"]
            if (endpoint["type"], endpoint["id"]) in resolved
        }
        if not source_nodes or not target_nodes:
            return {"candidates": [], "unresolved_ids": unresolved, "truncated": False}

        degree_cache = {}
        property_cache = {}
        evidence_cache = {}
        neighbor_cache = degree_cache
        paths = []
        frontier = [
            {"nodes": [source], "edges": []}
            for source in source_nodes
        ]
        max_hops = request["max_hops"]
        hub = request["hub_avoidance"]
        excluded_ids = set(hub["exclude_ids"])
        intermediate_filter = request["intermediate_node_types"]

        for _ in range(max_hops):
            next_frontier = []
            if len(frontier) > MAX_FRONTIER_SIZE:
                raise PathSearchResourceError(
                    f"Path search exceeded MAX_FRONTIER_SIZE ({MAX_FRONTIER_SIZE})"
                )
            frontier_nodes = list(dict.fromkeys(
                state["nodes"][-1] for state in frontier
            ))
            for reverse in (False, True):
                self._batch_neighbors(
                    frontier_nodes, reverse, deadline, neighbor_cache
                )
            for state in frontier:
                current = state["nodes"][-1]
                for reverse in (False, True):
                    atoms = neighbor_cache.get((current, reverse), [])
                    for atom in atoms:
                        if len(atom) != 4 or atom[0] != "path_edge":
                            continue
                        _, atom_predicate, source_node, target_node = atom
                        if atom_predicate not in predicates:
                            continue
                        source = tuple(str(value) for value in source_node)
                        target = tuple(str(value) for value in target_node)
                        neighbor = source if reverse else target
                        if neighbor in state["nodes"]:
                            continue
                        if neighbor not in target_nodes:
                            if neighbor[1] in excluded_ids:
                                continue
                            if intermediate_filter["allow"] and neighbor[0] not in intermediate_filter["allow"]:
                                continue
                            if neighbor[0] in intermediate_filter["exclude"]:
                                continue
                            if hub["enabled"] and self._degree(neighbor, deadline, neighbor_cache) > hub["max_degree"]:
                                continue
                        edge = {
                            "predicate": atom_predicate,
                            "source": source,
                            "target": target,
                        }
                        next_state = {
                            "nodes": state["nodes"] + [neighbor],
                            "edges": state["edges"] + [edge],
                        }
                        if neighbor in target_nodes:
                            paths.append(next_state)
                        elif len(next_state["edges"]) < max_hops:
                            next_frontier.append(next_state)
                        if len(paths) > MAX_EXPANDED_PATHS:
                            raise PathSearchResourceError(
                                f"Path search exceeded MAX_EXPANDED_PATHS ({MAX_EXPANDED_PATHS})"
                            )
                        if len(next_frontier) > MAX_FRONTIER_SIZE:
                            raise PathSearchResourceError(
                                f"Path search exceeded MAX_FRONTIER_SIZE ({MAX_FRONTIER_SIZE})"
                            )
            frontier = next_frontier
            if not frontier:
                break

        candidates = []
        for path in paths:
            edges = []
            valid = True
            for raw_edge in path["edges"]:
                evidence = self._edge_evidence(raw_edge, deadline, evidence_cache)
                if any(
                    not _compare(evidence.get(item["field"]), item["operator"], item["value"])
                    for item in request["evidence_filters"]
                ):
                    valid = False
                    break
                edges.append({
                    "source": raw_edge["source"][1],
                    "target": raw_edge["target"][1],
                    "source_type": raw_edge["source"][0],
                    "target_type": raw_edge["target"][0],
                    "type": self._output_predicate(raw_edge["predicate"], request),
                    "evidence": deepcopy(evidence),
                })
            if not valid:
                continue
            nodes = [
                {
                    "id": node[1],
                    "type": node[0],
                    "properties": self._node_properties(
                        node, deadline, property_cache, degree_cache
                    ),
                }
                for node in path["nodes"]
            ]
            candidates.append({"nodes": nodes, "edges": edges})

        candidates.sort(
            key=lambda candidate: (
                len(candidate["edges"]),
                -self._score(candidate["edges"]),
                tuple(node["id"] for node in candidate["nodes"]),
                tuple(edge["type"] for edge in candidate["edges"]),
            )
        )
        cap = min(max(request["k"] * 5, request["k"] + 1), 1000)
        truncated = len(candidates) > cap
        return {
            "candidates": candidates[:cap],
            "unresolved_ids": unresolved,
            "truncated": truncated,
        }
