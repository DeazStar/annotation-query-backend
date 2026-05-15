import copy

import pytest

from app.lib.graph_legacy import PythonGraphOps


graph_native = pytest.importorskip("graph_native")


def _normalize(graph):
    normalized = {"nodes": [], "edges": []}
    sorted_nodes = sorted(
        graph.get("nodes", []),
        key=lambda n: (
            n["data"].get("type", ""),
            n["data"].get("name", ""),
        ),
    )
    id_map = {}
    for i, node in enumerate(sorted_nodes):
        orig_id = node["data"]["id"]
        id_map[orig_id] = f"id_{i}"

    for node in sorted_nodes:
        data = copy.deepcopy(node["data"])
        orig_id = data.pop("id", None)
        if "parent" in data and data["parent"] in id_map:
            data["parent"] = id_map[data["parent"]]
        normalized["nodes"].append(data)

    for edge in graph.get("edges", []):
        data = copy.deepcopy(edge["data"])
        data.pop("id", None)
        if data.get("source") in id_map:
            data["source"] = id_map[data["source"]]
        if data.get("target") in id_map:
            data["target"] = id_map[data["target"]]
        normalized["edges"].append(data)

    normalized["edges"] = sorted(
        normalized["edges"],
        key=lambda item: (
            item.get("source", ""),
            item.get("target", ""),
            item.get("edge_id", ""),
            item.get("label", ""),
        ),
    )
    return normalized


def _sample_graph():
    return {
        "nodes": [
            {"data": {"id": "protein p1", "type": "protein", "name": "protein p1"}},
            {"data": {"id": "protein p2", "type": "protein", "name": "protein p2"}},
            {"data": {"id": "gene g1", "type": "gene", "name": "gene g1"}},
        ],
        "edges": [
            {
                "data": {
                    "id": "e1",
                    "source": "protein p1",
                    "target": "gene g1",
                    "edge_id": "protein_related_gene",
                    "label": "related",
                }
            },
            {
                "data": {
                    "id": "e2",
                    "source": "protein p2",
                    "target": "gene g1",
                    "edge_id": "protein_related_gene",
                    "label": "related",
                }
            },
        ],
    }


def test_group_graph_parity():
    graph = _sample_graph()
    py_ops = PythonGraphOps()
    python_result = py_ops.group_graph(copy.deepcopy(graph))
    native_result = graph_native.group_graph(copy.deepcopy(graph))
    assert _normalize(native_result) == _normalize(python_result)


def test_group_node_only_parity():
    graph = {"nodes": _sample_graph()["nodes"], "edges": []}
    request = {"nodes": [{"type": "protein"}, {"type": "gene"}]}
    py_ops = PythonGraphOps()
    python_result = py_ops.group_node_only(copy.deepcopy(graph), copy.deepcopy(request))
    native_result = graph_native.group_node_only(copy.deepcopy(graph), copy.deepcopy(request))
    assert _normalize(native_result) == _normalize(python_result)
