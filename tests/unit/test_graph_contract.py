import copy

from app.lib.graph import Graph


def _normalize_graph(graph):
    normalized = {"nodes": [], "edges": []}
    for node in graph.get("nodes", []):
        data = copy.deepcopy(node["data"])
        data.pop("id", None)
        normalized["nodes"].append(data)
    for edge in graph.get("edges", []):
        data = copy.deepcopy(edge["data"])
        data.pop("id", None)
        normalized["edges"].append(data)
    normalized["nodes"] = sorted(
        normalized["nodes"],
        key=lambda item: (
            item.get("type", ""),
            item.get("name", ""),
            item.get("parent", ""),
            len(item.get("nodes", [])),
        ),
    )
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


def test_group_node_only_contract_shape():
    graph = {
        "nodes": [
            {"data": {"id": "p1", "type": "protein", "name": "protein a"}},
            {"data": {"id": "p2", "type": "protein", "name": "protein b"}},
            {"data": {"id": "g1", "type": "gene", "name": "gene a"}},
        ],
        "edges": [],
    }
    request = {"nodes": [{"type": "protein"}, {"type": "gene"}]}

    result = Graph().group_node_only(graph, request)

    assert set(result.keys()) == {"nodes", "edges"}
    assert result["edges"] == []
    assert len(result["nodes"]) == 2
    for node in result["nodes"]:
        assert "data" in node
        assert "id" in node["data"]
        assert "type" in node["data"]
        assert "nodes" in node["data"]


def test_break_grouping_contract_shape():
    grouped = {
        "nodes": [
            {
                "data": {
                    "id": "group_src",
                    "type": "protein",
                    "name": "2 protein nodes",
                    "nodes": [
                        {"id": "protein a", "type": "protein", "name": "protein a"},
                        {"id": "protein b", "type": "protein", "name": "protein b"},
                    ],
                }
            },
            {
                "data": {
                    "id": "group_tgt",
                    "type": "gene",
                    "name": "2 gene nodes",
                    "nodes": [
                        {"id": "gene a", "type": "gene", "name": "gene a"},
                        {"id": "gene b", "type": "gene", "name": "gene b"},
                    ],
                }
            },
        ],
        "edges": [
            {
                "data": {
                    "id": "e1",
                    "source": "group_src",
                    "target": "group_tgt",
                    "edge_id": "protein_related_gene",
                    "label": "related",
                }
            }
        ],
    }

    result = Graph().break_grouping(grouped)

    assert set(result.keys()) == {"nodes", "edges"}
    assert len(result["nodes"]) == 4
    assert len(result["edges"]) == 4
    for edge in result["edges"]:
        assert set(edge["data"].keys()) == {"id", "source", "target", "label", "edge_id"}


def test_group_graph_contract_shape():
    source_graph = {
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
    grouped = Graph().group_graph(source_graph)
    normalized = _normalize_graph(grouped)

    assert set(grouped.keys()) == {"nodes", "edges"}
    assert len(grouped["nodes"]) >= 2
    assert len(grouped["edges"]) >= 1
    node_types = {node.get("type") for node in normalized["nodes"]}
    assert "protein" in node_types or "parent" in node_types
