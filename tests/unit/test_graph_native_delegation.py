from app.lib import graph as graph_module


class NativeStub:
    @staticmethod
    def group_graph(graph):
        return {"nodes": [{"data": {"id": "n1", "type": "native", "name": "native"}}], "edges": []}

    @staticmethod
    def group_node_only(graph, request):
        return {"nodes": [{"data": {"id": "n2", "type": "native", "name": "native"}}], "edges": []}

    @staticmethod
    def break_grouping(graph):
        return {"nodes": [], "edges": [{"data": {"id": "e1", "source": "a", "target": "b", "label": "l", "edge_id": "e"}}]}


def _sample_graph():
    return {
        "nodes": [{"data": {"id": "protein p1", "type": "protein", "name": "protein p1", "nodes": [{"id": "protein p1"}]}}],
        "edges": [],
    }


def test_graph_uses_python_when_native_disabled(monkeypatch):
    monkeypatch.setattr(graph_module, "_graph_native", NativeStub)
    monkeypatch.setenv("GRAPH_NATIVE_ENABLED", "false")
    monkeypatch.setenv("GRAPH_NATIVE_SHADOW", "false")

    graph = graph_module.Graph()
    result = graph.group_node_only(_sample_graph(), {"nodes": [{"type": "protein"}]})
    assert result["nodes"][0]["data"]["type"] == "protein"


def test_graph_uses_native_when_enabled(monkeypatch):
    monkeypatch.setattr(graph_module, "_graph_native", NativeStub)
    monkeypatch.setenv("GRAPH_NATIVE_ENABLED", "true")
    monkeypatch.setenv("GRAPH_NATIVE_SHADOW", "false")

    graph = graph_module.Graph()
    result = graph.group_graph(_sample_graph())
    assert result["nodes"][0]["data"]["type"] == "native"


