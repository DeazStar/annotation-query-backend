import copy
import logging
import os

from app.lib.graph_legacy import PythonGraphOps

try:
    import graph_native as _graph_native
except Exception:
    _graph_native = None

_graph_metrics = {
    "native_success": 0,
    "native_fallback": 0,
    "native_parity_mismatch": 0,
}


def _normalize_graph(graph):
    normalized = {"nodes": [], "edges": []}
    for node in graph.get("nodes", []):
        data = copy.deepcopy(node.get("data", {}))
        data.pop("id", None)
        normalized["nodes"].append(data)
    for edge in graph.get("edges", []):
        data = copy.deepcopy(edge.get("data", {}))
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


class Graph:
    def __init__(self):
        self.python_ops = PythonGraphOps()
        self.native_enabled = os.getenv("GRAPH_NATIVE_ENABLED", "true").lower() == "true"
        self.shadow_mode = os.getenv("GRAPH_NATIVE_SHADOW", "false").lower() == "true"

    def group_graph(self, graph):
        return self._execute("group_graph", graph)

    def group_node_only(self, graph, request):
        return self._execute("group_node_only", graph, request)

    def break_grouping(self, graph):
        return self._execute("break_grouping", graph)

    def _execute(self, operation_name, *args):
        native_func = getattr(_graph_native, operation_name, None) if _graph_native else None
        python_func = getattr(self.python_ops, operation_name)

        if self.shadow_mode:
            python_result = python_func(*args)
            try:
                native_result = native_func(*args)
                if _normalize_graph(native_result) != _normalize_graph(python_result):
                    _graph_metrics["native_parity_mismatch"] += 1
                    logging.warning(
                        "Graph native parity mismatch for operation %s",
                        operation_name,
                    )
                if self.native_enabled:
                    _graph_metrics["native_success"] += 1
                    return native_result
            except Exception as error:
                _graph_metrics["native_fallback"] += 1
                logging.exception("Graph native shadow execution failed: %s", error)
            return python_result

        if self.native_enabled and native_func:
            try:
                result = native_func(*args)
                _graph_metrics["native_success"] += 1
                return result
            except Exception as error:
                _graph_metrics["native_fallback"] += 1
                logging.exception("Graph native execution failed, using python fallback: %s", error)

        return python_func(*args)
