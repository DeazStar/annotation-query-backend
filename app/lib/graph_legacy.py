from copy import deepcopy
from nanoid import generate

from app.lib.utils import extract_middle


class PythonGraphOps:
    """Pure Python graph operations kept as fallback/parity baseline."""

    def group_graph(self, graph):
        collapsed = self._collapse_nodes(graph)
        return self._group_into_parents(collapsed)

    def group_node_only(self, graph, request):
        nodes = graph.get("nodes", [])
        new_graph = {"nodes": [], "edges": []}
        node_map_by_label = {node["type"]: [] for node in request.get("nodes", [])}

        for node in nodes:
            node_type = node["data"].get("type")
            if node_type in node_map_by_label:
                node_map_by_label[node_type].append(deepcopy(node["data"]))

        for node_type, grouped_nodes in node_map_by_label.items():
            name = f"{len(grouped_nodes)} {node_type} nodes"
            new_graph["nodes"].append(
                {
                    "data": {
                        "id": generate(),
                        "type": node_type,
                        "name": name,
                        "nodes": grouped_nodes,
                    }
                }
            )
        return new_graph

    def break_grouping(self, graph):
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        parent_edges = {}

        for node in nodes:
            data = node.get("data", {})
            if data.get("type") == "parent":
                parent_edges[data.get("id")] = []

        for node in nodes:
            data = node.get("data", {})
            if "parent" in data and data.get("type") == "protein":
                parent_id = data.get("parent")
                if parent_id in parent_edges:
                    parent_edges[parent_id].append(data.get("id"))

        expanded_edges = []
        for edge in edges:
            edge_data = edge["data"]
            src = edge_data.get("source")
            tgt = edge_data.get("target")
            if src in parent_edges:
                for child in parent_edges[src]:
                    expanded_edges.append(
                        {
                            "data": {
                                "source": child,
                                "target": tgt,
                                "label": edge_data.get("label"),
                                "edge_id": edge_data.get("edge_id"),
                                "id": generate(),
                            }
                        }
                    )
            elif tgt in parent_edges:
                for child in parent_edges[tgt]:
                    expanded_edges.append(
                        {
                            "data": {
                                "source": src,
                                "target": child,
                                "label": edge_data.get("label"),
                                "edge_id": edge_data.get("edge_id"),
                                "id": generate(),
                            }
                        }
                    )
            else:
                expanded_edges.append(
                    {
                        "data": {
                            "source": src,
                            "target": tgt,
                            "label": edge_data.get("label"),
                            "edge_id": edge_data.get("edge_id"),
                            "id": generate(),
                        }
                    }
                )

        initial_node_map = {node["data"]["id"]: node for node in nodes}
        edge_map = {}
        for edge in expanded_edges:
            source = edge["data"]["source"]
            target = edge["data"]["target"]
            label = edge["data"]["label"]
            edge_id = edge["data"]["edge_id"]

            if source not in initial_node_map or target not in initial_node_map:
                continue

            source_nodes = []
            target_nodes = []
            source_data = initial_node_map[source]["data"]
            target_data = initial_node_map[target]["data"]

            if source_data.get("type") != "parent":
                source_nodes = [n["id"] for n in source_data.get("nodes", [])]
            if target_data.get("type") != "parent":
                target_nodes = [n["id"] for n in target_data.get("nodes", [])]

            for source_node in source_nodes:
                for target_node in target_nodes:
                    key = (source_node, label, target_node, edge_id)
                    edge_map[key] = {
                        "source": source_node,
                        "target": target_node,
                        "label": label,
                        "edge_id": edge_id,
                    }

        response = {"nodes": [], "edges": []}
        for value in edge_map.values():
            response["edges"].append(
                {
                    "data": {
                        "id": generate(),
                        "source": value["source"],
                        "target": value["target"],
                        "label": value["label"],
                        "edge_id": value["edge_id"],
                    }
                }
            )

        for node in nodes:
            node_data = node["data"]
            if node_data.get("type") == "parent":
                continue
            for single_node in node_data.get("nodes", []):
                response["nodes"].append({"data": deepcopy(single_node)})

        return response

    def _collapse_nodes(self, graph):
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        node_map = {node["data"]["id"]: deepcopy(node["data"]) for node in nodes}

        inbound = {node_id: [] for node_id in node_map}
        outbound = {node_id: [] for node_id in node_map}
        for edge in edges:
            edge_data = edge["data"]
            source = edge_data.get("source")
            target = edge_data.get("target")
            edge_id = edge_data.get("edge_id")
            if source in outbound:
                outbound[source].append((target, edge_id))
            if target in inbound:
                inbound[target].append((source, edge_id))

        signature_groups = {}
        for node_id in node_map:
            signature = (tuple(sorted(inbound[node_id])), tuple(sorted(outbound[node_id])))
            signature_groups.setdefault(signature, []).append(node_id)

        group_for_node = {}
        grouped_nodes = []
        for grouped_ids in signature_groups.values():
            first_data = node_map[grouped_ids[0]]
            base_label = first_data.get("id", "").split(" ")[0]
            grouped_id = generate()
            name = (
                first_data.get("name")
                if len(grouped_ids) == 1
                else f"{len(grouped_ids)} {base_label} nodes"
            )
            members = [deepcopy(node_map[node_id]) for node_id in grouped_ids]
            grouped_nodes.append(
                {
                    "data": {
                        "id": grouped_id,
                        "type": base_label,
                        "name": name,
                        "nodes": members,
                    }
                }
            )
            for node_id in grouped_ids:
                group_for_node[node_id] = grouped_id

        seen = set()
        grouped_edges = []
        for edge in edges:
            edge_data = edge["data"]
            grouped_source = group_for_node.get(edge_data.get("source"))
            grouped_target = group_for_node.get(edge_data.get("target"))
            if not grouped_source or not grouped_target or grouped_source == grouped_target:
                continue
            key = (grouped_source, grouped_target, edge_data.get("edge_id"))
            if key in seen:
                continue
            seen.add(key)
            grouped_edges.append(
                {
                    "data": {
                        "id": generate(),
                        "source": grouped_source,
                        "target": grouped_target,
                        "label": edge_data.get("label"),
                        "edge_id": edge_data.get("edge_id"),
                    }
                }
            )

        return {"nodes": grouped_nodes, "edges": grouped_edges}

    def _group_into_parents(self, graph):
        node_ids = {node["data"]["id"] for node in graph.get("nodes", [])}
        out_map = {node_id: {} for node_id in node_ids}
        in_map = {node_id: {} for node_id in node_ids}

        for edge in graph.get("edges", []):
            data = edge["data"]
            source = data.get("source")
            target = data.get("target")
            edge_id = data.get("edge_id")
            if source in out_map:
                out_map[source].setdefault(edge_id, set()).add(target)
            if target in in_map:
                in_map[target].setdefault(edge_id, set()).add(source)

        parent_map = {}
        for node_id in node_ids:
            for edge_id, neighbors in out_map[node_id].items():
                if len(neighbors) < 2:
                    continue
                key_nodes = sorted(neighbors)
                key = ",".join(key_nodes)
                if key not in parent_map:
                    parent_map[key] = {
                        "id": generate(),
                        "node": node_id,
                        "edge_id": edge_id,
                        "label": extract_middle(edge_id),
                        "count": len(neighbors),
                        "is_source": True,
                    }
            for edge_id, neighbors in in_map[node_id].items():
                if len(neighbors) < 2:
                    continue
                key_nodes = sorted(neighbors)
                key = ",".join(key_nodes)
                if key not in parent_map:
                    parent_map[key] = {
                        "id": generate(),
                        "node": node_id,
                        "edge_id": edge_id,
                        "label": extract_middle(edge_id),
                        "count": len(neighbors),
                        "is_source": False,
                    }

        keys = list(parent_map.keys())
        invalid_groups = []
        for current_key in keys:
            parent_current = parent_map[current_key]
            current_set = set(current_key.split(","))
            for other_key in keys:
                if other_key == current_key:
                    continue
                parent_other = parent_map[other_key]
                if (
                    parent_other["is_source"] == parent_current["is_source"]
                    and parent_other["count"] > parent_current["count"]
                    and current_set.intersection(set(other_key.split(",")))
                ):
                    invalid_groups.append(current_key)
                    break
        for key in invalid_groups:
            parent_map.pop(key, None)

        parents = set()
        grouped_nodes = {}
        for node in graph.get("nodes", []):
            node_count = 0
            node_id = node["data"]["id"]
            for key, parent in parent_map.items():
                if node_id in key.split(",") and parent["count"] > node_count:
                    node["data"]["parent"] = parent["id"]
                    node_count = parent["count"]
            parent_id = node["data"].get("parent")
            if parent_id:
                parents.add(parent_id)
                grouped_nodes.setdefault(parent_id, []).append(node)

        for parent_id, grouped in list(grouped_nodes.items()):
            if len(grouped) < 2:
                parents.discard(parent_id)
                for node in grouped:
                    node["data"]["parent"] = ""
                grouped_nodes.pop(parent_id, None)

        for parent_id in sorted(parents):
            graph["nodes"].append(
                {"data": {"id": parent_id, "type": "parent", "name": parent_id}}
            )

        new_edges = []
        for edge in graph.get("edges", []):
            keep_edge = True
            edge_data = edge["data"]
            for key, parent in parent_map.items():
                if parent["id"] not in parents:
                    continue
                if parent["is_source"]:
                    edge_key = edge_data["target"]
                    parent_node = edge_data["source"]
                else:
                    edge_key = edge_data["source"]
                    parent_node = edge_data["target"]
                if (
                    edge_key in key.split(",")
                    and parent["node"] == parent_node
                    and parent["edge_id"] == edge_data["edge_id"]
                ):
                    keep_edge = False
                    break
            if keep_edge:
                new_edges.append(edge)

        for parent in parent_map.values():
            if parent["id"] not in parents:
                continue
            if parent["is_source"]:
                source = parent["node"]
                target = parent["id"]
            else:
                source = parent["id"]
                target = parent["node"]
            new_edges.append(
                {
                    "data": {
                        "id": generate(),
                        "source": source,
                        "target": target,
                        "label": parent["label"],
                        "edge_id": parent["edge_id"],
                    }
                }
            )

        graph["edges"] = new_edges
        return graph
