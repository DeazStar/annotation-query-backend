from collections import defaultdict
import uuid
import json
def group_graph(result_graph, request):
    
    MINIMUM_EDGES_TO_COLLAPSE = 2
     
    result_graph=json.loads(result_graph)
     
    Edge=result_graph['edges']
     
    # Get all unique edge types
    edge_types = list(set(edge["type"] for edge in request['requests']["predicates"]))

 
    # Group edges by source or target for each edge type
    edge_groupings = []
    for edge_type in edge_types:
        # Filter edges of this type
        
        edges_of_type = [edge for edge  in Edge if edge["data"]["label"] == edge_type]
        
        # Group by source and target
        source_groups = defaultdict(list)
        target_groups = defaultdict(list)
        for edge in edges_of_type:
            source_groups[edge["data"]["source"]].append(edge)
            target_groups[edge["data"]["target"]].append(edge)

        # Choose grouping with fewer groups
        grouped_by = "source" if len(source_groups) <= len(target_groups) else "target"
        groups = source_groups if grouped_by == "source" else target_groups
         
        # Save grouping info
        edge_groupings.append({
            "count": len(edges_of_type),
            "edgeType": edge_type,
            "groupedBy": grouped_by,
            "groups": groups,
        })

    # Sort edge groupings to process the most impactful ones first
    edge_groupings.sort(key=lambda g: g["count"] - len(g["groups"]), reverse=True)
     
    # Process each edge grouping to modify the graph
    new_graph = result_graph.copy()
    for grouping in edge_groupings:
        sorted_groups = sorted(grouping["groups"].items(), key=lambda x: len(x[1]), reverse=True)
        for key, edges in sorted_groups:
            if len(edges) < MINIMUM_EDGES_TO_COLLAPSE:
                continue
             
            # Get IDs of nodes to be grouped
            child_node_ids = [
                edge["data"]["source"] if grouping["groupedBy"] == "target" else edge["data"]["target"]
                for edge in edges
            ]

            # Filter child nodes
            child_nodes = [node for node in new_graph["nodes"] if node["data"]["id"] in child_node_ids]
            unique_parents = list({node["data"].get("parent") for node in child_nodes if "parent" in node["data"]})

            if len(unique_parents) > 1:
                continue  # Skip if nodes have different parents

            # Check if the parent has the same child nodes
            if unique_parents:
                parent_id = unique_parents[0]
                all_child_nodes = [
                    node for node in new_graph["nodes"]
                    if node["data"].get("parent") == parent_id
                ]
                if len(all_child_nodes) == len(child_nodes):
                    add_new_edge(new_graph, edges[0], parent_id, grouping["groupedBy"])
                    continue
             
            # Create a new parent node
            parent_id = f"n{uuid.uuid4().hex[:8]}"
            parent_node = {"data": {"id": parent_id, "type": "parent"}}
         
            new_graph["nodes"].append(parent_node)

            # Assign parent to child nodes
            for node in new_graph["nodes"]:
                if node["data"]["id"] in child_node_ids:
                    node["data"]["parent"] = parent_id

            # Add a new edge pointing to the parent node
            add_new_edge(new_graph, edges[0], parent_id, grouping["groupedBy"])
             
    # Step 6: Count types for each parent
    parent_counts = defaultdict(lambda: defaultdict(int))

    # Iterate through all nodes to check for parent nodes and count based on types
    for node in new_graph["nodes"]:
        # Check if the node is a parent node
        if node["data"].get("type") == "parent":
            parent_id = node["data"].get("id")  # Get parent ID
            if parent_id:
                # Now iterate through all nodes to check the parent and count based on type
                for child_node in new_graph["nodes"]:
                    if child_node["data"].get("parent") == parent_id:
                        node_type = child_node["data"].get("type")
                        # Increment count based on the node type
                        if node_type:  # Check if the type exists
                            parent_counts[parent_id][node_type] += 1

    # Step 7: Update the parent nodes with the counts for each type (promoter, gene, etc.)
    for parent_id, counts in parent_counts.items():
        for node in new_graph["nodes"]:
            if node["data"].get("id") == parent_id:
                node["data"].update(counts)  # Update the parent node with the counts
                break
    update_graph=transform_and_update_graph(new_graph)
    return update_graph


def add_new_edge(graph, edge, parent_id, grouped_by):
    new_edge_id = f"e{uuid.uuid4().hex[:8]}"
    new_edge = {
        "data": {
            **edge["data"],
            "id": new_edge_id,
            grouped_by: parent_id,
        }
    }
    graph["edges"] = [
        new_edge
    ] + [e for e in graph["edges"] if e != edge]
def transform_and_update_graph(graph):
    transformed_nodes = []

    # Step 1: Transform parent nodes and collect the new nodes
    for node in graph["nodes"]:
        data = node.get("data", {})
        
        # Check if the node's type is 'parent'
        if data.get("type") == "parent":
            parent_id = data.get("id")

            # Find the first key that is not 'id' or 'type' (to avoid processing those)
            for key, value in data.items():
                if key not in ["id", "type"]:
                    # Construct the name dynamically as "value key"
                    name_value = f"{value} {key}"
                    transformed_nodes.append({"data": {"parent": parent_id, "name": name_value}})
                    break  # Process only the first non-'id' and non-'type' field

    # Step 2: Remove the original parent nodes and replace them with the transformed nodes
    graph["nodes"] = [node for node in graph["nodes"] if node.get("data", {}).get("type") != "parent"]

    # Step 3: Add the transformed nodes to the graph
    graph["nodes"].extend(transformed_nodes)

    return graph