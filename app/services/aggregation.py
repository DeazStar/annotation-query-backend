"""Aggregated multi-source query handler.

Runs a single validated request against the Human, Fly and Custom Neo4j
sources independently, merges the results by node id (gene id), and returns
one generalized response together with the query-agnostic schema.
"""

import logging

from app import db_instance, schema_manager
from app.lib import validate_request, heuristic_sort, merge_graphs, normalize_custom_graph, build_custom_request

logger = logging.getLogger(__name__)

SOURCES = ["human", "fly", "custom"]


def build_general_processed_schema(custom_representation):
    """Union of the processed human/fly schemas plus the custom edges."""
    general = {}
    general.update(schema_manager.human_schema)
    general.update(schema_manager.fly_schema)

    if custom_representation:
        for edge in custom_representation.get("edges", {}).values():
            source = edge['source'].replace(' ', '_')
            target = edge['target'].replace(' ', '_')
            key = f"{source}_{edge['label']}_{target}"
            general[key] = {
                "source": edge["source"],
                "target": edge["target"],
                "label": edge["label"],
            }

    return general


def handle_aggregated_query(requests, folder_id, limit=None, source=None):
    """Validate, query and merge across human + fly + custom sources."""
    if not hasattr(db_instance, "custom_driver"):
        raise ValueError("Aggregated query requires the cypher (Neo4j) backend")
    if db_instance.custom_driver is None:
        raise ValueError("CUSTOM_NEO4J_URI is not configured")

    custom_representation = {"nodes": {}, "edges": {}}
    try:
        custom_representation = db_instance.introspect_tenant_schema(folder_id)
    except Exception as e:
        logger.error(
            f"Custom schema introspection failed (folder_id={folder_id}): {e}")
    general_processed_schema = build_general_processed_schema(custom_representation)

    node_map = validate_request(requests, general_processed_schema, source)
    if node_map is None:
        raise ValueError("Invalid node_map returned by validate_request")

    requests = db_instance.parse_id(requests)
    requests = heuristic_sort(requests, node_map)

    node_only = True if source == 'hypothesis' else False

    base_query = db_instance.query_Generator(requests, node_map, limit, node_only)
    custom_requests, custom_node_map = build_custom_request(requests)
    custom_query = db_instance.query_Generator(
        custom_requests, custom_node_map, limit, node_only, tenant_id=folder_id)

    graph_components = {
        "nodes": requests["nodes"],
        "predicates": requests["predicates"],
        "properties": True,
    }

    graphs = []
    responded_sources = []
    for species in SOURCES:
        query_code = custom_query[0] if species == "custom" else base_query[0]
        try:
            result = db_instance.run_query(query_code, species=species)
            graph = db_instance.parse_and_serialize(
                result, schema_manager.full_schema_representation,
                graph_components, result_type="graph")
            if species == "custom":
                graph = normalize_custom_graph(graph)
            graphs.append(graph)
            responded_sources.append(species)
        except Exception as e:
            logger.error(f"Source '{species}' failed during aggregated query: {e}")

    merged = merge_graphs(graphs)

    general_schema = schema_manager.build_general_schema(custom_representation)

    return {
        "nodes": merged["nodes"],
        "edges": merged["edges"],
        "schema": general_schema,
        "sources": responded_sources,
    }