from app.lib.merge import (
    merge_graphs, normalize_custom_node_id, normalize_custom_graph, is_empty_id,
    build_custom_request,
)


def test_merge_graphs_unifies_shared_gene_nodes():
    human = {
        "nodes": [{"data": {"id": "gene ENSG00000139618", "type": "gene", "gene_name": "BRCA1"}}],
        "edges": [],
    }
    custom = {
        "nodes": [{"data": {"id": "gene ENSG00000139618", "type": "gene", "custom_prop": "x"}}],
        "edges": [],
    }
    merged = merge_graphs([human, custom])
    assert len(merged["nodes"]) == 1
    node = merged["nodes"][0]["data"]
    assert node["gene_name"] == "BRCA1"
    assert node["custom_prop"] == "x"


def test_merge_graphs_keeps_distinct_nodes():
    g1 = {"nodes": [{"data": {"id": "gene ENSG0001", "type": "gene"}}], "edges": []}
    g2 = {"nodes": [{"data": {"id": "gene FBgn0001", "type": "gene"}}], "edges": []}
    merged = merge_graphs([g1, g2])
    assert len(merged["nodes"]) == 2


def test_merge_graphs_deduplicates_edges():
    edge = {"data": {"source": "gene ENSG1", "target": "disease D1", "label": "associates"}}
    g1 = {"nodes": [], "edges": [edge]}
    g2 = {"nodes": [], "edges": [edge]}
    merged = merge_graphs([g1, g2])
    assert len(merged["edges"]) == 1


def test_normalize_custom_node_id_restores_biological_id():
    normalized = normalize_custom_node_id(
        {"id": "gene gene_ensg00000139618", "type": "gene"})
    assert normalized["id"] == "gene ENSG00000139618"


def test_normalize_custom_node_id_restores_flybase_case():
    normalized = normalize_custom_node_id(
        {"id": "gene gene_FBgn0000001", "type": "gene"})
    assert normalized["id"] == "gene FBgn0000001"


def test_normalize_custom_node_id_restores_lowercase_fbgn():
    normalized = normalize_custom_node_id(
        {"id": "gene gene_fbgn0000001", "type": "gene"})
    assert normalized["id"] == "gene FBgn0000001"


def test_normalize_custom_node_id_leaves_unrelated_ids_unchanged():
    node = {"id": "disease doid_123", "type": "disease"}
    assert normalize_custom_node_id(node)["id"] == "disease doid_123"


def test_normalize_custom_graph_normalizes_edges():
    graph = {
        "nodes": [{"data": {"id": "gene gene_ensg00000139618", "type": "gene"}}],
        "edges": [{"data": {
            "source": "gene gene_ensg00000139618",
            "target": "gene gene_FBgn0000001",
            "label": "interacts",
        }}],
    }
    normalized = normalize_custom_graph(graph)
    edge = normalized["edges"][0]["data"]
    assert normalized["nodes"][0]["data"]["id"] == "gene ENSG00000139618"
    assert edge["source"] == "gene ENSG00000139618"
    assert edge["target"] == "gene FBgn0000001"


def test_merge_graphs_merges_custom_with_human_after_normalization():
    human = {
        "nodes": [{"data": {"id": "gene ENSG00000139618", "type": "gene", "gene_name": "BRCA1"}}],
        "edges": [],
    }
    custom = normalize_custom_graph({
        "nodes": [{"data": {"id": "gene gene_ensg00000139618", "type": "gene", "custom_prop": "x"}}],
        "edges": [],
    })
    merged = merge_graphs([human, custom])
    assert len(merged["nodes"]) == 1
    node = merged["nodes"][0]["data"]
    assert node["gene_name"] == "BRCA1"
    assert node["custom_prop"] == "x"


def test_is_empty_id_recognizes_formatter_empty_ids():
    assert is_empty_id(None) is True
    assert is_empty_id("") is True
    assert is_empty_id("gene ") is True
    assert is_empty_id("gene None") is True
    assert is_empty_id("gene ENSG00000139618") is False


def test_merge_graphs_keeps_nodes_with_empty_id_separate():
    g1 = {"nodes": [{"data": {"id": "gene ", "type": "gene", "name": "A"}}], "edges": []}
    g2 = {"nodes": [{"data": {"id": "gene ", "type": "gene", "name": "B"}}], "edges": []}
    merged = merge_graphs([g1, g2])
    assert len(merged["nodes"]) == 2


def test_merge_graphs_keeps_nodes_with_none_id_separate():
    g1 = {"nodes": [{"data": {"id": None, "type": "gene", "name": "A"}}], "edges": []}
    g2 = {"nodes": [{"data": {"id": None, "type": "gene", "name": "B"}}], "edges": []}
    merged = merge_graphs([g1, g2])
    assert len(merged["nodes"]) == 2


def test_merge_graphs_keeps_nodes_with_missing_id_separate():
    g1 = {"nodes": [{"data": {"type": "gene", "name": "A"}}], "edges": []}
    g2 = {"nodes": [{"data": {"type": "gene", "name": "B"}}], "edges": []}
    merged = merge_graphs([g1, g2])
    assert len(merged["nodes"]) == 2


def test_build_custom_request_prefixes_node_ids_with_label():
    requests = {
        "nodes": [
            {"node_id": "n1", "type": "gene", "id": "ensg00000139618",
             "properties": {}},
            {"node_id": "n2", "type": "transcript", "id": "enst00000415118",
             "properties": {}},
        ],
        "predicates": [],
    }
    custom_requests, custom_node_map = build_custom_request(requests)

    assert custom_requests["nodes"][0]["id"] == "gene_ensg00000139618"
    assert custom_requests["nodes"][1]["id"] == "transcript_enst00000415118"
    assert custom_node_map["n1"]["id"] == "gene_ensg00000139618"
    assert requests["nodes"][0]["id"] == "ensg00000139618"


def test_build_custom_request_does_not_prefix_empty_ids():
    requests = {
        "nodes": [{"node_id": "n1", "type": "gene", "id": "", "properties": {}}],
        "predicates": [],
    }
    custom_requests, _ = build_custom_request(requests)
    assert custom_requests["nodes"][0]["id"] == ""


def test_build_custom_request_preprocesses_colon_ids():
    requests = {
        "nodes": [
            {"node_id": "d1", "type": "disease", "id": "DOID:123",
             "properties": {}},
            {"node_id": "d2", "type": "disease", "id": "MONDO:0010",
             "properties": {}},
        ],
        "predicates": [],
    }
    custom_requests, _ = build_custom_request(requests)
    assert custom_requests["nodes"][0]["id"] == "disease_doid_123"
    assert custom_requests["nodes"][1]["id"] == "disease_mondo_0010"