"""Unit tests: label counts leave out what NOT excludes from the results.

A far node of a negated predicate is never returned, so it must not show up
as "pathway: 0" next to a result.
"""
import os

# task_handler imports the endpoint dependencies, which need a configured model.
os.environ.setdefault("LLM_MODEL", "openai")

from app.lib.result_formatter import Result_Formatter  # noqa: E402
from app.lib.validator import counted_nodes_and_predicates  # noqa: E402
from app.workers.task_handler import generate_empty_label_count  # noqa: E402


def make_request(logic=None):
    """Validated request: gene n0 -> transcript n1 (p0), gene n0 -> pathway n2 (p1)."""
    request = {
        "nodes": [
            {"node_id": "n0", "id": "", "type": "gene", "properties": {}},
            {"node_id": "n1", "id": "", "type": "transcript", "properties": {}},
            {"node_id": "n2", "id": "", "type": "pathway", "properties": {}},
        ],
        "predicates": [
            {"predicate_id": "p0", "type": "transcribed to", "source": "n0", "target": "n1"},
            {"predicate_id": "p1", "type": "genes pathways", "source": "n0", "target": "n2"},
        ],
    }
    if logic is not None:
        request["logic"] = logic
    return request


NOT_PATHWAY = {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["p1"]}]}


def ids(items, key):
    return [item[key] for item in items]


def test_negated_predicate_and_its_far_node_are_not_counted():
    nodes, predicates = counted_nodes_and_predicates(make_request(NOT_PATHWAY))

    assert ids(nodes, "node_id") == ["n0", "n1"]
    assert ids(predicates, "predicate_id") == ["p0"]


def test_everything_is_counted_without_negated_predicates():
    for logic in (
        None,
        {"operator": "AND", "children": [{"operator": "OR", "predicates": ["p0", "p1"]}]},
        {"operator": "AND", "children": [
            {"operator": "NOT", "nodes": {"node_id": "n0", "properties": {"gene_type": ["lncRNA"]}}}]},
    ):
        nodes, predicates = counted_nodes_and_predicates(make_request(logic))

        assert ids(nodes, "node_id") == ["n0", "n1", "n2"]
        assert ids(predicates, "predicate_id") == ["p0", "p1"]


def test_a_type_still_counted_when_another_node_of_it_is_kept():
    # Excluded by node, not by type: a kept pathway keeps the "pathway" label.
    request = make_request(NOT_PATHWAY)
    request["nodes"].append({"node_id": "n3", "id": "", "type": "pathway", "properties": {}})
    request["predicates"].append(
        {"predicate_id": "p2", "type": "genes pathways", "source": "n1", "target": "n3"}
    )

    nodes, _ = counted_nodes_and_predicates(request)

    assert ids(nodes, "type") == ["gene", "transcript", "pathway"]


def test_count_formatter_shows_no_label_for_the_far_node():
    request = make_request(NOT_PATHWAY)
    nodes, predicates = counted_nodes_and_predicates(request)
    count_rows = [
        {"total_nodes": 4, "total_edges": 3},
        {"n0_gene": 1, "n1_transcript": 3, "p0_transcribed_to": 3},
    ]

    counts = Result_Formatter().format_result(
        count_rows, "neo4j", {"nodes": nodes, "predicates": predicates, "properties": False}, "count"
    )

    assert counts["node_count_by_label"] == [{"label": "gene", "count": 1}, {"label": "transcript", "count": 3}]
    assert counts["edge_count_by_label"] == [{"label": "transcribed_to", "count": 3}]


def test_empty_label_count_after_a_failure_leaves_out_the_far_node():
    update = generate_empty_label_count(make_request(NOT_PATHWAY))

    assert update == {
        "node_count_by_label": [{"gene": 0}, {"transcript": 0}],
        "edge_count_by_label": [{"transcribed to": 0}],
    }