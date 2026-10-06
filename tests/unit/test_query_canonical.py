"""Unit tests for app.lib.query_canonical: canonical renaming and fingerprints.

Requests here are already in validated form (as produced by validate_request),
because canonicalize_graph always runs after validation in process_query.
"""
import copy

import pytest

from app.lib.query_canonical import canonicalize_graph, query_fingerprint

# Fingerprint settings shared by every test; only the request changes.
FINGERPRINT_SETTINGS = {
    "species": "human",
    "data_source": "all",
    "limit": None,
    "node_only": False,
    "properties": True,
    "db_type": "cypher",
}

# Fingerprint of make_request() without logic, computed with the code from
# before logic support existed. It must never change: annotations already
# stored in MongoDB are found by this value.
FINGERPRINT_WITHOUT_LOGIC = "60b0f55693572bb9b6b3211c1f75822bedbd246ffb776edef2ca80706d1f8b8c"


def make_request(logic=None):
    """Return a fresh request using caller-chosen labels, listed out of canonical order.

    Canonically: g (gene) -> n0, pw (pathway) -> n1, tx (transcript) -> n2,
    has_pathway (n0 -> n1) -> p0, has_tx (n0 -> n2) -> p1.
    """
    request = {
        "nodes": [
            {"node_id": "pw", "id": "", "type": "pathway", "properties": {}},
            {"node_id": "tx", "id": "", "type": "transcript", "properties": {}},
            {"node_id": "g", "id": "", "type": "gene", "properties": {"gene_name": "FTO"}},
        ],
        "predicates": [
            {"predicate_id": "has_pathway", "type": "genes pathways", "source": "g", "target": "pw"},
            {"predicate_id": "has_tx", "type": "transcribed to", "source": "g", "target": "tx"},
        ],
    }
    if logic is not None:
        request["logic"] = logic
    return request


def fingerprint(request):
    """Canonicalize a request and return its fingerprint, as process_query does."""
    canonical_request, _ = canonicalize_graph(request)
    return query_fingerprint(canonical_request, **FINGERPRINT_SETTINGS)


def canonical_logic(logic):
    """Return the 'logic' section of make_request(logic) after canonicalization."""
    canonical_request, _ = canonicalize_graph(make_request(logic))
    return canonical_request["logic"]


# --- Existing renaming behavior ---------------------------------------------

def test_nodes_and_predicates_get_canonical_names():
    canonical_request, old_to_new_node = canonicalize_graph(make_request())

    assert old_to_new_node == {"g": "n0", "pw": "n1", "tx": "n2"}
    assert [(n["node_id"], n["type"]) for n in canonical_request["nodes"]] == [
        ("n0", "gene"), ("n1", "pathway"), ("n2", "transcript"),
    ]
    assert [(p["predicate_id"], p["source"], p["target"]) for p in canonical_request["predicates"]] == [
        ("p0", "n0", "n1"), ("p1", "n0", "n2"),
    ]


def test_predicates_without_ids_still_get_canonical_ids():
    request = make_request()
    for predicate in request["predicates"]:
        del predicate["predicate_id"]

    canonical_request, _ = canonicalize_graph(request)

    assert [p["predicate_id"] for p in canonical_request["predicates"]] == ["p0", "p1"]


def test_temporary_fields_do_not_leak_into_the_result():
    canonical_request, _ = canonicalize_graph(make_request())

    for item in canonical_request["nodes"] + canonical_request["predicates"]:
        assert not [key for key in item if key.startswith("_")]


# --- Renaming references inside logic -----------------------------------------

def test_logic_node_reference_is_renamed():
    logic = canonical_logic({
        "operator": "AND",
        "children": [{"operator": "NOT", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}],
    })

    assert logic["children"][0]["nodes"]["node_id"] == "n0"


def test_logic_predicate_reference_is_renamed():
    logic = canonical_logic({
        "operator": "AND",
        "children": [{"operator": "OR", "predicates": ["has_tx", "has_pathway"]}],
    })

    assert logic["children"][0]["predicates"] == ["p0", "p1"]


def test_nested_logic_references_are_renamed():
    logic = canonical_logic({
        "operator": "AND",
        "children": [{"operator": "OR", "children": [
            {"operator": "AND", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}},
            {"operator": "AND", "nodes": {"node_id": "g", "properties": {"gene_type": ["pseudogene"]}}},
        ]}],
    })

    nested = logic["children"][0]["children"]
    assert [child["nodes"]["node_id"] for child in nested] == ["n0", "n0"]


# --- Deterministic order inside logic -----------------------------------------

def test_values_and_predicate_ids_are_sorted_and_deduplicated():
    logic = canonical_logic({
        "operator": "AND",
        "children": [
            {"operator": "OR", "nodes": {"node_id": "g", "properties": {"gene_type": ["pseudogene", "lncRNA", "lncRNA"]}}},
            {"operator": "OR", "predicates": ["has_tx", "has_pathway", "has_tx"]},
        ],
    })

    groups = {("nodes" if "nodes" in child else "predicates"): child for child in logic["children"]}
    assert groups["nodes"]["nodes"]["properties"]["gene_type"] == ["lncRNA", "pseudogene"]
    assert groups["predicates"]["predicates"] == ["p0", "p1"]


def test_children_order_does_not_change_the_canonical_form():
    property_group = {"operator": "NOT", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}
    predicate_group = {"operator": "OR", "predicates": ["has_pathway", "has_tx"]}

    forward = canonical_logic({"operator": "AND", "children": [property_group, predicate_group]})
    backward = canonical_logic({"operator": "AND", "children": [predicate_group, property_group]})

    assert forward == backward


def test_unknown_keys_in_logic_are_dropped():
    logic = canonical_logic({
        "operator": "AND",
        "comment": "ignored",
        "children": [{"operator": "NOT", "label": "ignored",
                      "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}],
    })

    assert logic == {
        "operator": "AND",
        "children": [{"operator": "NOT", "nodes": {"node_id": "n0", "properties": {"gene_type": ["lncRNA"]}}}],
    }


# --- The caller's request is never modified -----------------------------------

def test_original_request_is_not_modified():
    request = make_request({
        "operator": "AND",
        "children": [{"operator": "NOT", "predicates": ["has_pathway"]}],
    })
    original = copy.deepcopy(request)

    canonicalize_graph(request)

    assert request == original


# --- Fingerprints ----------------------------------------------------------------

@pytest.mark.parametrize("logic", [None, "absent"], ids=["logic-null", "logic-absent"])
def test_fingerprint_without_logic_is_unchanged(logic):
    request = make_request()
    if logic is None:
        request["logic"] = None

    assert fingerprint(request) == FINGERPRINT_WITHOUT_LOGIC


def test_same_question_with_different_labels_has_same_fingerprint():
    logic = {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["has_pathway"]}]}
    relabeled = make_request()
    renames = {"pw": "pathway_1", "tx": "transcript_1", "g": "gene_1"}
    for node in relabeled["nodes"]:
        node["node_id"] = renames[node["node_id"]]
    for predicate in relabeled["predicates"]:
        predicate["source"] = renames[predicate["source"]]
        predicate["target"] = renames[predicate["target"]]
    relabeled["predicates"][0]["predicate_id"] = "edge_a"
    relabeled["logic"] = {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["edge_a"]}]}

    assert fingerprint(make_request(logic)) == fingerprint(relabeled)


def test_equivalent_logic_in_different_order_has_same_fingerprint():
    first = make_request({"operator": "AND", "children": [
        {"operator": "OR", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA", "pseudogene"]}}},
        {"operator": "NOT", "predicates": ["has_pathway"]},
    ]})
    second = make_request({"operator": "AND", "children": [
        {"operator": "NOT", "predicates": ["has_pathway", "has_pathway"]},
        {"operator": "OR", "nodes": {"node_id": "g", "properties": {"gene_type": ["pseudogene", "lncRNA"]}}},
    ]})

    assert fingerprint(first) == fingerprint(second)


@pytest.mark.parametrize(
    "logic_a, logic_b",
    [
        (
            None,
            {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["has_pathway"]}]},
        ),
        (
            {"operator": "AND", "children": [
                {"operator": "OR", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}]},
            {"operator": "AND", "children": [
                {"operator": "NOT", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}]},
        ),
        (
            {"operator": "AND", "children": [
                {"operator": "NOT", "nodes": {"node_id": "g", "properties": {"gene_type": ["lncRNA"]}}}]},
            {"operator": "AND", "children": [
                {"operator": "NOT", "nodes": {"node_id": "g", "properties": {"gene_type": ["pseudogene"]}}}]},
        ),
        (
            {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["has_pathway"]}]},
            {"operator": "AND", "children": [{"operator": "NOT", "predicates": ["has_tx"]}]},
        ),
    ],
    ids=[
        "plain-vs-not",
        "or-vs-not-on-same-property",
        "different-excluded-value",
        "different-negated-predicate",
    ],
)
def test_different_logic_has_different_fingerprint(logic_a, logic_b):
    assert fingerprint(make_request(logic_a)) != fingerprint(make_request(logic_b))