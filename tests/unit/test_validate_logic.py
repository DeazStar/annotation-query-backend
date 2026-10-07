"""Unit tests for the boolean 'logic' section handled by app.lib.validator.

These tests need no database and no schema files: validate_request only uses
the schema to look up relationship keys, so a two-entry dict is enough.
"""
import re

import pytest

from app.lib.validator import (
    MAX_LOGIC_DEPTH,
    LogicValidationError,
    negated_predicate_ends,
    validate_logic,
    validate_request,
)

# The only two relationships the test requests use.
SCHEMA = {
    "gene_transcribed_to_transcript": {},
    "gene_genes_pathways_pathway": {},
    "transcript_translates_to_protein": {},
}


def make_request(logic=None):
    """Return a fresh request: gene n0 -> transcript n1 (p0), gene n0 -> pathway n2 (p1)."""
    request = {
        "nodes": [
            {"node_id": "n0", "id": "", "type": "gene", "properties": {}},
            {"node_id": "n1", "id": "", "type": "transcript", "properties": {}},
            {"node_id": "n2", "id": "", "type": "pathway", "properties": {}},
        ],
        "predicates": [
            {"type": "transcribed to", "source": "n0", "target": "n1"},
            {"type": "genes pathways", "source": "n0", "target": "n2"},
        ],
    }
    if logic is not None:
        request["logic"] = logic
    return request


def node_group(operator, node_id, properties):
    """Return a property group: conditions on one node's properties."""
    return {"operator": operator, "nodes": {"node_id": node_id, "properties": properties}}


def predicate_group(operator, predicate_ids):
    """Return a predicate group: a list of predicate ids."""
    return {"operator": operator, "predicates": predicate_ids}


def nested_group(operator, children):
    """Return a nested group: a list of sub-groups."""
    return {"operator": operator, "children": children}


def assert_rejected(request, message):
    """Assert validate_request rejects the request with exactly this message."""
    with pytest.raises(LogicValidationError, match=f"^{re.escape(message)}$"):
        validate_request(request, SCHEMA, None)


# --- Requests without logic keep their current behavior ----------------------

@pytest.mark.parametrize("logic", [None, "absent"], ids=["logic-null", "logic-absent"])
def test_request_without_logic_is_unchanged(logic):
    request = make_request()
    if logic is None:
        request["logic"] = None

    node_map = validate_request(request, SCHEMA, None)

    assert set(node_map) == {"n0", "n1", "n2"}
    assert request.get("logic") is None


# --- Valid logic is accepted and normalized ----------------------------------

def test_property_group_is_normalized():
    request = make_request({
        "children": [node_group("or", "n_0", {"gene_name": "FTO", "gene_type": "protein_coding"})],
    })

    validate_request(request, SCHEMA, None)

    assert request["logic"] == {
        "operator": "AND",
        "children": [
            node_group("OR", "n0", {"gene_name": ["FTO"], "gene_type": ["protein_coding"]}),
        ],
    }


def test_list_values_and_numbers_become_lists_of_strings():
    request = make_request({
        "children": [node_group("NOT", "n0", {"gene_type": ["lncRNA", "pseudogene"], "taxon_id": 9606})],
    })

    validate_request(request, SCHEMA, None)

    properties = request["logic"]["children"][0]["nodes"]["properties"]
    assert properties == {"gene_type": ["lncRNA", "pseudogene"], "taxon_id": ["9606"]}


def test_chr_values_get_the_chr_prefix():
    request = make_request({"children": [node_group("OR", "n0", {"chr": [16, "chr1"]})]})

    validate_request(request, SCHEMA, None)

    assert request["logic"]["children"][0]["nodes"]["properties"]["chr"] == ["chr16", "chr1"]


@pytest.mark.parametrize(
    "logic",
    [
        {"children": [predicate_group("OR", ["p0", "p1"])]},
        {"children": [predicate_group("NOT", ["p1"])]},
        {"children": [node_group("OR", "n0", {"gene_name": "FTO"}), predicate_group("NOT", ["p1"])]},
        {"operator": "OR", "children": [
            node_group("AND", "n0", {"gene_name": "FTO", "gene_type": "protein_coding"}),
            node_group("AND", "n0", {"gene_name": "BRCA1"}),
        ]},
        {"children": [nested_group("OR", [
            node_group("AND", "n0", {"gene_name": "FTO", "gene_type": "protein_coding"}),
            node_group("AND", "n0", {"gene_name": "BRCA1"}),
        ])]},
    ],
    ids=[
        "predicate-or-sharing-a-node",
        "predicate-not",
        "property-and-predicate-groups-under-and-root",
        "or-root-on-one-node",
        "nested-groups-on-one-node",
    ],
)
def test_valid_logic_is_accepted(logic):
    validate_request(make_request(logic), SCHEMA, None)


def test_validate_logic_can_be_called_directly():
    logic = {"children": [node_group("OR", "n0", {"gene_name": "FTO"})]}
    node_map = {"n0": {"node_id": "n0", "type": "gene", "properties": {}}}

    validate_logic(logic, node_map, predicate_map={})

    assert logic["operator"] == "AND"


# --- Invalid structure -------------------------------------------------------

@pytest.mark.parametrize(
    "logic, message",
    [
        ([], "'logic' must be an object"),
        ({"operator": "AND"}, "'logic' must contain a 'children' list and no 'nodes' or 'predicates'"),
        ({"children": []}, "'children' must be a non-empty list"),
        ({"children": ["not a group"]}, "each logic group must be an object"),
        (
            {"children": [node_group("NOR", "n0", {"gene_name": "FTO"})]},
            "invalid operator 'NOR'; expected AND, OR or NOT",
        ),
        (
            {"children": [{"nodes": {"node_id": "n0", "properties": {"gene_name": "FTO"}}}]},
            "invalid operator None; expected AND, OR or NOT",
        ),
        (
            {"children": [{"operator": "OR", "predicates": ["p0"],
                           "nodes": {"node_id": "n0", "properties": {"gene_name": "FTO"}}}]},
            "each logic group needs exactly one of 'nodes', 'predicates', 'children'",
        ),
        (
            {"children": [{"operator": "OR"}]},
            "each logic group needs exactly one of 'nodes', 'predicates', 'children'",
        ),
    ],
    ids=[
        "logic-not-an-object",
        "root-without-children",
        "empty-children",
        "group-not-an-object",
        "unknown-operator",
        "missing-operator-in-group",
        "two-kinds-in-one-group",
        "no-kind-in-group",
    ],
)
def test_invalid_structure_is_rejected(logic, message):
    assert_rejected(make_request(logic), message)


def test_nesting_deeper_than_the_limit_is_rejected():
    group = node_group("AND", "n0", {"gene_name": "FTO"})
    for _ in range(MAX_LOGIC_DEPTH):
        group = nested_group("OR", [group])

    assert_rejected(
        make_request({"children": [group]}),
        f"'logic' is nested too deeply: the maximum depth is {MAX_LOGIC_DEPTH}",
    )


# --- Invalid references and values ------------------------------------------

@pytest.mark.parametrize(
    "logic, message",
    [
        (
            {"children": [node_group("OR", "n9", {"gene_name": "FTO"})]},
            "logic refers to unknown node 'n9'",
        ),
        (
            {"children": [predicate_group("OR", ["p7"])]},
            "logic refers to unknown predicate 'p7'",
        ),
        (
            {"children": [predicate_group("OR", [])]},
            "'predicates' in a logic group must be a non-empty list of predicate ids",
        ),
        (
            {"children": [node_group("OR", "n0", {})]},
            "logic group on node 'n0' has no properties",
        ),
        (
            {"children": [node_group("OR", "n0", {"gene_name": True})]},
            "property 'gene_name' in 'logic' must be a string or a list of strings",
        ),
        (
            {"children": [node_group("OR", "n0", {"gene_name": []})]},
            "property 'gene_name' in 'logic' must not be an empty list",
        ),
        (
            {"children": [node_group("OR", "n0", {"gene_name": ["  "]})]},
            "property 'gene_name' in 'logic' must not be empty",
        ),
    ],
    ids=[
        "unknown-node",
        "unknown-predicate",
        "empty-predicate-list",
        "no-properties",
        "boolean-value",
        "empty-value-list",
        "blank-value",
    ],
)
def test_invalid_reference_or_value_is_rejected(logic, message):
    assert_rejected(make_request(logic), message)


def test_property_in_both_node_and_logic_is_rejected():
    request = make_request({"children": [node_group("NOT", "n0", {"gene_type": "lncRNA"})]})
    request["nodes"][0]["properties"]["gene_type"] = "protein_coding"

    assert_rejected(
        request,
        "property 'gene_type' of node 'n0' is in both 'properties' and 'logic'; keep it in one place",
    )


# --- Outside the v1 scope ----------------------------------------------------

@pytest.mark.parametrize(
    "logic, message",
    [
        (
            {"children": [{"operator": "NOT", "nodes": {"node_id": "n0"}}]},
            "not supported yet: NOT on the whole node 'n0'; list the properties to exclude instead",
        ),
        (
            {"children": [node_group("OR", "n0", {"start": "100"})]},
            "not supported yet: position filter 'start' inside 'logic'",
        ),
        (
            {"operator": "OR", "children": [
                node_group("AND", "n0", {"gene_name": "FTO"}),
                predicate_group("AND", ["p1"]),
            ]},
            "not supported yet: predicate groups must sit directly under a root with operator AND",
        ),
        (
            {"children": [nested_group("OR", [
                node_group("AND", "n0", {"gene_name": "FTO"}),
                predicate_group("AND", ["p1"]),
            ])]},
            "not supported yet: predicate groups must sit directly under a root with operator AND",
        ),
        (
            {"operator": "OR", "children": [
                node_group("AND", "n0", {"gene_name": "FTO"}),
                node_group("AND", "n1", {"transcript_name": "FTO-201"}),
            ]},
            "not supported yet: a combined group must refer to exactly one node, got ['n0', 'n1']",
        ),
    ],
    ids=[
        "not-on-whole-node",
        "position-filter",
        "node-or-predicate-at-root",
        "node-or-predicate-nested",
        "or-across-two-nodes",
    ],
)
def test_outside_v1_scope_is_rejected(logic, message):
    assert_rejected(make_request(logic), message)


def test_or_between_predicates_sharing_no_node_is_rejected():
    request = make_request({"children": [predicate_group("OR", ["p0", "p1"])]})
    # Re-point p1 so it starts at a second gene n3: p0 and p1 then share no node.
    # The extra predicate p2 keeps the graph connected, so the request is realistic.
    request["nodes"].append({"node_id": "n3", "id": "", "type": "gene", "properties": {}})
    request["predicates"][1]["source"] = "n3"
    request["predicates"].append({"type": "transcribed to", "source": "n3", "target": "n1"})

    assert_rejected(request, "not supported yet: OR between predicates ['p0', 'p1'] that share no node")


# --- Predicate ids must be unique when logic is used ------------------------

def _give_predicate_ids(request, first_id, second_id):
    """Set the ids of the two base predicates; None leaves one for the validator to assign."""
    for predicate, predicate_id in zip(request["predicates"], (first_id, second_id)):
        if predicate_id is not None:
            predicate["predicate_id"] = predicate_id


@pytest.mark.parametrize(
    "first_id, second_id, duplicate",
    [
        ("x", "x", "x"),
        # The first predicate gets the auto-assigned id 'p0', which collides with the given one.
        (None, "p0", "p0"),
    ],
    ids=["same-id-sent-twice", "given-id-collides-with-assigned-id"],
)
def test_duplicate_predicate_ids_are_rejected_with_logic(first_id, second_id, duplicate):
    request = make_request({"children": [predicate_group("NOT", [duplicate])]})
    _give_predicate_ids(request, first_id, second_id)

    assert_rejected(
        request,
        f"duplicate predicate_id '{duplicate}'; when 'logic' is used, give every predicate its own predicate_id",
    )


def test_duplicate_predicate_ids_are_still_accepted_without_logic():
    # Without logic nothing refers to predicates by id, and canonicalization
    # renames them to p0, p1, ... before any query is built.
    request = make_request()
    _give_predicate_ids(request, "x", "x")

    validate_request(request, SCHEMA, None)

    assert [predicate["predicate_id"] for predicate in request["predicates"]] == ["x", "x"]


# --- NOT on a predicate: which end is kept -----------------------------------

def edge(predicate_id, source, target):
    """Return a bare predicate; negated_predicate_ends only reads ids and ends."""
    return {"predicate_id": predicate_id, "type": "any", "source": source, "target": target}


# gene n0 -> transcript n1 (p0), gene n0 -> pathway n2 (p1), transcript n1 -> protein n3 (p2)
P0, P1, P2 = edge("p0", "n0", "n1"), edge("p1", "n0", "n2"), edge("p2", "n1", "n3")


@pytest.mark.parametrize(
    "negated, predicates, kept, far",
    [
        (P1, [P1], "n0", "n2"),
        (P1, [P0, P1], "n0", "n2"),
        (P2, [P0, P2], "n1", "n3"),
        (P0, [P0, P2], "n1", "n0"),
        (P0, [P0, P1], "n0", "n1"),
    ],
    ids=[
        "whole-pattern-keeps-the-source",
        "far-end-is-the-unshared-end",
        "chain-negating-the-last-edge",
        "chain-negating-the-first-edge-keeps-the-target",
        "two-negated-edges-from-one-node",
    ],
)
def test_negated_predicate_ends(negated, predicates, kept, far):
    assert negated_predicate_ends(negated, predicates) == (kept, far)


@pytest.mark.parametrize(
    "negated, predicates, message",
    [
        (
            P0, [P0, P1, P2],
            "not supported yet: NOT on predicate 'p0', whose two nodes are both used by other predicates",
        ),
        (
            edge("p0", "n0", "n0"), [edge("p0", "n0", "n0")],
            "not supported yet: NOT on predicate 'p0', which starts and ends at the same node",
        ),
    ],
    ids=["both-ends-used-elsewhere", "self-loop"],
)
def test_negated_predicate_without_a_far_end_is_rejected(negated, predicates, message):
    with pytest.raises(LogicValidationError, match=f"^{re.escape(message)}$"):
        negated_predicate_ends(negated, predicates)


def test_not_on_the_only_predicate_is_accepted():
    request = {
        "nodes": [
            {"node_id": "n0", "id": "", "type": "gene", "properties": {}},
            {"node_id": "n2", "id": "", "type": "pathway", "properties": {}},
        ],
        "predicates": [{"type": "genes pathways", "source": "n0", "target": "n2"}],
        "logic": {"children": [predicate_group("NOT", ["p0"])]},
    }

    validate_request(request, SCHEMA, None)


def test_validator_rejects_not_when_both_ends_are_used_elsewhere():
    request = make_request({"children": [predicate_group("NOT", ["p0"])]})
    request["nodes"].append({"node_id": "n3", "id": "", "type": "protein", "properties": {}})
    request["predicates"].append({"type": "translates to", "source": "n1", "target": "n3"})

    assert_rejected(
        request,
        "not supported yet: NOT on predicate 'p0', whose two nodes are both used by other predicates",
    )


def test_predicate_in_two_groups_is_rejected():
    request = make_request({"children": [
        predicate_group("OR", ["p0", "p1"]),
        predicate_group("NOT", ["p1"]),
    ]})

    assert_rejected(request, "predicate 'p1' is used in more than one predicate group")


# --- Error type --------------------------------------------------------------

def test_logic_error_is_a_value_error():
    # process_query turns any exception from validate_request into HTTP 400,
    # and a ValueError subclass keeps that behavior.
    assert issubclass(LogicValidationError, ValueError)