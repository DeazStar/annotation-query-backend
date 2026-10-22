"""Unit tests for titles built without an LLM (LLMHandler.generate_title_no_llm).

The sentence describes what a query returns; the boolean 'logic' section is
described after ", where", so a NOT or OR query does not get the title of
the plain query.
"""
import os

import pytest

# LLMHandler needs a model type to be configured; the tests never call the model.
os.environ.setdefault("LLM_MODEL", "openai")

from app.services.llm_handler import LLMHandler  # noqa: E402


def node(node_id, node_type, properties=None):
    return {"node_id": node_id, "id": "", "type": node_type, "properties": properties or {}}


TRANSCRIBED = {"predicate_id": "p0", "type": "transcribed to", "source": "n0", "target": "n1"}
IN_PATHWAY = {"predicate_id": "p1", "type": "genes pathways", "source": "n0", "target": "n2"}
FTO_TRANSCRIPT_PATHWAY = [node("n0", "gene", {"gene_name": "FTO"}), node("n1", "transcript"), node("n2", "pathway")]


def logic(*children):
    return {"operator": "AND", "children": list(children)}


def property_group(operator, properties):
    return {"operator": operator, "nodes": {"node_id": "n0", "properties": properties}}


@pytest.fixture(scope="module")
def handler():
    # generate_title_no_llm never calls the model, so these tests pass whether
    # or not an LLM API key is configured in the environment.
    return LLMHandler()


def title(handler, nodes, predicates, logic_section=None):
    request = {"nodes": nodes, "predicates": predicates}
    if logic_section is not None:
        request["logic"] = logic_section
    return handler.generate_title_no_llm(request, {n["node_id"]: n for n in nodes})


@pytest.mark.parametrize(
    "nodes, predicates, logic_section, expected",
    [
        (
            FTO_TRANSCRIPT_PATHWAY, [TRANSCRIBED, IN_PATHWAY], None,
            "Fto transcribed to a Transcript, Fto genes pathways",
        ),
        (
            [node("n0", "gene")], [],
            logic(property_group("OR", {"gene_name": ["FTO"], "gene_type": ["protein_coding"]})),
            "Selection: Unnamed Gene, where Gene has gene name FTO or gene type protein_coding",
        ),
        (
            [node("n0", "gene", {"gene_name": "FTO"})], [],
            logic(property_group("NOT", {"gene_type": ["lncRNA", "pseudogene"]})),
            "Selection: Fto, where Gene has no gene type lncRNA or gene type pseudogene",
        ),
        (
            [node("n0", "gene")], [],
            logic({"operator": "OR", "children": [
                property_group("AND", {"gene_name": ["FTO"], "gene_type": ["protein_coding"]}),
                property_group("AND", {"gene_name": ["BRCA1"]}),
            ]}),
            "Selection: Unnamed Gene, where Gene has (gene name FTO and gene type protein_coding) or gene name BRCA1",
        ),
        (
            FTO_TRANSCRIPT_PATHWAY, [TRANSCRIBED, IN_PATHWAY],
            logic({"operator": "NOT", "predicates": ["p1"]}),
            "Fto transcribed to a Transcript, where Gene has no 'genes pathways' link to a Pathway",
        ),
        (
            [node("n0", "gene"), node("n2", "pathway")], [IN_PATHWAY],
            logic({"operator": "NOT", "predicates": ["p1"]}),
            "Selection: Unnamed Gene, where Gene has no 'genes pathways' link to a Pathway",
        ),
        (
            FTO_TRANSCRIPT_PATHWAY, [TRANSCRIBED, IN_PATHWAY],
            logic({"operator": "OR", "predicates": ["p0", "p1"]}),
            "Fto transcribed to a Transcript, Fto genes pathways, where Gene has a 'transcribed to' link "
            "to a Transcript or a 'genes pathways' link to a Pathway",
        ),
    ],
    ids=[
        "no-logic-unchanged",
        "property-or",
        "property-not",
        "nested-groups",
        "not-on-an-edge",
        "not-on-the-only-edge",
        "or-between-edges",
    ],
)
def test_no_llm_title(handler, nodes, predicates, logic_section, expected):
    assert title(handler, nodes, predicates, logic_section) == expected


def test_titles_of_plain_and_negated_queries_differ(handler):
    plain = title(handler, FTO_TRANSCRIPT_PATHWAY, [TRANSCRIBED, IN_PATHWAY])
    negated = title(handler, FTO_TRANSCRIPT_PATHWAY, [TRANSCRIBED, IN_PATHWAY],
                    logic({"operator": "NOT", "predicates": ["p1"]}))

    assert plain != negated