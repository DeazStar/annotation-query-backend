"""Integration test: boolean 'logic' queries against a real Neo4j.

The unit tests compare query text; this runs the queries. It loads a small
graph whose answers are known in advance into an EMPTY Neo4j, sends each
request through the same steps as process_query (validate_request ->
canonicalize_graph -> heuristic_sort -> query_Generator) and checks:

  1. the right nodes come back;
  2. the count queries agree with the result query (totals and per label);
  3. no plan uses a CartesianProduct (EXPLAIN);
  4. no step makes more rows than the graph has nodes and edges (PROFILE):
     a query that builds transcripts x pathways rows before counting fails.

It is skipped unless LOGIC_TEST_NEO4J_URI is set. Start a throwaway Neo4j 5
(the CALL (x) {...} syntax needs 5.21 or later):

    docker run --rm -d --name logic-test -p 7690:7687 \\
        -e NEO4J_AUTH=neo4j/logic-test-pw neo4j:5.26-community

Then, from the repository root, one command at a time:

    LOGIC_TEST_NEO4J_URI=bolt://localhost:7690 LOGIC_TEST_NEO4J_PASSWORD=logic-test-pw \\
        python -m pytest tests/integration/cypher/test_logic_neo4j.py --noconftest -v

    docker stop logic-test

The test refuses to run on a database that is not empty, and deletes what it
loaded afterwards.
"""
import copy
import os
import time
from collections import defaultdict

import pytest

from app.lib.heuristic_sort import heuristic_sort
from app.lib.query_canonical import canonicalize_graph
from app.lib.validator import validate_request
from app.services.cypher_generator import CypherQueryGenerator

neo4j = pytest.importorskip("neo4j")

NEO4J_URI = os.environ.get("LOGIC_TEST_NEO4J_URI")
pytestmark = pytest.mark.skipif(not NEO4J_URI, reason="set LOGIC_TEST_NEO4J_URI to run against a real Neo4j")

SCHEMA = {
    "gene_transcribed_to_transcript": {},
    "gene_genes_pathways_pathway": {},
    "transcript_translates_to_protein": {},
}

# FTO: 3 transcripts (only FTO-201 makes a protein), pathways Insulin signaling and Obesity.
# GENE_A: 2 transcripts, no pathway. GENE_B: no transcript, pathway Obesity, no gene_type.
# GENE_C: nothing. ORPHAN-1: a transcript from no gene. HUB: 20 transcripts that each
# make a protein, and 20 pathways: a flat MATCH over both edges makes 400 rows for it.
HUB_FAN_OUT = 20
SEED = f"""
CREATE (fto:gene {{id: 'ENSG00000140718', gene_name: 'FTO', gene_type: 'protein_coding'}}),
       (a:gene {{id: 'GENEA', gene_name: 'GENE_A', gene_type: 'lncRNA'}}),
       (b:gene {{id: 'GENEB', gene_name: 'GENE_B'}}),
       (c:gene {{id: 'GENEC', gene_name: 'GENE_C', gene_type: 'pseudogene'}}),
       (t1:transcript {{id: 'T1', transcript_name: 'FTO-201', transcript_type: 'protein_coding'}}),
       (t2:transcript {{id: 'T2', transcript_name: 'FTO-202', transcript_type: 'retained_intron'}}),
       (t3:transcript {{id: 'T3', transcript_name: 'FTO-203', transcript_type: 'protein_coding'}}),
       (a1:transcript {{id: 'A1', transcript_name: 'A-201', transcript_type: 'lncRNA'}}),
       (a2:transcript {{id: 'A2', transcript_name: 'A-202', transcript_type: 'lncRNA'}}),
       (o1:transcript {{id: 'O1', transcript_name: 'ORPHAN-1', transcript_type: 'protein_coding'}}),
       (insulin:pathway {{id: 'P1', pathway_name: 'Insulin signaling'}}),
       (obesity:pathway {{id: 'P2', pathway_name: 'Obesity'}}),
       (pr1:protein {{id: 'PR1', protein_name: 'PROT1'}}),
       (pr2:protein {{id: 'PR2', protein_name: 'PROT2'}}),
       (fto)-[:transcribed_to]->(t1), (fto)-[:transcribed_to]->(t2), (fto)-[:transcribed_to]->(t3),
       (a)-[:transcribed_to]->(a1), (a)-[:transcribed_to]->(a2),
       (fto)-[:genes_pathways]->(insulin), (fto)-[:genes_pathways]->(obesity),
       (b)-[:genes_pathways]->(obesity),
       (t1)-[:translates_to]->(pr1), (o1)-[:translates_to]->(pr2)
WITH 1 AS one
CREATE (hub:gene {{id: 'HUB', gene_name: 'HUB', gene_type: 'protein_coding'}})
WITH hub
UNWIND range(1, {HUB_FAN_OUT}) AS i
CREATE (hub)-[:transcribed_to]->(:transcript {{id: 'HT' + i, transcript_name: 'HUB-T' + i,
                                               transcript_type: 'protein_coding'}})
               -[:translates_to]->(:protein {{id: 'HP' + i, protein_name: 'HUB-P' + i}}),
       (hub)-[:genes_pathways]->(:pathway {{id: 'HW' + i, pathway_name: 'HUB-PW' + i}})
"""

HUB_TRANSCRIPTS = {f"HUB-T{i}" for i in range(1, HUB_FAN_OUT + 1)}


def node(node_id, node_type, properties=None):
    return {"node_id": node_id, "id": "", "type": node_type, "properties": properties or {}}


def edge(predicate_id, predicate_type, source, target):
    return {"predicate_id": predicate_id, "type": predicate_type, "source": source, "target": target}


def props(operator, node_id, properties):
    return {"operator": operator, "nodes": {"node_id": node_id, "properties": properties}}


def preds(operator, *predicate_ids):
    return {"operator": operator, "predicates": list(predicate_ids)}


GENE_TX = edge("tx", "transcribed to", "g", "t")
GENE_PW = edge("pw", "genes pathways", "g", "p")
TX_PROTEIN = edge("tr", "translates to", "t", "r")
GENE_TX_PW_NODES = [node("g", "gene"), node("t", "transcript"), node("p", "pathway")]

# name -> (request, {label: names expected for that label})
SCENARIOS = {
    "no-logic-transcripts-and-pathways": (
        {"nodes": GENE_TX_PW_NODES, "predicates": [GENE_TX, GENE_PW]},
        {"gene": {"FTO", "HUB"}},
    ),
    "property-or": (
        {"nodes": [node("g", "gene")], "predicates": [],
         "logic": {"children": [props("OR", "g", {"gene_name": "FTO", "gene_type": "lncRNA"})]}},
        {"gene": {"FTO", "GENE_A"}},
    ),
    "single-group-at-the-root": (
        {"nodes": [node("g", "gene")], "predicates": [],
         "logic": props("OR", "g", {"gene_name": "FTO", "gene_type": "lncRNA"})},
        {"gene": {"FTO", "GENE_A"}},
    ),
    "property-not-keeps-missing-property": (
        {"nodes": [node("g", "gene")], "predicates": [],
         "logic": {"children": [props("NOT", "g", {"gene_type": "protein_coding"})]}},
        {"gene": {"GENE_A", "GENE_B", "GENE_C"}},
    ),
    "nested-groups-on-one-node": (
        {"nodes": [node("g", "gene")], "predicates": [],
         "logic": {"children": [{"operator": "OR", "children": [
             props("AND", "g", {"gene_name": "FTO", "gene_type": "protein_coding"}),
             props("AND", "g", {"gene_name": "GENE_C"})]}]}},
        {"gene": {"FTO", "GENE_C"}},
    ),
    "not-edge-no-pathway": (
        {"nodes": [node("g", "gene"), node("p", "pathway")], "predicates": [GENE_PW],
         "logic": {"children": [preds("NOT", "pw")]}},
        {"gene": {"GENE_A", "GENE_C"}},
    ),
    "not-two-edges-none-of-them": (
        {"nodes": GENE_TX_PW_NODES, "predicates": [GENE_TX, GENE_PW],
         "logic": {"children": [preds("NOT", "tx", "pw")]}},
        {"gene": {"GENE_C"}},
    ),
    "not-edge-next-to-a-required-edge": (
        {"nodes": GENE_TX_PW_NODES, "predicates": [GENE_TX, GENE_PW],
         "logic": {"children": [preds("NOT", "pw")]}},
        {"gene": {"GENE_A"}},
    ),
    "not-edge-far-filter": (
        {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway", {"pathway_name": "Insulin signaling"})],
         "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("NOT", "pw")]}},
        {"gene": {"GENE_A", "HUB"}},
    ),
    "not-edge-inside-an-arm": (
        {"nodes": [node("g", "gene", {"gene_name": "FTO"}), node("t", "transcript"), node("p", "pathway"),
                   node("r", "protein")],
         "predicates": [GENE_TX, GENE_PW, TX_PROTEIN], "logic": {"children": [preds("NOT", "tr")]}},
        {"transcript": {"FTO-202", "FTO-203"}},
    ),
    "not-edge-kept-end-is-the-target": (
        {"nodes": [node("g", "gene"), node("t", "transcript"), node("r", "protein")],
         "predicates": [GENE_TX, TX_PROTEIN], "logic": {"children": [preds("NOT", "tx")]}},
        {"transcript": {"ORPHAN-1"}},
    ),
    "or-edges": (
        {"nodes": GENE_TX_PW_NODES, "predicates": [GENE_TX, GENE_PW],
         "logic": {"children": [preds("OR", "tx", "pw")]}},
        {"gene": {"FTO", "GENE_A", "GENE_B", "HUB"}},
    ),
    "or-edges-far-filter": (
        {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway", {"pathway_name": "Insulin signaling"})],
         "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("OR", "tx", "pw")]}},
        {"gene": {"FTO", "GENE_A", "HUB"}},
    ),
    "or-alternative-with-an-anded-edge": (
        # (a transcript that makes a protein) OR (a pathway): GENE_A's transcripts make
        # none, and only the transcripts that make a protein come back.
        {"nodes": GENE_TX_PW_NODES + [node("r", "protein")], "predicates": [GENE_TX, GENE_PW, TX_PROTEIN],
         "logic": {"children": [preds("OR", "tx", "pw")]}},
        {"gene": {"FTO", "GENE_B", "HUB"}, "transcript": {"FTO-201"} | HUB_TRANSCRIPTS},
    ),
    "list-values-and-not-edge": (
        {"nodes": [node("g", "gene", {"gene_name": "FTO,GENE_A"}), node("t", "transcript"), node("p", "pathway")],
         "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("NOT", "pw")]}},
        {"gene": {"GENE_A"}},
    ),
}

NAME_KEYS = ("gene_name", "transcript_name", "pathway_name", "protein_name")


def build_queries(request):
    """Run the request through the same steps as process_query; return the three queries."""
    request = copy.deepcopy(request)
    validate_request(request, SCHEMA, None)
    canonical, _ = canonicalize_graph(request)
    node_map = {n["node_id"]: n for n in canonical["nodes"]}
    canonical = heuristic_sort(canonical, node_map)
    generator = CypherQueryGenerator.__new__(CypherQueryGenerator)  # no driver: text only
    return generator.query_Generator(canonical, node_map)


def entities_by_variable(records):
    """Return {variable: {element_id: entity}} for every node and edge in the result rows.

    Top-level values are a variable's node or edge; a list holds one CALL arm's
    maps, whose keys are the variables of that arm.
    """
    found = defaultdict(dict)

    def add(variable, value):
        if isinstance(value, (neo4j.graph.Node, neo4j.graph.Relationship)):
            found[variable][value.element_id] = value

    for record in records:
        for key, value in record.items():
            if isinstance(value, list):
                for row in value:
                    for variable, item in row.items():
                        add(variable, item)
            else:
                add(key, value)
    return found


def names_with_label(found, label):
    return {
        next(entity[key] for key in NAME_KEYS if key in entity)
        for entities in found.values()
        for entity in entities.values()
        if isinstance(entity, neo4j.graph.Node) and label in entity.labels
    }


def operators(plan):
    """Yield every operator of a plan tree."""
    yield plan
    for child in plan.get("children", []):
        yield from operators(child)


@pytest.fixture(scope="module")
def session():
    password = os.environ.get("LOGIC_TEST_NEO4J_PASSWORD", "logic-test-pw")
    user = os.environ.get("LOGIC_TEST_NEO4J_USER", "neo4j")
    wait_seconds = int(os.environ.get("LOGIC_TEST_NEO4J_WAIT", "120"))
    with neo4j.GraphDatabase.driver(NEO4J_URI, auth=(user, password)) as driver:
        deadline = time.monotonic() + wait_seconds
        while True:  # a container that just started refuses connections for a while
            try:
                driver.verify_connectivity()
                break
            except Exception:
                if time.monotonic() > deadline:
                    pytest.fail(f"Neo4j at {NEO4J_URI} did not become ready within {wait_seconds} seconds")
                time.sleep(3)
        with driver.session() as session:
            if session.run("MATCH (n) RETURN count(n) AS n").single()["n"] != 0:
                pytest.fail("refusing to run: the database is not empty; use a throwaway Neo4j (see the docstring)")
            session.run(SEED).consume()
            try:
                yield session
            finally:
                session.run("MATCH (n) DETACH DELETE n").consume()


@pytest.fixture(scope="module")
def row_limit(session):
    """Twice the number of nodes plus edges: a query that stays within it never multiplied rows."""
    nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
    edges = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    return 2 * (nodes + edges)


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_results_are_right(session, name):
    request, expected = SCENARIOS[name]
    result_query, _, _ = build_queries(request)

    found = entities_by_variable(session.run(result_query))

    assert {label: names_with_label(found, label) for label in expected} == expected


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_counts_agree_with_the_results(session, name):
    request, _ = SCENARIOS[name]
    result_query, total_query, label_query = build_queries(request)

    found = entities_by_variable(session.run(result_query))
    totals = session.run(total_query).single()
    by_label = session.run(label_query).single().data()

    # Each count-by-label key is "<variable>_<label>"; its value must be the
    # number of distinct entities that variable has in the result.
    for key, count in by_label.items():
        variable = key.split("_", 1)[0]
        assert count == len(found.get(variable, {})), key
    node_total = sum(len(e) for e in found.values() if all(isinstance(x, neo4j.graph.Node) for x in e.values()))
    edge_total = sum(len(e) for e in found.values() if all(isinstance(x, neo4j.graph.Relationship) for x in e.values()))
    assert (totals["total_nodes"], totals["total_edges"]) == (node_total, edge_total)


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_plans_have_no_cartesian_product(session, name):
    request, _ = SCENARIOS[name]

    for query in build_queries(request):
        plan = session.run("EXPLAIN " + query).consume().plan
        used = [op["operatorType"] for op in operators(plan)]
        assert not any("CartesianProduct" in op for op in used), (query, used)


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_no_step_multiplies_rows(session, row_limit, name):
    request, _ = SCENARIOS[name]

    for query in build_queries(request):
        result = session.run("PROFILE " + query)
        list(result)
        profile = result.consume().profile
        busiest = max(operators(profile), key=lambda op: op.get("rows", 0))
        assert busiest.get("rows", 0) <= row_limit, (query, busiest["operatorType"], busiest.get("rows"))


def test_a_flat_match_over_two_edges_does_multiply_rows(session, row_limit):
    # Proves the check above can fail: this is the shape the count queries had
    # before (one flat MATCH over both edges), and HUB alone gives 20 x 20 rows.
    query = (
        "MATCH (g:gene)-[tx:transcribed_to]->(t:transcript), (g:gene)-[pw:genes_pathways]->(p:pathway) "
        "RETURN count(DISTINCT g) + count(DISTINCT t) + count(DISTINCT p) AS total_nodes"
    )
    result = session.run("PROFILE " + query)
    list(result)

    busiest = max(operators(result.consume().profile), key=lambda op: op.get("rows", 0))

    assert busiest["rows"] > row_limit