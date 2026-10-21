"""Snapshot tests for CypherQueryGenerator.query_Generator.

Each case records the exact Cypher produced today for one request shape:
the result query, the total-count query and the count-by-label query.
They are a safety net: work on boolean 'logic' must not change the queries
built for requests without it. A failing case means the generated text
changed; if the change is intended, update the expected text in the same PR.

Intended changes for requests without logic (see the cases): the count
queries of the anchor path are built on the result query's CALL head instead
of one flat MATCH, so they never multiply rows; and every CALL arm starts
from the anchor.

The snapshots record current behavior, not necessarily correct behavior.
Whitespace is ignored when comparing; every other character must match.
"""
import copy
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import pytest

from app.lib.validator import LogicValidationError
from app.services.cypher_generator import CypherQueryGenerator

REPO_ROOT = Path(__file__).resolve().parents[2]


class Case(NamedTuple):
    request: dict
    options: dict      # extra keyword arguments for query_Generator (limit, node_only)
    expected: tuple    # (result query, total-count query, count-by-label query)


def node(node_id, node_type, properties=None, node_ref=""):
    """Return a request node; node_ref is the database id ('' means any node)."""
    return {"node_id": node_id, "id": node_ref, "type": node_type, "properties": properties or {}}


def predicate(predicate_id, predicate_type, source, target):
    """Return a request predicate (edge) from source to target."""
    return {"predicate_id": predicate_id, "type": predicate_type, "source": source, "target": target}


FTO = {"gene_name": "FTO"}
FTO_PROTEIN_CODING = {"gene_name": "FTO", "gene_type": "protein_coding"}
PROTEIN_CODING_TRANSCRIPT = {"transcript_type": "protein_coding"}

GENE_TRANSCRIPT_PATHWAY = {
    "nodes": [node("n0", "gene", FTO_PROTEIN_CODING), node("n1", "transcript"), node("n2", "pathway")],
    "predicates": [
        predicate("p0", "transcribed to", "n0", "n1"),
        predicate("p1", "genes pathways", "n0", "n2"),
    ],
}

GENE_TRANSCRIPT_PATHWAY_BY_NAME = {
    "nodes": [node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "pathway")],
    "predicates": [
        predicate("p0", "transcribed to", "n0", "n1"),
        predicate("p1", "genes pathways", "n0", "n2"),
    ],
}

GENE_PATHWAY_NO_EDGE = {"nodes": [node("n0", "gene", FTO), node("n1", "pathway")], "predicates": []}

ANCHOR_TWO_ARMS_HEAD = """
    MATCH (n0:gene)
    WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
    CALL (n0) {
      MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
      RETURN collect(DISTINCT {n1: n1, p0: p0}) AS p0
    }
    CALL (n0) {
      MATCH (n0)-[p1:genes_pathways]->(n2:pathway)
      RETURN collect(DISTINCT {n2: n2, p1: p1}) AS p1
    }
    WITH * WHERE size(p0) > 0 AND size(p1) > 0
"""

ANCHOR_TWO_ARMS_RESULT = ANCHOR_TWO_ARMS_HEAD + "RETURN n0, p0, p1"

# The count queries reuse the result query's head (same anchors, same arms),
# then turn each anchor row into one row per thing it found, so the rows add
# up over the arms instead of multiplying (no transcripts x pathways rows).
ANCHOR_TWO_ARMS_UNFOLD = """
    CALL (n0, p0, p1) {
      RETURN 'n0' AS var_name, n0 AS entity
      UNION ALL
      UNWIND p0 AS row RETURN 'n1' AS var_name, row.n1 AS entity
      UNION ALL
      UNWIND p0 AS row RETURN 'p0' AS var_name, row.p0 AS entity
      UNION ALL
      UNWIND p1 AS row RETURN 'n2' AS var_name, row.n2 AS entity
      UNION ALL
      UNWIND p1 AS row RETURN 'p1' AS var_name, row.p1 AS entity
    }
"""

ANCHOR_TWO_ARMS_COUNTS = (
    ANCHOR_TWO_ARMS_HEAD + ANCHOR_TWO_ARMS_UNFOLD + """
    RETURN (count(DISTINCT CASE WHEN var_name = 'n0' THEN entity END)
            + count(DISTINCT CASE WHEN var_name = 'n1' THEN entity END)
            + count(DISTINCT CASE WHEN var_name = 'n2' THEN entity END)) AS total_nodes,
           (count(DISTINCT CASE WHEN var_name = 'p0' THEN entity END)
            + count(DISTINCT CASE WHEN var_name = 'p1' THEN entity END)) AS total_edges
    """,
    ANCHOR_TWO_ARMS_HEAD + ANCHOR_TWO_ARMS_UNFOLD + """
    RETURN count(DISTINCT CASE WHEN var_name = 'n0' THEN entity END) AS n0_gene,
           count(DISTINCT CASE WHEN var_name = 'n1' THEN entity END) AS n1_transcript,
           count(DISTINCT CASE WHEN var_name = 'n2' THEN entity END) AS n2_pathway,
           count(DISTINCT CASE WHEN var_name = 'p0' THEN entity END) AS p0_transcribed_to,
           count(DISTINCT CASE WHEN var_name = 'p1' THEN entity END) AS p1_genes_pathways
    """,
)


def call_counts(result_query, arms, labels):
    """Return the expected (total-count, count-by-label) queries of the anchor path.

    Spells out the shape written in full in ANCHOR_TWO_ARMS_COUNTS: the result
    query up to its final RETURN, one UNION ALL branch per collected variable,
    then one count(DISTINCT ...) per variable.

    Args:
        result_query: The expected result query (its final RETURN is dropped).
        arms: [(alias, [variables])] in arm order; the first entry is the anchor
            alone, as (anchor, []).
        labels: [(variable, label)]: nodes in sorted order, then predicates in
            request order, as the count-by-label query lists them.
    """
    head = result_query.rsplit("RETURN", 1)[0]
    anchor = arms[0][0]
    branches = [f"RETURN '{anchor}' AS var_name, {anchor} AS entity"] + [
        f"UNWIND {alias} AS row RETURN '{var}' AS var_name, row.{var} AS entity"
        for alias, variables in arms[1:] for var in variables
    ]
    unfold = f"CALL ({', '.join(alias for alias, _ in arms)}) {{ " + " UNION ALL ".join(branches) + " }"

    def count(var):
        return f"count(DISTINCT CASE WHEN var_name = '{var}' THEN entity END)"

    nodes = [var for var, _ in labels if var.startswith("n")]
    edges = [var for var, _ in labels if var.startswith("p")]
    total = (f"RETURN ({' + '.join(map(count, nodes))}) AS total_nodes, "
             f"({' + '.join(map(count, edges))}) AS total_edges")
    by_label = "RETURN " + ", ".join(f"{count(var)} AS {var}_{label}" for var, label in labels)
    return (f"{head} {unfold} {total}", f"{head} {unfold} {by_label}")


def anchor_path(result_query, arms, labels):
    """Return the expected (result, total-count, count-by-label) queries of the anchor path."""
    return (result_query,) + call_counts(result_query, arms, labels)


GENE_PATHWAY_COUNTS = (
    """
    MATCH (n0:gene), (n1:pathway) WHERE n0.gene_name =~ '(?i)FTO'
    RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1)) AS total_nodes, (0) AS total_edges
    """,
    """
    MATCH (n0:gene), (n1:pathway) WHERE n0.gene_name =~ '(?i)FTO'
    RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_pathway
    """,
)

CASES = {
    # --- No predicates: plain MATCH of nodes -------------------------------
    "node-only-single-node": Case(
        request={
            "nodes": [node("n0", "gene", {"gene_name": "FTO", "gene_type": "protein_coding"})],
            "predicates": [],
        },
        options={},
        expected=(
            """
            MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN n0
            """,
            """
            MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN (COUNT(DISTINCT n0)) AS total_nodes, (0) AS total_edges
            """,
            """
            MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN COUNT(DISTINCT n0) AS n0_gene
            """,
        ),
    ),
    "node-only-two-nodes": Case(
        request=GENE_PATHWAY_NO_EDGE,
        options={},
        expected=(
            "MATCH (n0:gene), (n1:pathway) WHERE n0.gene_name =~ '(?i)FTO' RETURN n0, n1",
        ) + GENE_PATHWAY_COUNTS,
    ),
    "node-only-hypothesis": Case(
        request={"nodes": [node("n0", "gene", FTO_PROTEIN_CODING), node("n1", "pathway")], "predicates": []},
        options={"node_only": True},
        expected=(
            # Recorded as-is: this WHERE belongs to the last OPTIONAL MATCH only.
            """
            OPTIONAL MATCH (n0:gene) OPTIONAL MATCH (n1:pathway)
            WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN n0, n1
            """,
            """
            MATCH (n0:gene), (n1:pathway)
            WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1)) AS total_nodes, (0) AS total_edges
            """,
            """
            MATCH (n0:gene), (n1:pathway)
            WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
            RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_pathway
            """,
        ),
    ),
    "node-only-with-limit": Case(
        request={"nodes": [node("n0", "gene", FTO)], "predicates": []},
        options={"limit": 10},
        expected=(
            "MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' RETURN n0 LIMIT 10",
            """
            MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO'
            RETURN (COUNT(DISTINCT n0)) AS total_nodes, (0) AS total_edges
            """,
            "MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' RETURN COUNT(DISTINCT n0) AS n0_gene",
        ),
    ),
    # --- One predicate: fallback chain path ----------------------------------
    "one-predicate": Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript", PROTEIN_CODING_TRANSCRIPT)],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
        },
        options={},
        expected=(
            """
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name =~ '(?i)FTO' AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN p0, n0, n1
            """,
            """
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name =~ '(?i)FTO' AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1)) AS total_nodes, (COUNT(DISTINCT p0)) AS total_edges
            """,
            """
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name =~ '(?i)FTO' AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript,
                   COUNT(DISTINCT p0) AS p0_transcribed_to
            """,
        ),
    ),
    "one-predicate-node-with-id": Case(
        request={
            "nodes": [node("n0", "gene", node_ref="ENSG00000140718"), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
        },
        options={},
        expected=(
            "MATCH (n0:gene {id: 'ENSG00000140718'})-[p0:transcribed_to]->(n1:transcript) RETURN p0, n0, n1",
            """
            MATCH (n0:gene {id: 'ENSG00000140718'})-[p0:transcribed_to]->(n1:transcript)
            RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1)) AS total_nodes, (COUNT(DISTINCT p0)) AS total_edges
            """,
            """
            MATCH (n0:gene {id: 'ENSG00000140718'})-[p0:transcribed_to]->(n1:transcript)
            RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript,
                   COUNT(DISTINCT p0) AS p0_transcribed_to
            """,
        ),
    ),
    # --- Two or more predicates: anchor node + CALL subqueries ---------------
    "anchor-two-arms": Case(
        request=GENE_TRANSCRIPT_PATHWAY,
        options={},
        expected=(ANCHOR_TWO_ARMS_RESULT,) + ANCHOR_TWO_ARMS_COUNTS,
    ),
    "anchor-two-arms-with-limit": Case(
        request=GENE_TRANSCRIPT_PATHWAY,
        options={"limit": 10},
        expected=(ANCHOR_TWO_ARMS_RESULT + " LIMIT 10",) + ANCHOR_TWO_ARMS_COUNTS,
    ),
    "anchor-chained-arm": Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "protein"), node("n3", "pathway")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "translates to", "n1", "n2"),
                predicate("p2", "genes pathways", "n0", "n3"),
            ],
        },
        options={},
        expected=anchor_path(
            """
            MATCH (n0:gene)
            WHERE n0.gene_name =~ '(?i)FTO'
            CALL (n0) {
              MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
              MATCH (n1)-[p1:translates_to]->(n2:protein)
              RETURN collect(DISTINCT {n1: n1, p0: p0, n2: n2, p1: p1}) AS p0_p1
            }
            CALL (n0) {
              MATCH (n0)-[p2:genes_pathways]->(n3:pathway)
              RETURN collect(DISTINCT {n3: n3, p2: p2}) AS p2
            }
            WITH * WHERE size(p0_p1) > 0 AND size(p2) > 0
            RETURN n0, p0_p1, p2
            """,
            arms=[('n0', []), ('p0_p1', ['n1', 'p0', 'n2', 'p1']), ('p2', ['n3', 'p2'])],
            labels=[('n0', 'gene'), ('n1', 'transcript'), ('n2', 'protein'), ('n3', 'pathway'), ('p0', 'transcribed_to'), ('p1', 'translates_to'), ('p2', 'genes_pathways')],
        ),
    ),
    "anchor-arm-starts-at-the-anchor": Case(
        # transcript -> protein is listed first, but its arm still starts from the
        # gene, so no MATCH in the arm scans every edge of one type.
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "protein"), node("n3", "pathway")],
            "predicates": [
                predicate("p1", "translates to", "n1", "n2"),
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p2", "genes pathways", "n0", "n3"),
            ],
        },
        options={},
        expected=anchor_path(
            """
            MATCH (n0:gene)
            WHERE n0.gene_name =~ '(?i)FTO'
            CALL (n0) {
              MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
              MATCH (n1)-[p1:translates_to]->(n2:protein)
              RETURN collect(DISTINCT {n1: n1, p0: p0, n2: n2, p1: p1}) AS p0_p1
            }
            CALL (n0) {
              MATCH (n0)-[p2:genes_pathways]->(n3:pathway)
              RETURN collect(DISTINCT {n3: n3, p2: p2}) AS p2
            }
            WITH * WHERE size(p0_p1) > 0 AND size(p2) > 0
            RETURN n0, p0_p1, p2
            """,
            arms=[("n0", []), ("p0_p1", ["n1", "p0", "n2", "p1"]), ("p2", ["n3", "p2"])],
            labels=[("n0", "gene"), ("n1", "transcript"), ("n2", "protein"), ("n3", "pathway"),
                    ("p1", "translates_to"), ("p0", "transcribed_to"), ("p2", "genes_pathways")],
        ),
    ),
    "anchor-with-overlap-keeps-flat-counts": Case(
        # 'overlaps with' is computed from positions, not stored, so its counts
        # keep the flat MATCH (unchanged from before logic support).
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "enhancer")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "overlaps with", "n0", "n2"),
            ],
        },
        options={},
        expected=(
            """
            MATCH (n0:gene)
            WHERE n0.gene_name =~ '(?i)FTO'
            CALL (n0) {
              MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
              RETURN collect(DISTINCT {n1: n1, p0: p0}) AS p0
            }
            CALL (n0) {
              MATCH (n0)-[p1:overlaps_with]->(n2:enhancer)
              RETURN collect(DISTINCT {n2: n2, p1: p1}) AS p1
            }
            WITH * WHERE size(p0) > 0 AND size(p1) > 0
            RETURN n0, p0, p1
            """,
        ) + tuple(
            "MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript), (n0:gene), (n2:enhancer) "
            "WHERE n0.gene_name =~ '(?i)FTO' AND n0.chr = n2.chr AND toInteger(n0.start) < toInteger(n2.end) "
            "AND toInteger(n2.start) < toInteger(n0.end) "
            "WITH *, apoc.create.vRelationship(n0, 'overlaps_with', {source:'virtual'}, n2) AS p1 "
            + ret
            for ret in (
                "RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1) + COUNT(DISTINCT n2)) AS total_nodes, "
                "(COUNT(DISTINCT p0) + COUNT(DISTINCT p1)) AS total_edges",
                "RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript, "
                "COUNT(DISTINCT n2) AS n2_enhancer, COUNT(DISTINCT p0) AS p0_transcribed_to, "
                "COUNT(DISTINCT p1) AS p1_overlaps_with",
            )
        ),
    ),
    # --- Comma-separated values: list path -----------------------------------
    "list-query": Case(
        request={
            "nodes": [node("n0", "gene", {"gene_name": "FTO,BRCA1"}), node("n1", "transcript", PROTEIN_CODING_TRANSCRIPT)],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
        },
        options={},
        expected=(
            """
            WITH ['FTO', 'BRCA1'] AS listA
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name IN listA AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN p0, n0, n1
            """,
            """
            WITH ['FTO', 'BRCA1'] AS listA
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name IN listA AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN COUNT(DISTINCT n0) + COUNT(DISTINCT n1) AS total_nodes, COUNT(DISTINCT p0) AS total_edges
            """,
            """
            WITH ['FTO', 'BRCA1'] AS listA
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript)
            WHERE n0.gene_name IN listA AND n1.transcript_type =~ '(?i)protein_coding'
            RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript,
                   COUNT(DISTINCT p0) AS p0_transcribed_to
            """,
        ),
    ),
}


def one_line(text):
    """Collapse all whitespace, so layout differences do not fail a snapshot."""
    return " ".join(text.split())


@pytest.fixture
def generator():
    # Skip __init__, which opens Neo4j connections; building query text needs none.
    return CypherQueryGenerator.__new__(CypherQueryGenerator)


def generate(generator, case):
    """Run query_Generator on a copy of the case's request, as process_query would."""
    request = copy.deepcopy(case.request)
    node_map = {n["node_id"]: n for n in request["nodes"]}
    return generator.query_Generator(request, node_map, **case.options)


@pytest.mark.parametrize("name", list(CASES))
def test_generated_queries_match_snapshot(generator, name):
    case = CASES[name]

    queries = generate(generator, case)

    assert [one_line(q) for q in queries] == [one_line(q) for q in case.expected]


def test_count_helper_matches_the_counts_written_in_full():
    # call_counts spells out the same shape as ANCHOR_TWO_ARMS_COUNTS; if they
    # drift apart, the snapshots built with the helper prove nothing.
    built = call_counts(
        ANCHOR_TWO_ARMS_RESULT,
        arms=[("n0", []), ("p0", ["n1", "p0"]), ("p1", ["n2", "p1"])],
        labels=[("n0", "gene"), ("n1", "transcript"), ("n2", "pathway"),
                ("p0", "transcribed_to"), ("p1", "genes_pathways")],
    )

    assert [one_line(q) for q in built] == [one_line(q) for q in ANCHOR_TWO_ARMS_COUNTS]


def test_anchor_path_counts_have_no_flat_match_over_several_edges(generator):
    # A flat MATCH over several edges makes one row per combination before
    # counting. Every anchor-path count query must start with the CALL head.
    for name in ("anchor-two-arms", "anchor-chained-arm", "anchor-arm-starts-at-the-anchor"):
        result_query, *count_queries = generate(generator, CASES[name])
        head = one_line(result_query).rsplit(" RETURN ", 1)[0]
        for query in count_queries:
            assert one_line(query).startswith(head), name


def test_generated_text_does_not_depend_on_hash_seed():
    # Python randomizes set order per process (PYTHONHASHSEED), so this runs the
    # generator in separate processes and checks every run writes the same text.
    script = (
        "from app.services.cypher_generator import CypherQueryGenerator\n"
        "g = CypherQueryGenerator.__new__(CypherQueryGenerator)\n"
        "r = {'nodes': [{'node_id': 'n0', 'id': '', 'type': 'gene', 'properties': {'gene_name': 'FTO'}},\n"
        "               {'node_id': 'n1', 'id': '', 'type': 'transcript', 'properties': {}}],\n"
        "     'predicates': [{'predicate_id': 'p0', 'type': 'transcribed to', 'source': 'n0', 'target': 'n1'}]}\n"
        "query = g.query_Generator(r, {n['node_id']: n for n in r['nodes']})[0]\n"
        "print('QUERY:' + ' '.join(query.split()))\n"
    )
    outputs = set()
    for seed in ("1", "2", "3", "4"):
        env = {**os.environ, "PYTHONHASHSEED": seed, "PYTHONPATH": str(REPO_ROOT)}
        completed = subprocess.run(
            [sys.executable, "-c", script], cwd=REPO_ROOT, env=env,
            capture_output=True, text=True, check=True,
        )
        # Libraries may log to stdout too, so pick out the marked line.
        outputs.add(next(line for line in completed.stdout.splitlines() if line.startswith("QUERY:")))

    assert len(outputs) == 1, outputs


# --- Requests with boolean 'logic' on node properties -------------------------
#
# Logic is written in its validated, canonical form (upper-case operators,
# values as lists), because query_Generator always receives it that way.

def logic_group(operator, node_id, properties):
    """Return a property group: conditions on one node's properties."""
    return {"operator": operator, "nodes": {"node_id": node_id, "properties": properties}}


def logic_tree(*children, operator="AND"):
    """Return a whole 'logic' section with the given top-level groups."""
    return {"operator": operator, "children": list(children)}


def gene_only(where):
    """Expected queries for a request with one gene node and no predicates."""
    return (
        f"MATCH (n0:gene) WHERE {where} RETURN n0",
        f"MATCH (n0:gene) WHERE {where} RETURN (COUNT(DISTINCT n0)) AS total_nodes, (0) AS total_edges",
        f"MATCH (n0:gene) WHERE {where} RETURN COUNT(DISTINCT n0) AS n0_gene",
    )


def gene_to_transcript(gene, where):
    """Expected queries for gene -> transcribed to -> transcript (simple chain path)."""
    match = f"MATCH {gene}-[p0:transcribed_to]->(n1:transcript) WHERE {where}"
    return (
        f"{match} RETURN p0, n0, n1",
        f"{match} RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1)) AS total_nodes, (COUNT(DISTINCT p0)) AS total_edges",
        f"{match} RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript, "
        "COUNT(DISTINCT p0) AS p0_transcribed_to",
    )


GENE_NAME_OR_TYPE = "(n0.gene_name =~ '(?i)FTO' OR n0.gene_type =~ '(?i)protein_coding')"
BRCA1_OR_FTO = "(n0.gene_name =~ '(?i)BRCA1' OR n0.gene_name =~ '(?i)FTO')"
NOT_RETAINED_INTRON = "NOT coalesce({var}.transcript_type =~ '(?i)retained_intron', false)"

LOGIC_CASES = {
    # --- No predicates --------------------------------------------------------
    "logic-or-is-the-only-filter": Case(
        # Regression: a node whose only filter is in logic used to lose it.
        # Properties are given out of order; the query lists them alphabetically.
        request={
            "nodes": [node("n0", "gene")],
            "predicates": [],
            "logic": logic_tree(logic_group("OR", "n0", {"gene_type": ["protein_coding"], "gene_name": ["FTO"]})),
        },
        options={},
        expected=gene_only(GENE_NAME_OR_TYPE),
    ),
    "logic-not-next-to-own-filter": Case(
        request={
            "nodes": [node("n0", "gene", FTO)],
            "predicates": [],
            "logic": logic_tree(logic_group("NOT", "n0", {"gene_type": ["protein_coding"]})),
        },
        options={},
        expected=gene_only(
            "n0.gene_name =~ '(?i)FTO' AND NOT coalesce(n0.gene_type =~ '(?i)protein_coding', false)"
        ),
    ),
    "logic-not-several-values": Case(
        # NOT means "none of them": the values are ORed inside the NOT.
        request={
            "nodes": [node("n0", "gene", FTO)],
            "predicates": [],
            "logic": logic_tree(logic_group("NOT", "n0", {"gene_type": ["lncRNA", "pseudogene"]})),
        },
        options={},
        expected=gene_only(
            "n0.gene_name =~ '(?i)FTO' AND "
            "NOT coalesce((n0.gene_type =~ '(?i)lncRNA' OR n0.gene_type =~ '(?i)pseudogene'), false)"
        ),
    ),
    "logic-nested-groups": Case(
        request={
            "nodes": [node("n0", "gene")],
            "predicates": [],
            "logic": logic_tree({"operator": "OR", "children": [
                logic_group("AND", "n0", {"gene_name": ["BRCA1"]}),
                logic_group("AND", "n0", {"gene_name": ["FTO"], "gene_type": ["protein_coding"]}),
            ]}),
        },
        options={},
        expected=gene_only(
            "(n0.gene_name =~ '(?i)BRCA1' OR (n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'))"
        ),
    ),
    "logic-or-root": Case(
        request={
            "nodes": [node("n0", "gene")],
            "predicates": [],
            "logic": logic_tree(
                logic_group("AND", "n0", {"gene_name": ["BRCA1"]}),
                logic_group("AND", "n0", {"gene_name": ["FTO"]}),
                operator="OR",
            ),
        },
        options={},
        expected=gene_only(BRCA1_OR_FTO),
    ),
    "logic-values-are-escaped": Case(
        # Same escaping as normal filters: '*' is escaped and quotes are removed.
        request={
            "nodes": [node("n0", "gene")],
            "predicates": [],
            "logic": logic_tree(logic_group("OR", "n0", {"gene_name": ["HLA-A*02", "it's"]})),
        },
        options={},
        expected=gene_only("(n0.gene_name =~ '(?i)HLA-A\\*02' OR n0.gene_name =~ '(?i)its')"),
    ),
    # --- One predicate: simple chain path ---------------------------------------
    "logic-one-predicate": Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
            "logic": logic_tree(logic_group("OR", "n1", {"transcript_type": ["lncRNA", "protein_coding"]})),
        },
        options={},
        expected=gene_to_transcript(
            "(n0:gene)",
            "n0.gene_name =~ '(?i)FTO' AND "
            "(n1.transcript_type =~ '(?i)lncRNA' OR n1.transcript_type =~ '(?i)protein_coding')",
        ),
    ),
    "logic-on-node-pinned-by-id": Case(
        # A node matched by id has no other WHERE condition, but its logic still applies.
        request={
            "nodes": [node("n0", "gene", node_ref="ENSG00000140718"), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
            "logic": logic_tree(logic_group("NOT", "n0", {"gene_type": ["lncRNA"]})),
        },
        options={},
        expected=gene_to_transcript(
            "(n0:gene {id: 'ENSG00000140718'})",
            "NOT coalesce(n0.gene_type =~ '(?i)lncRNA', false)",
        ),
    ),
    # --- Anchor + CALL path -------------------------------------------------------
    "logic-anchor-two-arms": Case(
        # The transcript's condition goes inside the transcript arm's CALL block.
        request={
            "nodes": [node("n0", "gene"), node("n1", "pathway"), node("n2", "transcript")],
            "predicates": [
                predicate("p0", "genes pathways", "n0", "n1"),
                predicate("p1", "transcribed to", "n0", "n2"),
            ],
            "logic": logic_tree(
                logic_group("OR", "n0", {"gene_name": ["BRCA1", "FTO"]}),
                logic_group("NOT", "n2", {"transcript_type": ["retained_intron"]}),
            ),
        },
        options={},
        expected=anchor_path(
            f"""
            MATCH (n0:gene)
            WHERE {BRCA1_OR_FTO}
            CALL (n0) {{
              MATCH (n0)-[p0:genes_pathways]->(n1:pathway)
              RETURN collect(DISTINCT {{n1: n1, p0: p0}}) AS p0
            }}
            CALL (n0) {{
              MATCH (n0)-[p1:transcribed_to]->(n2:transcript)
              WHERE {NOT_RETAINED_INTRON.format(var="n2")}
              RETURN collect(DISTINCT {{n2: n2, p1: p1}}) AS p1
            }}
            WITH * WHERE size(p0) > 0 AND size(p1) > 0
            RETURN n0, p0, p1
            """,
            arms=[('n0', []), ('p0', ['n1', 'p0']), ('p1', ['n2', 'p1'])],
            labels=[('n0', 'gene'), ('n1', 'pathway'), ('n2', 'transcript'), ('p0', 'genes_pathways'), ('p1', 'transcribed_to')],
        ),
    ),
    "logic-filter-picks-the-anchor": Case(
        # gene -> transcript -> protein. Without logic the transcript (2 edges) is
        # the anchor; a node filtered only by logic counts as filtered, so the gene is.
        request={
            "nodes": [node("n0", "gene"), node("n1", "transcript"), node("n2", "protein")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "translates to", "n1", "n2"),
            ],
            "logic": logic_tree(logic_group("OR", "n0", {"gene_name": ["BRCA1", "FTO"]})),
        },
        options={},
        expected=anchor_path(
            f"""
            MATCH (n0:gene)
            WHERE {BRCA1_OR_FTO}
            CALL (n0) {{
              MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
              MATCH (n1)-[p1:translates_to]->(n2:protein)
              RETURN collect(DISTINCT {{n1: n1, p0: p0, n2: n2, p1: p1}}) AS p0_p1
            }}
            WITH * WHERE size(p0_p1) > 0
            RETURN n0, p0_p1
            """,
            arms=[('n0', []), ('p0_p1', ['n1', 'p0', 'n2', 'p1'])],
            labels=[('n0', 'gene'), ('n1', 'transcript'), ('n2', 'protein'), ('p0', 'transcribed_to'), ('p1', 'translates_to')],
        ),
    ),
    # --- List path -------------------------------------------------------------------
    "logic-list-query": Case(
        request={
            "nodes": [node("n0", "gene", {"gene_name": "FTO,BRCA1"}), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
            "logic": logic_tree(logic_group("NOT", "n1", {"transcript_type": ["retained_intron"]})),
        },
        options={},
        expected=tuple(
            "WITH ['FTO', 'BRCA1'] AS listA "
            "MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript) "
            f"WHERE n0.gene_name IN listA AND {NOT_RETAINED_INTRON.format(var='n1')} {ret}"
            for ret in (
                "RETURN p0, n0, n1",
                "RETURN COUNT(DISTINCT n0) + COUNT(DISTINCT n1) AS total_nodes, COUNT(DISTINCT p0) AS total_edges",
                "RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript, "
                "COUNT(DISTINCT p0) AS p0_transcribed_to",
            )
        ),
    ),
}


@pytest.mark.parametrize("name", list(LOGIC_CASES))
def test_logic_queries_match_snapshot(generator, name):
    case = LOGIC_CASES[name]

    queries = generate(generator, case)

    assert [one_line(q) for q in queries] == [one_line(q) for q in case.expected]


def test_logic_of_one_request_does_not_leak_into_the_next(generator):
    # One generator instance serves every request, so logic must travel with the
    # request, never be remembered on the generator.
    generate(generator, LOGIC_CASES["logic-not-next-to-own-filter"])

    queries = generate(generator, CASES["node-only-with-limit"])

    assert [one_line(q) for q in queries] == [one_line(q) for q in CASES["node-only-with-limit"].expected]


def test_or_between_predicates_with_list_values_is_rejected(generator):
    # The list path has no CALL arms to OR; logic OR groups can replace the commas.
    case = Case(
        request={
            "nodes": [node("n0", "gene", {"gene_name": "FTO,BRCA1"}), node("n1", "transcript"), node("n2", "pathway")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=(),
    )

    with pytest.raises(LogicValidationError, match=re.escape(
        "not supported yet by the Cypher backend: OR between predicates together with "
        "comma-separated values; use an OR group in 'logic' for those values instead"
    )):
        generate(generator, case)


def test_or_groups_on_different_nodes_are_rejected(generator):
    # Each OR group needs its kept node to be the CALL anchor, and there is only one anchor.
    case = Case(
        request={
            "nodes": [
                node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "pathway"),
                node("n3", "transcript"), node("n4", "protein"), node("n5", "exon"),
            ],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
                predicate("p2", "transcribed to", "n0", "n3"),
                predicate("p3", "translates to", "n3", "n4"),
                predicate("p4", "includes", "n3", "n5"),
            ],
            "logic": logic_tree(
                {"operator": "OR", "predicates": ["p0", "p1"]},
                {"operator": "OR", "predicates": ["p3", "p4"]},
            ),
        },
        options={},
        expected=(),
    )

    with pytest.raises(LogicValidationError, match=re.escape(
        "not supported yet by the Cypher backend: OR groups on different nodes ['n0', 'n3']"
    )):
        generate(generator, case)


def test_arm_size_checks_find_the_or_predicate_anywhere_in_an_arm():
    # Defensive: arms start from the anchor today, but the OR'd predicate need not come first.
    arms = [
        [predicate("p2", "translates to", "n1", "n3"), predicate("p0", "transcribed to", "n0", "n1")],
        [predicate("p1", "genes pathways", "n0", "n2")],
    ]

    checks = CypherQueryGenerator._arm_size_checks(arms, ["p2_p0", "p1"], [["p0", "p1"]])

    assert checks == "(size(p2_p0) > 0 OR size(p1) > 0)"


def test_or_together_with_overlaps_with_is_rejected(generator):
    # 'overlaps with' keeps the flat count query, which can only mean AND.
    case = Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "enhancer")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "overlaps with", "n0", "n2"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=(),
    )

    with pytest.raises(LogicValidationError, match=re.escape(
        "not supported yet by the Cypher backend: OR between predicates together with 'overlaps with'"
    )):
        generate(generator, case)


# --- Requests with OR between predicates (edges) --------------------------------
#
# The OR group's shared node is the CALL anchor, each OR'd predicate is its own
# arm, and the arms' size checks are joined with OR. Predicates that continue
# from an OR'd predicate's far node are ANDed inside that predicate's arm.

TRANSCRIPT_OR_PATHWAY_ARMS = """
    CALL (n0) {{
      MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
      RETURN collect(DISTINCT {{n1: n1, p0: p0}}) AS p0
    }}
    CALL (n0) {{
      MATCH (n0)-[p1:genes_pathways]->(n2:pathway){pathway_where}
      RETURN collect(DISTINCT {{n2: n2, p1: p1}}) AS p1
    }}
"""

THREE_NODE_LABELS = [
    ("n0", "gene"), ("n1", "transcript"), ("n2", "pathway"), ("p0", "transcribed_to"), ("p1", "genes_pathways"),
]


def or_edges_expected(gene_where, pathway_where=""):
    """Expected queries for gene n0 with OR [p0 -> transcript n1, p1 -> pathway n2]."""
    where = f"WHERE {gene_where}" if gene_where else ""
    arms = TRANSCRIPT_OR_PATHWAY_ARMS.format(pathway_where=pathway_where)
    return anchor_path(
        f"MATCH (n0:gene) {where} {arms} WITH * WHERE (size(p0) > 0 OR size(p1) > 0) RETURN n0, p0, p1",
        arms=[("n0", []), ("p0", ["n1", "p0"]), ("p1", ["n2", "p1"])],
        labels=THREE_NODE_LABELS,
    )


# gene n0: OR [p0 -> transcript n1, p1 -> pathway n2], and transcript n1 -> protein n3 (p2).
TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_NODES = [
    node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "pathway"), node("n3", "protein"),
]
TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_LABELS = [
    ("n0", "gene"), ("n1", "transcript"), ("n2", "pathway"), ("n3", "protein"),
    ("p0", "transcribed_to"), ("p1", "genes_pathways"), ("p2", "translates_to"),
]

TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_RESULT = """
    MATCH (n0:gene)
    WHERE n0.gene_name =~ '(?i)FTO'
    CALL (n0) {
      MATCH (n0)-[p0:transcribed_to]->(n1:transcript)
      MATCH (n1)-[p2:translates_to]->(n3:protein)
      RETURN collect(DISTINCT {n1: n1, p0: p0, n3: n3, p2: p2}) AS p0_p2
    }
    CALL (n0) {
      MATCH (n0)-[p1:genes_pathways]->(n2:pathway)
      RETURN collect(DISTINCT {n2: n2, p1: p1}) AS p1
    }
    WITH * WHERE (size(p0_p2) > 0 OR size(p1) > 0)
    RETURN n0, p0_p2, p1
"""

OR_EDGE_CASES = {
    "or-edges-only": Case(
        request={**GENE_TRANSCRIPT_PATHWAY_BY_NAME, "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]})},
        options={},
        expected=or_edges_expected("n0.gene_name =~ '(?i)FTO'"),
    ),
    "or-kept-node-is-anchor-despite-far-filter": Case(
        # Without OR, the filtered pathway would become the anchor and both edges
        # would share one arm; the OR group's shared gene must be the anchor.
        request={
            "nodes": [node("n0", "gene"), node("n1", "transcript"), node("n2", "pathway", {"pathway_name": "Insulin signaling"})],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=or_edges_expected("", " WHERE n2.pathway_name =~ '(?i)Insulin signaling'"),
    ),
    "or-next-to-a-required-chain": Case(
        request={
            "nodes": [
                node("n0", "gene", FTO), node("n1", "transcript"), node("n2", "pathway"),
                node("n3", "transcript"), node("n4", "protein"),
            ],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
                predicate("p2", "transcribed to", "n0", "n3"),
                predicate("p3", "translates to", "n3", "n4"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=anchor_path(
            "MATCH (n0:gene) WHERE n0.gene_name =~ '(?i)FTO' "
            + TRANSCRIPT_OR_PATHWAY_ARMS.format(pathway_where="")
            + """
            CALL (n0) {
              MATCH (n0)-[p2:transcribed_to]->(n3:transcript)
              MATCH (n3)-[p3:translates_to]->(n4:protein)
              RETURN collect(DISTINCT {n3: n3, p2: p2, n4: n4, p3: p3}) AS p2_p3
            }
            WITH * WHERE (size(p0) > 0 OR size(p1) > 0) AND size(p2_p3) > 0
            RETURN n0, p0, p1, p2_p3
            """,
            arms=[("n0", []), ("p0", ["n1", "p0"]), ("p1", ["n2", "p1"]), ("p2_p3", ["n3", "p2", "n4", "p3"])],
            labels=[
                ("n0", "gene"), ("n1", "transcript"), ("n2", "pathway"), ("n3", "transcript"), ("n4", "protein"),
                ("p0", "transcribed_to"), ("p1", "genes_pathways"), ("p2", "transcribed_to"),
                ("p3", "translates_to"),
            ],
        ),
    ),
    "or-alternative-with-an-anded-edge": Case(
        # "FTO has a transcript that makes a protein, OR a pathway": p2 continues
        # from p0's far node, so it is ANDed inside p0's arm.
        request={
            "nodes": TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_NODES,
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
                predicate("p2", "translates to", "n1", "n3"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=anchor_path(
            TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_RESULT,
            arms=[("n0", []), ("p0_p2", ["n1", "p0", "n3", "p2"]), ("p1", ["n2", "p1"])],
            labels=TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_LABELS,
        ),
    ),
    "or-alternative-with-the-anded-edge-listed-first": Case(
        # The same question with p2 listed first. Arms start from the anchor, so
        # the result query is the same, and p0_p2 is still p0's alternative (OR).
        request={
            "nodes": TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_NODES,
            "predicates": [
                predicate("p2", "translates to", "n1", "n3"),
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
            ],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0", "p1"]}),
        },
        options={},
        expected=anchor_path(
            TRANSCRIPT_WITH_PROTEIN_OR_PATHWAY_RESULT,
            arms=[("n0", []), ("p0_p2", ["n1", "p0", "n3", "p2"]), ("p1", ["n2", "p1"])],
            labels=[
                ("n0", "gene"), ("n1", "transcript"), ("n2", "pathway"), ("n3", "protein"),
                ("p2", "translates_to"), ("p0", "transcribed_to"), ("p1", "genes_pathways"),
            ],
        ),
    ),
    "or-group-of-one-changes-nothing": Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
            "logic": logic_tree({"operator": "OR", "predicates": ["p0"]}),
        },
        options={},
        expected=gene_to_transcript("(n0:gene)", "n0.gene_name =~ '(?i)FTO'"),
    ),
}


@pytest.mark.parametrize("name", list(OR_EDGE_CASES))
def test_or_edge_queries_match_snapshot(generator, name):
    case = OR_EDGE_CASES[name]

    queries = generate(generator, case)

    assert [one_line(q) for q in queries] == [one_line(q) for q in case.expected]


# --- Requests with NOT on a predicate (edge) -----------------------------------
#
# A negated predicate and its far node leave normal matching; the kept node gets
# NOT EXISTS { MATCH ... } instead, with the far node's own filters inside.

def negated(*predicate_ids):
    """Return a predicate group that negates the given predicates."""
    return {"operator": "NOT", "predicates": list(predicate_ids)}


NO_PATHWAY = "NOT EXISTS {{ MATCH (n0)-[:genes_pathways]->({far}:pathway) }}"

NOT_EDGE_CASES = {
    "not-edge-whole-pattern": Case(
        # "Genes with no pathway": nothing else is left to match, so it is a node-only query.
        request={
            "nodes": [node("n0", "gene"), node("n1", "pathway")],
            "predicates": [predicate("p0", "genes pathways", "n0", "n1")],
            "logic": logic_tree(negated("p0")),
        },
        options={},
        expected=gene_only(NO_PATHWAY.format(far="n1")),
    ),
    "not-edge-simple-chain": Case(
        request={**GENE_TRANSCRIPT_PATHWAY_BY_NAME, "logic": logic_tree(negated("p1"))},
        options={},
        expected=gene_to_transcript("(n0:gene)", f"n0.gene_name =~ '(?i)FTO' AND {NO_PATHWAY.format(far='n2')}"),
    ),
    "not-edge-far-node-filters": Case(
        # The far node's own properties and logic go inside the braces.
        request={
            "nodes": [
                node("n0", "gene", FTO), node("n1", "transcript"),
                node("n2", "pathway", {"pathway_name": "Insulin signaling"}),
            ],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
            ],
            "logic": logic_tree(negated("p1"), logic_group("NOT", "n2", {"source": ["KEGG"]})),
        },
        options={},
        expected=gene_to_transcript(
            "(n0:gene)",
            "n0.gene_name =~ '(?i)FTO' AND NOT EXISTS { MATCH (n0)-[:genes_pathways]->(n2:pathway) "
            "WHERE n2.pathway_name =~ '(?i)Insulin signaling' AND NOT coalesce(n2.source =~ '(?i)KEGG', false) }",
        ),
    ),
    "not-edge-kept-node-is-target": Case(
        # gene -> transcript -> protein, NOT on the first edge: the transcript is kept,
        # so the far gene is written on the left of the arrow.
        request={
            "nodes": [node("n0", "gene"), node("n1", "transcript"), node("n2", "protein")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "translates to", "n1", "n2"),
            ],
            "logic": logic_tree(negated("p0")),
        },
        options={},
        expected=tuple(
            "MATCH (n1:transcript)-[p1:translates_to]->(n2:protein) "
            f"WHERE NOT EXISTS {{ MATCH (n0:gene)-[:transcribed_to]->(n1) }} {ret}"
            for ret in (
                "RETURN p1, n1, n2",
                "RETURN (COUNT(DISTINCT n1) + COUNT(DISTINCT n2)) AS total_nodes, (COUNT(DISTINCT p1)) AS total_edges",
                "RETURN COUNT(DISTINCT n1) AS n1_transcript, COUNT(DISTINCT n2) AS n2_protein, "
                "COUNT(DISTINCT p1) AS p1_translates_to",
            )
        ),
    ),
    "not-edge-inside-anchor-arm": Case(
        # The condition belongs to the transcript, so it goes inside the transcript arm.
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "pathway"), node("n2", "transcript"), node("n3", "protein")],
            "predicates": [
                predicate("p0", "genes pathways", "n0", "n1"),
                predicate("p1", "transcribed to", "n0", "n2"),
                predicate("p2", "translates to", "n2", "n3"),
            ],
            "logic": logic_tree(negated("p2")),
        },
        options={},
        expected=anchor_path(
            """
            MATCH (n0:gene)
            WHERE n0.gene_name =~ '(?i)FTO'
            CALL (n0) {
              MATCH (n0)-[p0:genes_pathways]->(n1:pathway)
              RETURN collect(DISTINCT {n1: n1, p0: p0}) AS p0
            }
            CALL (n0) {
              MATCH (n0)-[p1:transcribed_to]->(n2:transcript)
              WHERE NOT EXISTS { MATCH (n2)-[:translates_to]->(n3:protein) }
              RETURN collect(DISTINCT {n2: n2, p1: p1}) AS p1
            }
            WITH * WHERE size(p0) > 0 AND size(p1) > 0
            RETURN n0, p0, p1
            """,
            arms=[('n0', []), ('p0', ['n1', 'p0']), ('p1', ['n2', 'p1'])],
            labels=[('n0', 'gene'), ('n1', 'pathway'), ('n2', 'transcript'), ('p0', 'genes_pathways'), ('p1', 'transcribed_to')],
        ),
    ),
    "not-two-edges-with-property-group": Case(
        # NOT [a, b] means "none of them": one NOT EXISTS per edge, joined with AND.
        request={
            "nodes": [node("n0", "gene"), node("n1", "pathway"), node("n2", "transcript")],
            "predicates": [
                predicate("p0", "genes pathways", "n0", "n1"),
                predicate("p1", "transcribed to", "n0", "n2"),
            ],
            "logic": logic_tree(negated("p0", "p1"), logic_group("OR", "n0", {"gene_name": ["BRCA1", "FTO"]})),
        },
        options={},
        expected=gene_only(
            f"{BRCA1_OR_FTO} AND {NO_PATHWAY.format(far='n1')} "
            "AND NOT EXISTS { MATCH (n0)-[:transcribed_to]->(n2:transcript) }"
        ),
    ),
    "not-edge-list-query": Case(
        request={
            "nodes": [node("n0", "gene", {"gene_name": "FTO,BRCA1"}), node("n1", "transcript"), node("n2", "pathway")],
            "predicates": [
                predicate("p0", "transcribed to", "n0", "n1"),
                predicate("p1", "genes pathways", "n0", "n2"),
            ],
            "logic": logic_tree(negated("p1")),
        },
        options={},
        expected=tuple(
            "WITH ['FTO', 'BRCA1'] AS listA "
            "MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript) "
            f"WHERE n0.gene_name IN listA AND {NO_PATHWAY.format(far='n2')} {ret}"
            for ret in (
                "RETURN p0, n0, n1",
                "RETURN COUNT(DISTINCT n0) + COUNT(DISTINCT n1) AS total_nodes, COUNT(DISTINCT p0) AS total_edges",
                "RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript, "
                "COUNT(DISTINCT p0) AS p0_transcribed_to",
            )
        ),
    ),
    "not-edge-kept-node-pinned-by-id": Case(
        request={
            "nodes": [node("n0", "gene", node_ref="ENSG00000140718"), node("n1", "pathway")],
            "predicates": [predicate("p0", "genes pathways", "n0", "n1")],
            "logic": logic_tree(negated("p0")),
        },
        options={},
        expected=tuple(
            f"MATCH (n0:gene {{id: 'ENSG00000140718'}}) WHERE {NO_PATHWAY.format(far='n1')} {ret}"
            for ret in (
                "RETURN n0",
                "RETURN (COUNT(DISTINCT n0)) AS total_nodes, (0) AS total_edges",
                "RETURN COUNT(DISTINCT n0) AS n0_gene",
            )
        ),
    ),
    "and-predicate-group-changes-nothing": Case(
        # AND on a predicate means "it must exist", which every predicate already means.
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "transcript")],
            "predicates": [predicate("p0", "transcribed to", "n0", "n1")],
            "logic": logic_tree({"operator": "AND", "predicates": ["p0"]}),
        },
        options={},
        expected=gene_to_transcript("(n0:gene)", "n0.gene_name =~ '(?i)FTO'"),
    ),
}


@pytest.mark.parametrize("name", list(NOT_EDGE_CASES))
def test_not_edge_queries_match_snapshot(generator, name):
    case = NOT_EDGE_CASES[name]

    queries = generate(generator, case)

    assert [one_line(q) for q in queries] == [one_line(q) for q in case.expected]


def test_list_values_on_a_far_node_are_rejected(generator):
    # Matched literally, "A,B" would find no pathway and keep every gene.
    case = Case(
        request={
            "nodes": [node("n0", "gene", FTO), node("n1", "pathway", {"pathway_name": "A,B"})],
            "predicates": [predicate("p0", "genes pathways", "n0", "n1")],
            "logic": logic_tree(negated("p0")),
        },
        options={},
        expected=(),
    )

    with pytest.raises(
        LogicValidationError,
        match=re.escape(
            "not supported yet by the Cypher backend: list values on node 'n1', which is only used "
            "by a negated predicate; use an OR group in 'logic' for its values instead"
        ),
    ):
        generate(generator, case)


def test_negated_predicates_leave_the_callers_lists_intact(generator):
    # The generator works on a copy of the lists, so the request it was given
    # still lists every node and predicate afterwards.
    request = copy.deepcopy({**GENE_TRANSCRIPT_PATHWAY_BY_NAME, "logic": logic_tree(negated("p1"))})

    generator.query_Generator(request, {n["node_id"]: n for n in request["nodes"]})

    assert [n["node_id"] for n in request["nodes"]] == ["n0", "n1", "n2"]
    assert [p["predicate_id"] for p in request["predicates"]] == ["p0", "p1"]