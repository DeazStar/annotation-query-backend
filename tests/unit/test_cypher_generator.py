"""Snapshot tests for CypherQueryGenerator.query_Generator.

Each case records the exact Cypher produced today for one request shape:
the result query, the total-count query and the count-by-label query.
They are a safety net: work on boolean 'logic' must not change the queries
built for requests without it. A failing case means the generated text
changed; if the change is intended, update the expected text in the same PR.

The snapshots record current behavior, not necessarily correct behavior.
Whitespace is ignored when comparing; every other character must match.
"""
import copy
import os
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import pytest

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

GENE_PATHWAY_NO_EDGE = {"nodes": [node("n0", "gene", FTO), node("n1", "pathway")], "predicates": []}

ANCHOR_TWO_ARMS_RESULT = """
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
    RETURN n0, p0, p1
"""

ANCHOR_TWO_ARMS_MATCH = """
    MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript), (n0:gene)-[p1:genes_pathways]->(n2:pathway)
    WHERE n0.gene_name =~ '(?i)FTO' AND n0.gene_type =~ '(?i)protein_coding'
"""

ANCHOR_TWO_ARMS_COUNTS = (
    ANCHOR_TWO_ARMS_MATCH + """
    RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1) + COUNT(DISTINCT n2)) AS total_nodes,
           (COUNT(DISTINCT p0) + COUNT(DISTINCT p1)) AS total_edges
    """,
    ANCHOR_TWO_ARMS_MATCH + """
    RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript,
           COUNT(DISTINCT n2) AS n2_pathway, COUNT(DISTINCT p0) AS p0_transcribed_to,
           COUNT(DISTINCT p1) AS p1_genes_pathways
    """,
)

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
        expected=(
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
            """
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript), (n1:transcript)-[p1:translates_to]->(n2:protein),
                  (n0:gene)-[p2:genes_pathways]->(n3:pathway)
            WHERE n0.gene_name =~ '(?i)FTO'
            RETURN (COUNT(DISTINCT n0) + COUNT(DISTINCT n1) + COUNT(DISTINCT n2) + COUNT(DISTINCT n3)) AS total_nodes,
                   (COUNT(DISTINCT p0) + COUNT(DISTINCT p1) + COUNT(DISTINCT p2)) AS total_edges
            """,
            """
            MATCH (n0:gene)-[p0:transcribed_to]->(n1:transcript), (n1:transcript)-[p1:translates_to]->(n2:protein),
                  (n0:gene)-[p2:genes_pathways]->(n3:pathway)
            WHERE n0.gene_name =~ '(?i)FTO'
            RETURN COUNT(DISTINCT n0) AS n0_gene, COUNT(DISTINCT n1) AS n1_transcript,
                   COUNT(DISTINCT n2) AS n2_protein, COUNT(DISTINCT n3) AS n3_pathway,
                   COUNT(DISTINCT p0) AS p0_transcribed_to, COUNT(DISTINCT p1) AS p1_translates_to,
                   COUNT(DISTINCT p2) AS p2_genes_pathways
            """,
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