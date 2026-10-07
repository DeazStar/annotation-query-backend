"""Check the Cypher built for boolean 'logic' requests against a real Neo4j.

Loads a tiny graph whose answers are known in advance into an EMPTY Neo4j,
sends each request through the real pipeline (validate_request ->
canonicalize_graph -> heuristic_sort -> query_Generator), runs the queries,
and checks the returned names and the total counts.

Start a throwaway Neo4j 5 (the CALL (x) {...} syntax needs 5.21 or later):

    docker run --rm -d --name logic-check -p 7690:7687 \\
        -e NEO4J_AUTH=neo4j/logic-check-pw neo4j:5.26-community

Then, from the repository root:

    PYTHONPATH=. python scripts/check_cypher_logic.py --uri bolt://localhost:7690 --password logic-check-pw

Stop it afterwards with: docker stop logic-check
Use --dry-run to only print the generated queries, without a database.
"""
import argparse
import copy
import sys
import time

from app.lib.heuristic_sort import heuristic_sort
from app.lib.query_canonical import canonicalize_graph
from app.lib.validator import validate_request
from app.services.cypher_generator import CypherQueryGenerator

SCHEMA = {
    "gene_transcribed_to_transcript": {},
    "gene_genes_pathways_pathway": {},
    "transcript_translates_to_protein": {},
}

# Known data. FTO: 3 transcripts, pathways Insulin signaling and Obesity.
# GENE_A: 2 transcripts, no pathway. GENE_B: no transcript, pathway Obesity,
# no gene_type stored. GENE_C: nothing. ORPHAN-1: a transcript from no gene.
SEED = """
CREATE (fto:gene {id: 'ENSG00000140718', gene_name: 'FTO', gene_type: 'protein_coding'}),
       (a:gene {id: 'GENEA', gene_name: 'GENE_A', gene_type: 'lncRNA'}),
       (b:gene {id: 'GENEB', gene_name: 'GENE_B'}),
       (c:gene {id: 'GENEC', gene_name: 'GENE_C', gene_type: 'pseudogene'}),
       (t1:transcript {id: 'T1', transcript_name: 'FTO-201', transcript_type: 'protein_coding'}),
       (t2:transcript {id: 'T2', transcript_name: 'FTO-202', transcript_type: 'retained_intron'}),
       (t3:transcript {id: 'T3', transcript_name: 'FTO-203', transcript_type: 'protein_coding'}),
       (a1:transcript {id: 'A1', transcript_name: 'A-201', transcript_type: 'lncRNA'}),
       (a2:transcript {id: 'A2', transcript_name: 'A-202', transcript_type: 'lncRNA'}),
       (o1:transcript {id: 'O1', transcript_name: 'ORPHAN-1', transcript_type: 'protein_coding'}),
       (insulin:pathway {id: 'P1', pathway_name: 'Insulin signaling'}),
       (obesity:pathway {id: 'P2', pathway_name: 'Obesity'}),
       (pr1:protein {id: 'PR1', protein_name: 'PROT1'}),
       (pr2:protein {id: 'PR2', protein_name: 'PROT2'}),
       (fto)-[:transcribed_to]->(t1), (fto)-[:transcribed_to]->(t2), (fto)-[:transcribed_to]->(t3),
       (a)-[:transcribed_to]->(a1), (a)-[:transcribed_to]->(a2),
       (fto)-[:genes_pathways]->(insulin), (fto)-[:genes_pathways]->(obesity),
       (b)-[:genes_pathways]->(obesity),
       (t1)-[:translates_to]->(pr1), (o1)-[:translates_to]->(pr2)
"""


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

# (name, request, label to check, names expected for that label)
SCENARIOS = [
    ("no logic: transcripts AND pathways",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway")], "predicates": [GENE_TX, GENE_PW]},
     "gene", {"FTO"}),
    ("property OR: FTO or lncRNA",
     {"nodes": [node("g", "gene")], "predicates": [],
      "logic": {"children": [props("OR", "g", {"gene_name": "FTO", "gene_type": "lncRNA"})]}},
     "gene", {"FTO", "GENE_A"}),
    ("property NOT: not protein_coding (GENE_B has no type: kept)",
     {"nodes": [node("g", "gene")], "predicates": [],
      "logic": {"children": [props("NOT", "g", {"gene_type": "protein_coding"})]}},
     "gene", {"GENE_A", "GENE_B", "GENE_C"}),
    ("nested: (FTO and protein_coding) or GENE_C",
     {"nodes": [node("g", "gene")], "predicates": [],
      "logic": {"children": [{"operator": "OR", "children": [
          props("AND", "g", {"gene_name": "FTO", "gene_type": "protein_coding"}),
          props("AND", "g", {"gene_name": "GENE_C"})]}]}},
     "gene", {"FTO", "GENE_C"}),
    ("NOT edge: genes with no pathway",
     {"nodes": [node("g", "gene"), node("p", "pathway")], "predicates": [GENE_PW],
      "logic": {"children": [preds("NOT", "pw")]}},
     "gene", {"GENE_A", "GENE_C"}),
    ("NOT edge: genes with transcripts and no pathway",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway")], "predicates": [GENE_TX, GENE_PW],
      "logic": {"children": [preds("NOT", "pw")]}},
     "gene", {"GENE_A"}),
    ("NOT edge, far filter: transcripts, not in Insulin signaling",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway", {"pathway_name": "Insulin signaling"})],
      "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("NOT", "pw")]}},
     "gene", {"GENE_A"}),
    ("NOT edge in an arm: FTO's transcripts that make no protein",
     {"nodes": [node("g", "gene", {"gene_name": "FTO"}), node("t", "transcript"), node("p", "pathway"), node("r", "protein")],
      "predicates": [GENE_TX, GENE_PW, edge("tr", "translates to", "t", "r")],
      "logic": {"children": [preds("NOT", "tr")]}},
     "transcript", {"FTO-202", "FTO-203"}),
    ("NOT edge, kept target: transcripts making a protein, from no gene",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("r", "protein")],
      "predicates": [GENE_TX, edge("tr", "translates to", "t", "r")],
      "logic": {"children": [preds("NOT", "tx")]}},
     "transcript", {"ORPHAN-1"}),
    ("OR edges: transcripts or pathways",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway")], "predicates": [GENE_TX, GENE_PW],
      "logic": {"children": [preds("OR", "tx", "pw")]}},
     "gene", {"FTO", "GENE_A", "GENE_B"}),
    ("OR edges, far filter: transcripts or Insulin signaling",
     {"nodes": [node("g", "gene"), node("t", "transcript"), node("p", "pathway", {"pathway_name": "Insulin signaling"})],
      "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("OR", "tx", "pw")]}},
     "gene", {"FTO", "GENE_A"}),
    ("list path + NOT edge: FTO,GENE_A with transcripts, no pathway",
     {"nodes": [node("g", "gene", {"gene_name": "FTO,GENE_A"}), node("t", "transcript"), node("p", "pathway")],
      "predicates": [GENE_TX, GENE_PW], "logic": {"children": [preds("NOT", "pw")]}},
     "gene", {"GENE_A"}),
]

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


def collect_nodes(value, found):
    """Collect every Neo4j node inside a result value (CALL arms return lists of maps)."""
    if hasattr(value, "labels") and hasattr(value, "element_id"):
        found[value.element_id] = value
    elif isinstance(value, (list, tuple)):
        for item in value:
            collect_nodes(item, found)
    elif isinstance(value, dict):
        for item in value.values():
            collect_nodes(item, found)


def names_with_label(nodes, label):
    return {
        next(node[key] for key in NAME_KEYS if key in node)
        for node in nodes.values() if label in node.labels
    }


def wait_for_neo4j(driver, timeout_seconds):
    """Wait until Neo4j accepts queries; a container that just started refuses them for a while."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            driver.verify_connectivity()
            return True
        except Exception:
            if time.monotonic() > deadline:
                return False
            print("Waiting for Neo4j to start...")
            time.sleep(3)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--uri", default="bolt://localhost:7690")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--password", default="logic-check-pw")
    parser.add_argument("--dry-run", action="store_true", help="print the queries without a database")
    parser.add_argument("--wait", type=int, default=120, help="seconds to wait for Neo4j to start (default 120)")
    args = parser.parse_args()

    if args.dry_run:
        for name, request, _, _ in SCENARIOS:
            print(f"--- {name}\n{build_queries(request)[0].strip()}\n")
        return 0

    from neo4j import GraphDatabase

    failures = 0
    with GraphDatabase.driver(args.uri, auth=(args.user, args.password)) as driver:
        if not wait_for_neo4j(driver, args.wait):
            print(f"Neo4j at {args.uri} did not become ready within {args.wait} seconds.")
            return 2
        with driver.session() as session:
            if session.run("MATCH (n) RETURN count(n) AS n").single()["n"] != 0:
                print("Refusing to run: the database is not empty. Use a throwaway Neo4j (see the docstring).")
                return 2
            session.run(SEED).consume()
            try:
                for name, request, label, expected in SCENARIOS:
                    result_query, total_query, _ = build_queries(request)
                    found = {}
                    for record in session.run(result_query):
                        collect_nodes(list(record.values()), found)
                    got = names_with_label(found, label)
                    total = session.run(total_query).single()["total_nodes"]

                    problems = []
                    if got != expected:
                        problems.append(f"{label}s: expected {sorted(expected)}, got {sorted(got)}")
                    if total != len(found):
                        problems.append(f"total_nodes count {total} != {len(found)} nodes in the result")
                    failures += bool(problems)
                    print(f"{'FAIL' if problems else 'ok  '}  {name}")
                    for problem in problems:
                        print(f"        {problem}")
            finally:
                session.run("MATCH (n) DETACH DELETE n").consume()

    print(f"\n{len(SCENARIOS) - failures}/{len(SCENARIOS)} scenarios passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())