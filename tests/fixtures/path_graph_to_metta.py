import argparse
from pathlib import Path

from tests.fixtures.path_graph import FULL_EDGES, FULL_NODES


def _atom(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value).replace(" ", "_")


def write_fixture(directory, id_prefix="", run_id=None):
    output = Path(directory)
    output.mkdir(parents=True, exist_ok=True)
    nodes = {node["id"]: node for node in FULL_NODES}
    edges = {}
    for edge in FULL_EDGES:
        key = (
            edge["type"].replace(" ", "_"),
            edge["source"],
            edge["target"],
        )
        edges.setdefault(key, edge)

    lines = []
    for node in nodes.values():
        node_type = node["type"]
        node_id = f"{id_prefix}{node['id']}"
        lines.append(f"({node_type} {node_id})")
        for key, value in node.get("properties", {}).items():
            if key == "degree":
                continue
            lines.append(f"({key} ({node_type} {node_id}) {_atom(value)})")
        if run_id is not None:
            lines.append(
                f"(test_run_id ({node_type} {node_id}) {_atom(run_id)})"
            )
    for edge in edges.values():
        predicate = edge["type"].replace(" ", "_")
        source = f"({edge['source_type']} {id_prefix}{edge['source']})"
        target = f"({edge['target_type']} {id_prefix}{edge['target']})"
        lines.append(f"({predicate} {source} {target})")
        evidence = dict(edge.get("evidence", {}))
        if run_id is not None:
            evidence["test_run_id"] = run_id
        for key, value in evidence.items():
            lines.append(
                f"({key} ({predicate} {source} {target}) {_atom(value)})"
            )
    (output / "path_graph.metta").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory")
    args = parser.parse_args()
    write_fixture(args.directory)


if __name__ == "__main__":
    main()
