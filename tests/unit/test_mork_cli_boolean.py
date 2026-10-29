from threading import Event

import pytest

from app.error import QueryValidationError
from app.services.mork_cli_generator import MorkCLIQueryGenerator
from app.services.mork_cli_generator import _split_sexprs


class _Symbol:
    def __init__(self, name):
        self._name = name

    def get_name(self):
        return self._name

    def __str__(self):
        return self._name


class _Expression:
    def __init__(self, *children):
        self._children = children

    def get_children(self):
        return self._children

    def __str__(self):
        return f"({' '.join(str(child) for child in self._children)})"


def _node_atom(node_id, node_type="gene"):
    return _Expression(_Symbol("result"), _Expression(_Symbol(node_type), _Symbol(node_id)))


class _BooleanGenerator(MorkCLIQueryGenerator):
    def __init__(self, atoms_by_pattern):
        self.atoms_by_pattern = atoms_by_pattern
        self.stop_events = []

    def run_query(self, query, stop_event=None, species="human"):
        self.stop_events.append(stop_event)
        return [self.atoms_by_pattern.get(query[0][0], [])]


def _query(query_type, spec):
    return (("unused",), ("template",), query_type, "query-id", spec)


def test_boolean_counts_match_or_result_nodes():
    atoms = {
        "property_a": [_node_atom("g1"), _node_atom("g2")],
        "property_b": [_node_atom("g2"), _node_atom("g3")],
    }
    generator = _BooleanGenerator(atoms)
    spec = {"logic": "OR", "patterns": ["property_a", "property_b"]}

    result = generator._run_boolean_query(_query("query", spec))
    count = generator._run_boolean_query(_query("total_count", spec))
    label_count = generator._run_boolean_query(_query("label_count", spec))

    assert len(result[0]) == count[0]["node_count"] == 3
    assert sum(item["count"] for item in label_count[0]["node_count_by_label"]) == len(result[0])


def test_boolean_counts_match_not_result_nodes():
    atoms = {
        "include": [_node_atom("g1"), _node_atom("g2"), _node_atom("g3")],
        "exclude": [_node_atom("g2")],
    }
    generator = _BooleanGenerator(atoms)
    spec = {"logic": "NOT", "include": "include", "exclude": "exclude"}

    result = generator._run_boolean_query(_query("query", spec))
    count = generator._run_boolean_query(_query("total_count", spec))
    label_count = generator._run_boolean_query(_query("label_count", spec))

    assert len(result[0]) == count[0]["node_count"] == 2
    assert sum(item["count"] for item in label_count[0]["node_count_by_label"]) == len(result[0])


def test_boolean_query_passes_stop_event_to_inner_queries():
    generator = _BooleanGenerator({"property": [_node_atom("g1")]})
    stop_event = Event()
    spec = {"logic": "OR", "patterns": ["property"]}

    generator._run_boolean_query(_query("query", spec), stop_event=stop_event)

    assert generator.stop_events == [stop_event]


def test_split_sexprs_creates_one_act_source_per_property():
    pattern = (
        "(gene_type (gene $n) protein_coding) "
        "(gene_name (gene $n) BRCA1)"
    )

    clauses = _split_sexprs(pattern)
    sources = [f"(ACT mork_test {clause})" for clause in clauses]

    assert len(sources) == 2
    assert all(source.startswith("(ACT mork_test (") for source in sources)


def test_split_sexprs_keeps_parentheses_inside_quoted_value():
    pattern = '(description (gene $n) "contains (parentheses)")'

    assert _split_sexprs(pattern) == [pattern]


@pytest.mark.parametrize(
    "node, message",
    [
        (
            {"node_id": "n1", "id": "gene-1", "type": "gene", "logic": "OR",
             "properties": {"gene_type": "protein_coding"}},
            "logic OR/NOT cannot be used on a node with an id",
        ),
        (
            {"node_id": "n1", "id": "", "type": "gene", "logic": "NOT",
             "properties": {}},
            "logic NOT requires at least one positive filter",
        ),
    ],
)
def test_invalid_boolean_filters_raise_query_validation_error(node, message):
    generator = MorkCLIQueryGenerator.__new__(MorkCLIQueryGenerator)

    with pytest.raises(QueryValidationError, match=message):
        generator._boolean_spec([node])
