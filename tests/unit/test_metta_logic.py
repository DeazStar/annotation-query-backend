import pytest

from app.error import LogicValidationError
from app.services.metta_generator import MeTTa_Query_Generator


@pytest.fixture
def generator():
    return MeTTa_Query_Generator.__new__(MeTTa_Query_Generator)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("protein coding", "protein_coding"),
        ("  schema:protein coding  ", "protein_coding"),
        ("GO:0008150", "0008150"),
        ("a->b", "a-b"),
        ("__protein__", "protein"),
        ("https://example.org/a", "https:example.orga"),
    ],
)
def test_serialize_metta_value_matches_writer_normalization(generator, value, expected):
    assert generator._serialize_metta_value(value) == expected


@pytest.mark.parametrize(
    "value",
    ["", "   ", "a\tb", "a(b", 'a"b', r"a\b", "$value", ";value"],
)
def test_serialize_metta_value_rejects_unsafe_values(generator, value):
    with pytest.raises(ValueError):
        generator._serialize_metta_value(value)


@pytest.mark.parametrize(
    "request_data, message",
    [
        (
            {"nodes": [{"node_id": "n1", "id": "", "type": "gene",
                        "logic": "OR", "properties": {"gene_type": "protein_coding"}}],
             "predicates": []},
            "node_only",
        ),
        (
            {"nodes": [
                {"node_id": "n1", "id": "", "type": "gene",
                 "logic": "OR", "properties": {"gene_type": "protein_coding"}},
                {"node_id": "n2", "id": "", "type": "protein",
                 "properties": {}},
            ], "predicates": []},
            "single-node",
        ),
        (
            {"nodes": [
                {"node_id": "n1", "id": "", "type": "gene",
                 "logic": "OR", "properties": {"gene_type": "protein_coding"}},
                {"node_id": "n2", "id": "", "type": "protein", "properties": {}},
            ], "predicates": [{"type": "encodes", "source": "n1", "target": "n2"}]},
            "predicates",
        ),
    ],
)
def test_unsupported_boolean_shapes_raise_logic_validation_error(request_data, message):
    generator = MeTTa_Query_Generator.__new__(MeTTa_Query_Generator)

    with pytest.raises(LogicValidationError, match=message):
        generator.query_Generator(
            request_data,
            {node["node_id"]: node for node in request_data["nodes"]},
            node_only=True if message == "node_only" else False,
        )
