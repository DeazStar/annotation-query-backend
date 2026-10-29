import pytest

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
