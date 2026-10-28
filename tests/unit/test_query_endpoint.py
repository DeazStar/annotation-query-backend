"""Unit tests for how POST /query reports errors raised while building a query.

process_query is called directly with fakes for the database, Redis and
MongoDB lookups, so no service needs to run.
"""
import contextlib
import os
from types import SimpleNamespace

import pytest

# The endpoint module builds an LLM handler on import; without a configured
# model it raises. 'openai' with no API key builds a handler with no model.
os.environ.setdefault("LLM_MODEL", "openai")

from fastapi import HTTPException  # noqa: E402

from app.api.api_v1.endpoints import query as query_endpoint  # noqa: E402
from app.lib.validator import LogicValidationError  # noqa: E402

REQUEST = {
    "nodes": [
        {"node_id": "n0", "id": "", "type": "gene", "properties": {"gene_name": "FTO"}},
        {"node_id": "n1", "id": "", "type": "transcript", "properties": {}},
    ],
    "predicates": [{"type": "transcribed to", "source": "n0", "target": "n1"}],
}


class FakeGenerator:
    """A backend whose query_Generator raises the given error."""

    def __init__(self, error):
        self.error = error

    def parse_id(self, request):
        return request

    def query_Generator(self, *args, **kwargs):
        raise self.error


class FakeRedis:
    def lock(self, *args, **kwargs):
        return contextlib.nullcontext()


@pytest.fixture
def call_process_query(monkeypatch):
    """Return a function that runs process_query with a backend raising `error`."""
    monkeypatch.setattr(query_endpoint.UserStorageService, "get", staticmethod(lambda user_id: None))
    monkeypatch.setattr(
        query_endpoint.AnnotationStorageService, "get_by_fingerprint", staticmethod(lambda fingerprint: None)
    )

    def call(error, logic=None):
        generator = FakeGenerator(error)
        monkeypatch.setattr(query_endpoint, "get_db_instance", lambda species: generator)
        requests = {**REQUEST, "nodes": [dict(n) for n in REQUEST["nodes"]],
                    "predicates": [dict(p) for p in REQUEST["predicates"]]}
        if logic is not None:
            requests["logic"] = logic
        return query_endpoint.process_query(
            data={"requests": requests},
            limit=None,
            properties=None,
            source=None,
            current_user_id="user-1",
            db_instance=generator,
            llm=None,
            redis_client=FakeRedis(),
            schema_manager=SimpleNamespace(schema={"human": {"gene_transcribed_to_transcript": {}}}),
        )

    return call


def test_logic_error_from_a_backend_becomes_400_with_its_message(call_process_query):
    with pytest.raises(HTTPException) as raised:
        call_process_query(LogicValidationError("not supported yet by the test backend: something"))

    assert raised.value.status_code == 400
    assert raised.value.detail == "not supported yet by the test backend: something"


def test_other_backend_errors_still_become_500(call_process_query):
    # Only LogicValidationError is the caller's fault; anything else stays a server error.
    with pytest.raises(HTTPException) as raised:
        call_process_query(RuntimeError("database down"))

    assert raised.value.status_code == 500


def test_misspelled_logic_key_becomes_400_naming_the_key(call_process_query):
    # The typo from the review: before, it was ignored and the opposite results came back.
    logic = {
        "operater": "NOT",
        "children": [{"operator": "AND", "nodes": {"node_id": "n0", "properties": {"gene_type": "lncRNA"}}}],
    }

    with pytest.raises(HTTPException) as raised:
        call_process_query(AssertionError("validation must stop the request first"), logic=logic)

    assert raised.value.status_code == 400
    assert "unknown key 'operater' in 'logic'" in raised.value.detail