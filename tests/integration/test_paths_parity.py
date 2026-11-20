"""Parity is not verified by unit/CI runs.

Live Neo4j/MORK parity requires Neo4j credentials and a host-visible
PATHS_PARITY_MORK_DIR shared by the Docker daemon. Neo4j connectivity is
also required for the existing neo4j_paths fixture.
"""

import os
import shutil
import subprocess
from copy import deepcopy

import pytest

from app.services.mork_cli_generator import MorkCLIQueryGenerator
from app.services.path_search import normalize_request, process_candidates
from scripts.build_act import build_act
from tests.fixtures.path_graph_to_metta import write_fixture
from tests.integration.test_paths_neo4j import (
    PREFIX,
    neo4j_paths,
)


def _docker_available():
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=10,
        )
        return True
    except (subprocess.SubprocessError, OSError):
        return False


def _mork_image_available():
    if not _docker_available():
        return False
    try:
        subprocess.run(
            ["docker", "image", "inspect", "mork:latest"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
            timeout=10,
        )
        return True
    except (subprocess.SubprocessError, OSError):
        return False


@pytest.fixture(scope="module")
def mork_parity(neo4j_paths):
    if not _docker_available():
        pytest.skip("Docker is unavailable")
    if not _mork_image_available():
        pytest.skip("Docker image mork:latest is missing")

    host_dir = os.getenv("PATHS_PARITY_MORK_DIR")
    if not host_dir:
        pytest.skip(
            "PATHS_PARITY_MORK_DIR is unset; the host-visible MORK directory "
            "cannot be mounted from the Docker daemon"
        )

    directory = os.path.abspath(host_dir)
    os.makedirs(directory, exist_ok=True)
    write_fixture(directory, id_prefix=PREFIX, run_id=neo4j_paths.run_id)
    os.environ["MORK_DATA_DIR"] = directory
    if not build_act(directory):
        pytest.fail(f"Failed to build annotation.act in {directory}")
    generator = MorkCLIQueryGenerator(directory)
    if not generator.is_ready():
        pytest.skip("annotation.act is missing after ACT build")
    return generator


def _request(context, **changes):
    value = {
        "sources": [{"type": "gene", "id": context.source_id}],
        "targets": [{"type": "gene", "id": context.target_id}],
    }
    value.update(changes)
    return normalize_request(value)


def _mork_request(**changes):
    value = {
        "sources": [{"type": "gene", "id": f"{PREFIX}gene-a"}],
        "targets": [{"type": "gene", "id": f"{PREFIX}gene-b"}],
    }
    value.update(changes)
    return normalize_request(value)


PARITY_REQUESTS = [
    pytest.param({}, id="base-hub-avoidance-off"),
    pytest.param(
        {"hub_avoidance": {"enabled": True, "max_degree": 3, "exclude_ids": []}},
        id="hub-avoidance-on-max-degree-3",
    ),
    pytest.param({"max_hops": 1}, id="max-hops-1"),
    pytest.param({"max_hops": 2}, id="max-hops-2"),
    pytest.param({"max_hops": 3}, id="max-hops-3"),
    pytest.param({"max_hops": 4}, id="max-hops-4"),
    pytest.param({"k": 1}, id="k-1-truncated"),
    pytest.param({"k": 2}, id="k-2"),
    pytest.param(
        {"edge_types": {"allow": ["interacts_with", "participates_in"]}},
        id="edge-types-allow",
    ),
    pytest.param(
        {"edge_types": {"exclude": ["participates_in"]}},
        id="edge-types-exclude",
    ),
    pytest.param(
        {"intermediate_node_types": {"allow": ["gene", "protein"]}},
        id="intermediate-node-types-allow",
    ),
    pytest.param(
        {"intermediate_node_types": {"exclude": ["protein"]}},
        id="intermediate-node-types-exclude",
    ),
    pytest.param(
        {"evidence_filters": [{"field": "score", "operator": ">=", "value": 700}]},
        id="score-gte-700",
    ),
    pytest.param(
        {"evidence_filters": [{"field": "score", "operator": "<", "value": 700}]},
        id="score-lt-700",
    ),
    pytest.param(
        {"evidence_filters": [{"field": "p_value", "operator": "<", "value": 0.03}]},
        id="p-value-lt-003",
    ),
    pytest.param(
        {"hub_avoidance": {"enabled": False, "exclude_ids": ["TESTPATHS_protein-a"]}},
        id="exclude-intermediate-id",
    ),
    pytest.param(
        {
            "sources": [{"type": "gene", "id": "TESTPATHS_gene-b"}],
            "targets": [{"type": "gene", "id": "TESTPATHS_gene-a"}],
        },
        id="reversed-direction",
    ),
    pytest.param(
        {
            "sources": [{"type": "protein", "id": "TESTPATHS_hub"}],
            "targets": [{"type": "gene", "id": "TESTPATHS_gene-b"}],
        },
        id="hub-source-endpoint",
    ),
    pytest.param(
        {"targets": [{"type": "gene", "id": "TESTPATHS_gene-c"}]},
        id="no-path-gene-c",
    ),
    pytest.param(
        {
            "sources": [{"type": "gene", "id": "TESTPATHS_unknown-source"}],
            "targets": [{"type": "gene", "id": "TESTPATHS_unknown-target"}],
        },
        id="all-unknown",
    ),
    pytest.param(
        {
            "targets": [
                {"type": "gene", "id": "TESTPATHS_gene-b"},
                {"type": "gene", "id": "TESTPATHS_unknown"},
            ],
        },
        id="partly-unknown",
    ),
    pytest.param(
        {
            "sources": [
                {"type": "gene", "id": "TESTPATHS_gene-a"},
                {"type": "gene", "id": "TESTPATHS_gene-c"},
            ],
            "targets": [
                {"type": "gene", "id": "TESTPATHS_gene-b"},
                {"type": "gene", "id": "TESTPATHS_gene-c"},
            ],
        },
        id="multiple-sources-and-targets",
    ),
]


@pytest.mark.parametrize("changes", PARITY_REQUESTS)
def test_neo4j_mork_paths_parity(neo4j_paths, mork_parity, changes):
    neo_request = _request(neo4j_paths, **deepcopy(changes))
    mork_request = _mork_request(**deepcopy(changes))

    neo_result = process_candidates(
        neo4j_paths.generator.find_paths(neo_request),
        neo_request,
    )
    mork_result = process_candidates(
        mork_parity.find_paths(mork_request),
        mork_request,
    )

    assert mork_result == neo_result
    assert mork_result["paths"] == neo_result["paths"]
    assert mork_result["truncated"] == neo_result["truncated"]
    assert mork_result["has_more"] == neo_result["has_more"]


def test_mork_paths_are_deterministic(neo4j_paths, mork_parity):
    request = _mork_request(hub_avoidance={"enabled": False})
    results = [
        process_candidates(mork_parity.find_paths(request), request)
        for _ in range(3)
    ]
    assert results[0] == results[1] == results[2]
