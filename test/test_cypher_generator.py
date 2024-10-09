import neo4j
import pytest
from unittest.mock import patch, MagicMock
from neo4j import GraphDatabase
from cypher_generator import CypherQueryGenerator

@pytest.fixture
def mock_driver():
    with patch('neo4j.GraphDatabase.driver') as mock:
        yield mock

@pytest.fixture
def cypher_generator(mock_driver):
    with patch.dict('os.environ', {'NEO4J_URI': 'bolt://localhost:7687', 'NEO4J_USERNAME': 'neo4j', 'NEO4J_PASSWORD': 'password'}):
        return CypherQueryGenerator('dummy/path')

def test_init(cypher_generator):
    assert isinstance(cypher_generator.driver, MagicMock)

def test_close(cypher_generator):
    cypher_generator.close()
    cypher_generator.driver.close.assert_called_once()

@pytest.mark.parametrize("path,expected", [
    ('/non/existent/path', ValueError),
    ('/path/with/no/cypher/files', ValueError),
])
def test_load_dataset_errors(cypher_generator, path, expected):
    with pytest.raises(expected):
        cypher_generator.load_dataset(path)

def test_load_dataset_success(cypher_generator, tmp_path):
    # Create dummy .cypher files
    (tmp_path / "nodes.cypher").write_text("CREATE (n:Person {name: 'Alice'})")
    (tmp_path / "edges.cypher").write_text("MATCH (a:Person), (b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob' CREATE (a)-[:KNOWS]->(b)")

    with patch.object(cypher_generator, 'run_query') as mock_run_query:
        cypher_generator.load_dataset(str(tmp_path))
        assert mock_run_query.call_count == 2

def test_run_query(cypher_generator):
    mock_session = MagicMock()
    cypher_generator.driver.session.return_value.__enter__.return_value = mock_session
    
    cypher_generator.run_query("MATCH (n) RETURN n")
    mock_session.run.assert_called_once_with("MATCH (n) RETURN n")

@pytest.mark.parametrize("request,node_map,expected", [
    (
        {
            "nodes": [{"id": "1", "type": "Person", "node_id": "p1"}],
            "predicates": []
        },
        {"p1": {"id": "1", "type": "Person", "node_id": "p1"}},
        ["MATCH (p1:Person {id: '1'}) RETURN p1"]
    ),
    (
        {
            "nodes": [
                {"id": "1", "type": "Person", "node_id": "p1"},
                {"id": "2", "type": "Person", "node_id": "p2"}
            ],
            "predicates": [{"type": "KNOWS", "source": "p1", "target": "p2"}]
        },
        {
            "p1": {"id": "1", "type": "Person", "node_id": "p1"},
            "p2": {"id": "2", "type": "Person", "node_id": "p2"}
        },
        ["MATCH (p1:Person {id: '1'}), (p1)-[r0:knows]->(p2:Person {id: '2'}) RETURN r0, p1, p2"]
    ),
])
def test_query_generator(cypher_generator, request, node_map, expected):
    result = cypher_generator.query_Generator(request, node_map)
    assert result == expected

def test_parse_neo4j_results(cypher_generator):
    mock_node = MagicMock(spec=neo4j.graph.Node)
    mock_node.__getitem__.side_effect = lambda key: "value" if key != "id" else "1"
    mock_node.labels = ["Person"]
    mock_node.items.return_value = [("name", "Alice"), ("age", 30)]

    mock_relationship = MagicMock(spec=neo4j.graph.Relationship)
    mock_relationship.type = "KNOWS"
    mock_relationship.start_node = mock_node
    mock_relationship.end_node = mock_node
    mock_relationship.items.return_value = [("since", 2020)]

    mock_results = [
        MagicMock(values=lambda: [mock_node]),
        MagicMock(values=lambda: [mock_relationship])
    ]

    result = cypher_generator.parse_neo4j_results(mock_results, all_properties=True)
    
    assert "nodes" in result
    assert "edges" in result
    assert len(result["nodes"]) == 1
    assert len(result["edges"]) == 1
    assert result["nodes"][0]["data"]["type"] == "Person"
    assert result["edges"][0]["data"]["label"] == "KNOWS"

def test_parse_id(cypher_generator):
    request = {
        "nodes": [
            {"id": "ALICE", "type": "gene", "node_id": "g1"},
            {"id": "ENSG00000139618", "type": "gene", "node_id": "g2"},
            {"id": "BRCA2-201", "type": "transcript", "node_id": "t1"}
        ]
    }
    
    result = cypher_generator.parse_id(request)
    
    assert result["nodes"][0]["properties"]["gene_name"] == "ALICE"
    assert result["nodes"][0]["id"] == ""
    assert result["nodes"][1]["id"] == "ensg00000139618"
    assert result["nodes"][2]["properties"]["transcript_name"] == "BRCA2-201"
    assert result["nodes"][2]["id"] == ""

if __name__ == '__main__':
    pytest.main(['-v', 'test_cypher_query_generator.py'])