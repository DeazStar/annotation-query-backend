import pytest
from flask import json
from unittest.mock import patch, MagicMock
from app import app, databases, schema_manager
from app.lib import validate_request, limit_graph
from distutils.util import strtobool

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

def test_get_nodes_endpoint(client):

    with patch.object(schema_manager, 'get_nodes', return_value=[{'node': 'data'}]):
        response = client.get('/nodes')
        assert response.status_code == 200
        assert json.loads(response.data) == [{'node': 'data'}]

def test_get_edges_endpoint(client):
    with patch.object(schema_manager, 'get_edges', return_value=[{'edge': 'data'}]):
        response = client.get('/edges')
        assert response.status_code == 200
        assert json.loads(response.data) == [{'edge': 'data'}]

def test_get_relations_for_node_endpoint(client):
    with patch.object(schema_manager, 'get_relations_for_node', return_value=[{'relation': 'data'}]):
        response = client.get('/relations/TestNode')
        assert response.status_code == 200
        assert json.loads(response.data) == [{'relation': 'data'}]

@pytest.mark.parametrize("limit,properties", [
    (None, None),
    ('10', 'true'),
    ('5', 'false'),
])
def test_process_query(client, limit, properties):
    mock_data = {
        'requests': {'test': 'data'},
        'email': 'test@example.com'
    }
    mock_node_map = {'node': 'map'}
    mock_query_code = 'test query'
    mock_result = MagicMock()
    mock_parsed_result = (['node1', 'node2'], ['edge1', 'edge2'])

    with patch('app.validate_request', return_value=mock_node_map), \
         patch.dict(databases, {'mock_db': MagicMock()}), \
         patch('app.config', {'database': {'type': 'mock_db'}}), \
         patch.object(databases['mock_db'], 'parse_id', return_value=mock_data['requests']), \
         patch.object(databases['mock_db'], 'query_Generator', return_value=mock_query_code), \
         patch.object(databases['mock_db'], 'run_query', return_value=mock_result), \
         patch.object(databases['mock_db'], 'parse_and_serialize', return_value=mock_parsed_result), \
         patch('app.limit_graph', return_value={'limited': 'graph'}) as mock_limit_graph:

        url = '/query'
        if limit:
            url += f'?limit={limit}'
        if properties:
            url += f'&properties={properties}'

        response = client.post(url, json=mock_data)

        assert response.status_code == 200
        response_data = json.loads(response.data)
        
        if limit:
            mock_limit_graph.assert_called_once()
            assert response_data == {'limited': 'graph'}
        else:
            assert response_data == {'nodes': ['node1', 'node2'], 'edges': ['edge1', 'edge2']}

def test_process_query_missing_data(client):
    response = client.post('/query', json={})
    assert response.status_code == 400
    assert json.loads(response.data) == {"error": "Missing requests data"}

def test_process_query_invalid_limit(client):
    response = client.post('/query?limit=invalid', json={'requests': {}})
    assert response.status_code == 400
    assert json.loads(response.data) == {"error": "Invalid limit value. It should be an integer."}

@patch('app.send_email')
@patch('threading.Thread')
def test_process_email_query(mock_thread, mock_send_email, client):
    mock_data = {
        'requests': {'test': 'data'},
        'email': 'test@example.com'
    }
    response = client.post('/email-query', json=mock_data)
    assert response.status_code == 200
    assert json.loads(response.data) == {'message': 'Email sent successfully'}
    mock_thread.assert_called_once()

def test_process_email_query_missing_data(client):
    response = client.post('/email-query', json={})
    assert response.status_code == 400
    assert json.loads(response.data) == {"error": "Missing requests data"}

def test_process_email_query_missing_email(client):
    response = client.post('/email-query', json={'requests': {}})
    assert response.status_code == 400
    assert json.loads(response.data) == {"error": "Email missing"}

# Add more tests as needed for other functions and edge cases