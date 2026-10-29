import pytest
from copy import deepcopy
from app.lib.validator import validate_request
from app.lib.query_canonical import canonicalize_graph, query_fingerprint
from app.services.schema_data import SchemaManager

schema_manager = SchemaManager(
    human_schema_config_path='./config/human_schema/human_full_schema_config.yaml',
    biocypher_config_path='./config/biocypher_config.yaml',
    human_datasources_config_path='./config/human_schema/data_source_schemas',
    fly_schema_config_path='./config/fly_base_schema/dmel_full_schema_config.yaml',
)


def test_validation_preserves_fingerprint_without_logic():
    request = {
        "nodes": [{
            "node_id": "n1",
            "id": "",
            "type": "gene",
            "properties": {"gene_type": "protein_coding"},
        }],
        "predicates": [],
    }
    before, _ = canonicalize_graph(deepcopy(request))
    before_fingerprint = query_fingerprint(
        before, "human", "all", None, False, True, "metta"
    )

    validate_request(request, schema_manager.schema, "hypothesis")

    after, _ = canonicalize_graph(deepcopy(request))
    after_fingerprint = query_fingerprint(
        after, "human", "all", None, False, True, "metta"
    )

    assert "logic" not in request["nodes"][0]
    assert after_fingerprint == before_fingerprint

def test_node_is_missing():
    request = []
    with pytest.raises(Exception, match="'nodes' key is missing"):
        validate_request(request, schema_manager.schema, None)

def test_wrong_node_type():
    requests = [{"nodes": ''},{"nodes": {}},{"nodes": set()},{"nodes": ()}]
    with pytest.raises(Exception, match="'nodes' must be a list"):
        for request in requests:
            validate_request(request, schema_manager.schema, None)

def test_wrong_node_values():
    requests = [ {'nodes':[str()]},{'nodes':[list()]},{'nodes':[set()]},{'nodes':[tuple()]} ]
    with pytest.raises(Exception, match="each item in 'nodes' must be a dictionary"):
        for request in requests:
            validate_request(request, schema_manager.schema, None)

def test_node_without_node_id():
    #assert each node in nodes value have a node_id
    request = {'nodes': [
        {"id": "", # node without node_id
        "type": "gene",
        "properties": {}
      }]}

    with pytest.raises(Exception, match="'node_id' is required"):
        validate_request(request, schema_manager.schema, None)

    request = { 'nodes':[
               {"node_id": "", # node with empty node_id
                "id": "",
                "type": "gene",
                "properties": {}
                }]}

    with pytest.raises(Exception, match="'node_id' is required"):
        validate_request(request, schema_manager.schema, None)


def test_node_without_id():
    request = {'nodes': [
        { "node_id": "n1", # node without id
          "type": "gene",
          "properties": {}
        }]}

    with pytest.raises(Exception, match="'id' is required"):
        validate_request(request, schema_manager.schema, None)

def test_node_without_type():
    request = {'nodes': [
        {"node_id": "n1", # node without type
         "id": "",
        "properties": {}
      }]}

    with pytest.raises(Exception, match="'type' is required"):
        validate_request(request, schema_manager.schema, None)

    request = { 'nodes':[
               {"node_id": "", # node with empty type
                "id": "",
                "type": "",
                "properties": {}
                }]}

    with pytest.raises(Exception, match="'type' is required"):
        validate_request(request, schema_manager.schema, None)

'''
def test_properties_key():
    # assert each predicate has proper keys
    request = {'nodes': [{
            "node_id": "n3",
            "id": "",
            "type": "protein",
            "properties": { # node with improper properties key
                    "protein_nam": "MKKS"
                }
        }
    ]}

    with pytest.raises(Exception, match="protein_nam doesn't exsist in the schema!"):
        validate_request(request, schema_manager.schema, None)
'''

def test_predicate_type():
    # assert predicate is of type list
    requests = [
                {'nodes': [{
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                  }]
                 , 'predicates': str()}, # predicates with str type
                {'nodes': [{
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                  }]
                 , 'predicates': dict()}, # predicates with list type
                {'nodes': [{
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                  }]
                 , 'predicates': set()},
                {'nodes': [{
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                  }]
                 , 'predicates': tuple()},
               ]


    with pytest.raises(Exception, match="'predicates' must be a list"):
        for request in requests:
            validate_request(request, schema_manager.schema, None)

def test_predicates_type():
    request = {
               'nodes':[
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                }],
                'predicates':[
                {
                    "source": "n1",
                    "target": "n2"
                }]
            }

    with pytest.raises(Exception, match="'type' is required for each predicate"):
        validate_request(request, schema_manager.schema, None)

    request['predicates'][0]['type'] = ""

    with pytest.raises(Exception, match="'type' is required for each predicate"):
        validate_request(request, schema_manager.schema, None)

def test_predicates_source():
    request = {
               'nodes':[
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                }],
                'predicates':[
                {
                    "type": "translates_to",
                    "target": "n2"
                }]
            }

    with pytest.raises(Exception, match="'source' is required"):
        validate_request(request, schema_manager.schema, None)

    request['predicates'][0]['source'] = ""

    with pytest.raises(Exception, match="'source' is required"):
        validate_request(request, schema_manager.schema, None)


def test_predicates_target():
    request = {
               'nodes':[
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                }],
                'predicates':[
                {
                    "type": "translates_to",
                    "source": "n1"
                }]
            }

    with pytest.raises(Exception, match="'target' is required"):
        validate_request(request, schema_manager.schema, None)

    request['predicates'][0]['target'] = ""

    with pytest.raises(Exception, match="'target' is required"):
        validate_request(request, schema_manager.schema, None)

def test_predicte_source_map():
    request = {
               'nodes':[
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                }],
                'predicates':[
                {
                    "type": "translates_to",
                    "source": "n0",
                    "target": "n1",
                }]
            }

    with pytest.raises(Exception, match="source node 'n0' does not exist"):
        validate_request(request, schema_manager.schema, None)

def test_predicte_target_map():
    request = {
               'nodes':[
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {}
                }],
                'predicates':[
                {
                    "type": "translates_to",
                    "source": "n1",
                    "target": "n0",
                }]
            }

    with pytest.raises(Exception, match="target node 'n0' does not exist"):
        validate_request(request, schema_manager.schema, None)

# add test for last exception
def test_predicate_schema_type():
    pass
