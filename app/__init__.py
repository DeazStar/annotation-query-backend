from flask import Flask
import yaml
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from app.services.schema_data import SchemaManager
from app.services.cypher_generator import CypherQueryGenerator
from app.services.metta_generator import MeTTa_Query_Generator
from db import mongo_init
from app.services.llm_handler import LLMHandler
from app.persistence.storage_service import StorageService
import os
config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)
 
app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
)

mongo_init()
""" 
databases = {
    "metta": MeTTa_Query_Generator("./Data"),
    "cypher": CypherQueryGenerator("./cypher_data")
    
    # Add other database instances here
}"""
database_type = config['database']['type']
db_instance = None

if database_type == "cypher":
    db_instance = CypherQueryGenerator("./cypher_data")
elif database_type == "metta":
    db_instance = MeTTa_Query_Generator("./Data")
else:
    raise ValueError(f"Unsupported database type: {database_type}")

llm = LLMHandler()  # Initialize the LLMHandler
storage_service = StorageService() # Initialize the storage service

app.config['llm_handler'] = llm
app.config['storage_service'] = storage_service

schema_manager = SchemaManager(schema_config_path='./config/schema_config.yaml', biocypher_config_path='./config/biocypher_config.yaml')

# Import routes at the end to avoid circular imports
from app import routes


























def validate_request(request, schema):
     

    if 'nodes' not in request:
        raise Exception("nodes array is missing")

    nodes = request['nodes']

    # Validate nodes
    if not isinstance(nodes, list):
        raise Exception("nodes should be a list")

    for node in nodes:
        if not isinstance(node, dict):
            raise Exception("Each node must be a dictionary")
        if 'id' not in node:
            raise Exception("id is required!")
        if 'type' not in node or node['type'] == "":
            raise Exception("type is required")
        if 'node_id' not in node or node['node_id'] == "":
            raise Exception("node_id is required")

        node.setdefault('properties', {})

        # Format 'chr' property
        if 'chr' in node["properties"]:
            chr_property = node["properties"]["chr"]
            chr_property = str(chr_property)
            if chr_property and not chr_property.startswith('chr'):
                node["properties"]["chr"] = 'chr' + chr_property

    # Ensure unique node_ids
    node_map = {}
    for node in nodes:
        if node['node_id'] not in node_map:
            node_map[node['node_id']] = node
        else:
            raise Exception(f"Repeated node_id: {node['node_id']}")

    # Validate predicates
    if 'predicates' in request:
        predicates = request['predicates']

        if not isinstance(predicates, list):
            raise Exception("predicates should be a list")
        for predicate in predicates:
            if 'id' not in predicate:
                raise Exception("predicate id is required")
            if 'type' not in predicate or predicate['type'] == "":
                raise Exception("predicate type is required")
            if 'source' not in predicate or predicate['source'] == "":
                raise Exception("source is required")
            if 'target' not in predicate or predicate['target'] == "":
                raise Exception("target is required")

            if predicate['source'] not in node_map:
                raise Exception(f"Source node {predicate['source']} does not exist in the nodes object")
            if predicate['target'] not in node_map:
                raise Exception(f"Target node {predicate['target']} does not exist in the nodes object")

            # Format the predicate type using '_'
            predicate_type = predicate['type'].split(' ')
            predicate_type = '_'.join(predicate_type)

            source_type = node_map[predicate['source']]['type']
            target_type = node_map[predicate['target']]['type']

            formatted_predicate = f'{source_type}-{predicate_type}-{target_type}'
            if formatted_predicate not in schema:
                raise Exception(f"Invalid source and target for the predicate {predicate['type']}")

    # Validate logic
    if 'logic' in request:
        logic = request['logic']

        if not isinstance(logic, dict):
            raise Exception("logic should be a dictionary")

        if 'children' not in logic:
            raise Exception("logic must include children")

        children = logic['children']
        if not isinstance(children, list):
            raise Exception("logic.children should be a list")

        for child in children:
            if not isinstance(child, dict):
                raise Exception("Each logic child must be a dictionary")
            if 'operator' not in child or child['operator'] == "":
                raise Exception("operator is required in each logic child")

            operator = child['operator']
            if operator not in ['AND', 'OR', 'NOT']:
                raise Exception(f"Invalid logic operator: {operator}")

            if operator in ['AND', 'OR'] and 'predicates' not in child:
                raise Exception(f"predicates are required for {operator} operator")
            if operator == 'NOT' and 'nodes' not in child:
                raise Exception("nodes are required for NOT operator")

            if 'predicates' in child:
                for predicate_id in child['predicates']:
                    if predicate_id not in [p['id'] for p in request.get('predicates', [])]:
                        raise Exception(f"Predicate {predicate_id} does not exist")

            if 'nodes' in child:
                node_id = child['nodes'].get('node_id')
                if node_id and node_id not in node_map:
                    raise Exception(f"Node {node_id} does not exist")

    return node_map

