from flask import Flask, request, jsonify
from biocypher import BioCypher
from metta_generator import generate_metta
from hyperon import MeTTa
import logging
# from hyperon import *
import json
import glob
import os
from typing import List
import re
import uuid
import yaml 
# Setup basic logging
logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
metta = MeTTa()
metta.run("!(bind! &space (new-space))")  # Initialize a new space at application start

bcy = BioCypher(schema_config_path='schema_config.yaml', biocypher_config_path='biocypher_config.yaml')
schema = bcy._get_ontology_mapping()._extend_schema()

def load_schema_config(path: str):
    with open(path, 'r') as file:
        schema = yaml.safe_load(file)
    return schema



def load_dataset(path: str) -> None:
    if not os.path.exists(path):
        raise ValueError(f"Dataset path '{path}' does not exist.")

    paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
    if not paths:
        raise ValueError(f"No .metta files found in dataset path '{path}'.")

    for path in paths:
        print(f"Start loading dataset from '{path}'...")

        try:
            metta.run(f'''
                !(load-ascii &space {path})
                ''')
        except Exception as e:
            print(f"Error loading dataset from '{path}': {e}")

    print(f"Finished loading {len(paths)} datasets.")


load_dataset("./Data")


def parse_and_serialize(input_string):
    # Remove the outermost brackets and any unwanted characters
    cleaned_string = re.sub(r"[,\[\]]", "", input_string)

    # Find all tuples using regex
    tuples = re.findall(r"(\w+)\s+\((\w+)\s+(\w+)\)\s+\((\w+)\s+(\w+)\)", cleaned_string)
    # logging.debug(f"Generated tuples Code: {tuples}")
    # Convert tuples to JSON format
    result = []
    for tuple in tuples:
        predicate, src_type, src_id, tgt_type, tgt_id = tuple
        result.append({
            "id": str(uuid.uuid4()),
            "predicate": predicate,
            "source": f"{src_type} {src_id}",
            "target": f"{tgt_type} {tgt_id}"
        })

    return json.dumps(result, indent=2)  


def get_nodes():
    nodes = []
    for key, value in schema.items():
        if value['represented_as'] == 'node':
            nodes.append({
                'type': key,
                'is_a': value['is_a'],
                'label': value['input_label'],
                'properties': value.get('properties', {})
            })
    
    return nodes

@app.route('/nodes', methods=['GET'])
def get_nodes_endpoint():
    return jsonify(get_nodes())

@app.route('/query', methods=['POST'])
def process_query():
    data = request.get_json()
    if not data or 'requests' not in data:
        return jsonify({"error": "Missing requests data"}), 400

    try:
        requests = data['requests']
        # Parse and serialize the received data
        schema = load_schema_config('./schema_config.yaml')
        # logging.debug(f"schema: {schema}")
        query_code = generate_metta(requests, schema)
        # logging.debug(f"query_code: {query_code}")
        result = metta.run(query_code)
        parsed_result = parse_and_serialize(str(result))
        # logging.debug(f"Generated result Code: {result}")
        # Return the serialized result
        return jsonify({"Generated query": query_code,"Result": json.loads(parsed_result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

