from typing import List
import logging
from dotenv import load_dotenv
import neo4j
from app.services.query_generator_interface import QueryGeneratorInterface
from neo4j import GraphDatabase
import glob
import os
from neo4j.graph import Node, Relationship

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CypherQueryGenerator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'),
            auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))
        )
        # self.dataset_path = dataset_path
        # self.load_dataset(self.dataset_path)

    def close(self):
        self.driver.close()

    def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.cypher"), recursive=True)
        if not paths:
            raise ValueError(f"No .cypher files found in dataset path '{path}'.")

        # Separate nodes and edges
        nodes_paths = [p for p in paths if p.endswith("nodes.cypher")]
        edges_paths = [p for p in paths if p.endswith("edges.cypher")]

        # Helper function to process files
        def process_files(file_paths, file_type):
            for file_path in file_paths:
                logger.info(f"Start loading {file_type} dataset from '{file_path}'...")
                try:
                    with open(file_path, 'r') as file:
                        data = file.read()
                        for line in data.splitlines():
                            self.run_query(line)
                except Exception as e:
                    logger.error(f"Error loading {file_type} dataset from '{file_path}': {e}")

        # Process nodes and edges files
        process_files(nodes_paths, "nodes")
        process_files(edges_paths, "edges")

        logger.info(f"Finished loading {len(nodes_paths)} nodes and {len(edges_paths)} edges datasets.")

    def run_query(self, query_code, limit, apply_limit=True):
        if isinstance(query_code, list):
            query_code = query_code[0]

        if apply_limit:
            try:
                curr_limit = min(5000, int(limit)) # TODO: Find a better way for the max limit
            except (ValueError, TypeError):
                curr_limit = 5000

            query_code += f"\nLIMIT {curr_limit}"
        
        with self.driver.session() as session:
            results = session.run(query_code)
            return list(results)

     

    def query_Generator(self, requests,node_map):
        nodes = requests["nodes"]
        predicates = requests["predicates"]
        logic = requests["logic"]

        cypher_queries = []
        match_clauses = []
        where_clauses = []
        return_clauses = []
        match_no_preds=[]
        where_no_preds=[]
        return_no_preds=[]
        node_ids = set() 
        node_map={}
         # Track all node variables

        # Process nodes and construct MATCH and WHERE clauses
        for node in nodes:
            var_name = f"n_{node['node_id']}"
            match_clauses.append(self.match_node(node, var_name))
            where_clauses.extend(self.where_construct(node, var_name))
            return_clauses.append(var_name)
            node_ids.add(var_name)
            node_map[node['node_id']]=node
        # Process predicates and relationships
        relationship_clauses = []
        for i, predicate in enumerate(predicates):
            source_var = f"n_{predicate['source']}"
            target_var = f"n_{predicate['target']}"
            rel_var = f"r{i}"

            relationship_clauses.append(f"({source_var})-[{rel_var}:{predicate['type'].replace(' ', '_').lower()}]->({target_var})")
            return_clauses.append(rel_var)

        # Incorporate logic conditions
        if logic:
            logic_condition = self.construct_logic(logic, predicates)
            where_clauses.append(logic_condition)

        # Construct the main query
        match_part = ", ".join(match_clauses + relationship_clauses)
        where_part = " AND ".join(where_clauses)
        return_part = ", ".join(return_clauses)
        query = f"MATCH {match_part} WHERE {where_part} RETURN {return_part}"
        
        for node_id, node in node_map:
            if node_id not in node_ids:
                var_name=f"n_{node_id}"
                match_no_preds.append(self.match_node(node,var_name))
                where_no_preds.extend(self.where_construct(node,var_name))
                return_no_preds.append(var_name)
                return_clauses(var_name)
        if match_no_preds:

        # Include UNION query for standalone nodes if necessary
            standalone_match = ", ".join(i for i in match_no_preds)
            standalone_return = ", ".join([i for i in  return_no_preds])
            standalone_where=", ".join([i for i in where_no_preds])
            query += f" UNION MATCH {standalone_match} WHERE {standalone_where} RETURN {standalone_return}"

        cypher_queries.append(query)

        print("--------------------------------------Cypher Queries------------------")
        print(cypher_queries)
        return cypher_queries
     
    def match_node(self, node, var_name):
        if node['id']:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        return f"({var_name}:{node['type']})"

    def where_construct(self, node, var_name):
        properties = []
        for key, value in node.get('properties', {}).items():
            properties.append(f"{var_name}.{key} =~ '(?i){value}'")
        return properties
   
    def construct_logic(self, logic, predicates):
        def build_condition(logic_node):
            operator = logic_node.get("operator")
            if operator == "AND":
                return " AND ".join([self.predicate_to_condition(p, predicates) for p in logic_node["predicates"]])
            elif operator == "OR":
                return " OR ".join([self.predicate_to_condition(p, predicates) for p in logic_node["predicates"]])
            elif operator == "NOT":
                node_ids = logic_node["nodes"]  # Assuming it's a list
                return " AND ".join([f"NOT (n_{node_id})" for node_id in node_ids])
            return ""

        conditions = [build_condition(child) for child in logic.get("children", [])]
        return f"({' AND '.join(conditions)})"


    def predicate_to_condition(self, predicate_id, predicates):
        predicate = next(p for p in predicates if p["id"] == predicate_id)
        source_var = f"n_{predicate['source']}"
        target_var = f"n_{predicate['target']}"
        predicate_type = predicate['type'].replace(" ", "_").lower()
        return f"({source_var})-[:{predicate_type}]->({target_var})"

    def parse_neo4j_results(self, results, all_properties):
        (nodes, edges, _, _) = self.process_result(results, all_properties)
        return {"nodes": nodes, "edges": edges}

    def parse_and_serialize(self, input, schema, all_properties):
        parsed_result = self.parse_neo4j_results(input, all_properties)
        return parsed_result["nodes"], parsed_result["edges"]

    def convert_to_dict(self, results, schema):
        (_, _, node_dict, edge_dict) = self.process_result(results, True)
        return (node_dict, edge_dict)

    def process_result(self, results, all_properties):
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()

        named_types = ['gene_name', 'transcript_name', 'protein_name', 'pathway_name', 'term_name']

        for record in results:
            for item in record.values():
                if isinstance(item, neo4j.graph.Node):
                    node_id = f"{list(item.labels)[0]} {item['id']}"
                    if node_id not in node_dict:
                        node_data = {
                            "data": {
                                "id": node_id,
                                "type": list(item.labels)[0],
                            }
                        }

                        for key, value in item.items():
                            if all_properties:
                                if key != "id" and key != "synonyms":
                                    node_data["data"][key] = value
                            else:
                                if key in named_types:
                                    node_data["data"]["name"] = value
                        if "name" not in node_data["data"]:
                            node_data["data"]["name"] = node_id
                        nodes.append(node_data)
                        if node_data["data"]["type"] not in node_type:
                            node_type.add(node_data["data"]["type"])
                            node_to_dict[node_data['data']['type']] = []
                        node_to_dict[node_data['data']['type']].append(node_data)
                        node_dict[node_id] = node_data
                elif isinstance(item, neo4j.graph.Relationship):
                    source_id = f"{list(item.start_node.labels)[0]} {item.start_node['id']}"
                    target_id = f"{list(item.end_node.labels)[0]} {item.end_node['id']}"
                    edge_data = {
                        "data": {
                            # "id": item.id,
                            "label": item.type,
                            "source": source_id,
                            "target": target_id,
                        }
                    }
                    temp_relation_id = f"{source_id} - {item.type} - {target_id}"
                    if temp_relation_id in visited_relations:
                        continue
                    visited_relations.add(temp_relation_id)

                    for key, value in item.items():
                        if key == 'source':
                            edge_data["data"]["source_data"] = value
                        else:
                            edge_data["data"][key] = value
                    edges.append(edge_data)
                    if edge_data["data"]["label"] not in edge_type:
                        edge_type.add(edge_data["data"]["label"])
                        edge_to_dict[edge_data['data']['label']] = []
                    edge_to_dict[edge_data['data']['label']].append(edge_data)
    
        return (nodes, edges, node_to_dict, edge_to_dict)

    def parse_id(self, request):
        nodes = request["nodes"]
        named_types = {"gene": "gene_name", "transcript": "transcript_name"}
        prefixes = ["ensg", "enst"]
 
        for node in nodes:
            is_named_type = node['type'] in named_types
            id = node["id"].lower()
            is_name_as_id = all(not id.startswith(prefix) for prefix in prefixes)
            no_id = node["id"] != ''
            if is_named_type and is_name_as_id and no_id:
                node_type = named_types[node['type']]
                node['properties'][node_type] = node["id"]
                node['id'] = ''
            node["id"] = node["id"].lower()
        return request
    




 