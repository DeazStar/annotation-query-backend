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

    def run_query(self, query_code):
        if isinstance(query_code, list):
            query_code = query_code[0]
        with self.driver.session() as session:
            results = session.run(query_code)
            result_list = [record for record in results]
            return result_list

    def query_Generator(self, requests, node_map):
        nodes = requests['nodes']

        if "predicates" in requests:
            predicates = requests["predicates"]
        else:
            predicates = None

        cypher_queries = []
        # node_dict = {node['node_id']: node for node in nodes}

        match_preds = []
        return_preds = []
        match_no_preds = []
        return_no_preds = []
        node_ids = set()
        # Track nodes that are included in relationships
        used_nodes = set()
        if not predicates:
            # Case when there are no predicates
            for node in nodes:
                var_name = f"n_{node['node_id']}"
                match_no_preds.append(self.match_node(node, var_name))
                return_no_preds.append(var_name)
            cypher_query = self.construct_clause(match_no_preds, return_no_preds)
            cypher_queries.append(cypher_query)
        else:
            for i, predicate in enumerate(predicates):
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]
                source_var = source_node['node_id']
                target_var = target_node['node_id']

                source_match = self.match_node(source_node, source_var)
                match_preds.append(source_match)
                target_match = self.match_node(target_node, target_var)

                match_preds.append(f"({source_var})-[r{i}:{predicate_type}]->{target_match}")
                return_preds.append(f"r{i}")

                used_nodes.add(predicate['source'])
                used_nodes.add(predicate['target'])
                node_ids.add(source_var)
                node_ids.add(target_var)

            for node_id, node in node_map.items():
                if node_id not in used_nodes:
                    var_name = f"n_{node_id}"
                    match_no_preds.append(self.match_node(node, var_name))
                    return_no_preds.append(var_name)

            return_preds.extend(list(node_ids))
                
            if (len(match_no_preds) == 0):
                cypher_query = self.construct_clause(match_preds, return_preds)
                cypher_queries.append(cypher_query)
            else:
                cypher_query = self.construct_union_clause(match_preds, return_preds, match_no_preds, return_no_preds)
                cypher_queries.append(cypher_query)
        return cypher_queries
    
    def construct_clause(self, match_clause, return_clause):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        query = f"{match_clause} {return_clause}"
        return query

    def construct_union_clause(self, match_preds, return_preds, match_no_preds, return_no_preds):
        match_preds = f"MATCH {', '.join(match_preds)}"
        tmp_return_preds = return_preds
        return_preds = f"RETURN {', '.join(return_preds)} , null AS {', null AS '.join(return_no_preds)}"
        match_no_preds = f"MATCH {', '.join(match_no_preds)}"
        return_no_preds = f"RETURN  {', '.join(return_no_preds)} , null AS {', null AS '.join(tmp_return_preds)}"
        query = f"{match_preds} {return_preds} UNION {match_no_preds} {return_no_preds}"
        return query

    def match_node(self, node, var_name):
        if node['id']:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        elif node['properties']:
            properties = ", ".join([f"{k}: '{v}'" for k, v in node['properties'].items()])
            return f"({var_name}:{node['type']} {{{properties}}})"
        else:
            return f"({var_name}:{node['type']})"

    def parse_neo4j_results(self, results):
        (nodes, edges, _, _) = self.process_result(results)
        return {"nodes": nodes, "edges": edges}

    def parse_and_serialize(self, input, schema):
        parsed_result = self.parse_neo4j_results(input)
        return parsed_result["nodes"], parsed_result["edges"]

    def convert_to_dict(self, results, schema):
        (_, _, node_dict, edge_dict) = self.process_result(results)
        return (node_dict, edge_dict)

    def process_result(self, results):
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()

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
                            if key != "id" and key!= "synonyms":
                                node_data["data"][key] = value
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
    
    def parse_id(self, requests, node_map):
        id_guide = {
                    # "abc-regulatory_region": "rs10000009",
                    # "caad-sequence_variant": "rs10",
                    # "dbsuper-super_enhancer": "chr1_119942741_120072458_GRCh38",
                    # "dbvar-structural_variant": "nssv16889290",
                    # "dgv-structural_variant": "chr1_10002_22119_GRCh38",
                    # "epd-promoter": "chr1_959246_959306_GRCh38",
                    "exon": lambda s : s.lower(),
                    "gene": lambda s : s.lower(),
                    "transcript": lambda s : s.lower(),
                    "motif": lambda s : s.lower(),
                    # "ontology-ontology_term": "GO:0000001",
                    # "peregrine-enhancer": "chr1_99534632_99534837_GRCh38",
                    # "reactome-pathway": "R-HSA-164843",
                    # "rna_central-non_coding_rna": "URS000035F234",
                    # "regulatory_region": "rs10000007",
                    # "tadmap-tad": "chr1_800000_1350000_GRCh38",
                    # "uniport-protein": "Q9NU02"
                }
        
        for node in requests['nodes']:
            if node['id'] != '' and node["type"] in id_guide.keys():
                node['id'] = id_guide[node["type"]](node["id"])
        
        for node in node_map.values():
            if node['id'] != '' and node["type"] in id_guide.keys():
                node['id'] = id_guide[node["type"]](node['id'])

        # print('reqests:\t', requests, '\n', 'node_map:\t', node_map)
        return requests, node_map