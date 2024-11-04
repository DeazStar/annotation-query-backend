from typing import List
import logging
from dotenv import load_dotenv
import neo4j
from app.services.query_generator_interface import QueryGeneratorInterface
from neo4j import GraphDatabase
import glob
import os
from neo4j.graph import Node, Relationship
from typing import List, Union

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

    def run_query(self, query_codes, limit):
        results = []
        if isinstance(query_codes, list):
            query_code = query_codes[0]
            count_code = query_codes[1]
        try:
            curr_limit = min(5000, int(limit)) # TODO: Find a better way for the max limit
        except (ValueError, TypeError):
            curr_limit = 5000

        query_code += f"\nLIMIT {curr_limit}"
        
        with self.driver.session() as session:
            results.append(list(session.run(query_code)))
        
        with self.driver.session() as session:
            results.append(list(session.run(count_code)))
        
        return results

    def query_Generator(self, requests, node_map):
        nodes = requests['nodes']
        if "predicates" in requests:
            predicates = requests["predicates"]
        else:
            predicates = None

        # node_dict = {node['node_id']: node for node in nodes}

        match_preds = []
        return_preds = []
        where_preds = []
        match_no_preds = []
        return_no_preds = []
        where_no_preds = []
        node_ids = set()
        # Track nodes that are included in relationships
        used_nodes = set()
        cypher_queries = []
        if not predicates:
            # Case when there are no predicates
            for node in nodes:
                var_name = f"n_{node['node_id']}"
                match_no_preds.append(self.match_node(node, var_name))
                if node['properties']:
                    where_no_preds.extend(self.where_construct(node, var_name))
                return_no_preds.append(var_name)
            cypher_query = self.construct_clause(match_no_preds, return_no_preds, where_no_preds)
            cypher_queries.append(cypher_query)
        else:
            for i, predicate in enumerate(predicates):
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]
                source_var = source_node['node_id']
                target_var = target_node['node_id']

                source_match = self.match_node(source_node, source_var)
                where_preds.extend(self.where_construct(source_node, source_var))
                match_preds.append(source_match)
                target_match = self.match_node(target_node, target_var)
                where_preds.extend(self.where_construct(target_node, target_var))

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
                    where_no_preds.extend(self.where_construct(node, var_name))
                    return_no_preds.append(var_name)

            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()
            return_preds.extend(list(list_of_node_ids))
                
            if (len(match_no_preds) == 0):
                cypher_query = self.construct_clause(match_preds, return_preds, where_preds)
                cypher_queries.append(cypher_query)
            else:
                cypher_query = self.construct_union_clause(match_preds, return_preds, where_preds, match_no_preds, return_no_preds, where_no_preds)
                cypher_queries.append(cypher_query)
        
        count_query = self.count_query_Generator(requests, node_map)
        return [cypher_query, count_query]
    
    def count_query_Generator(self, requests:dict, node_map:dict) -> str:
        """
        takes in a request object as dictionary and node_map dictionary from validate request
        RETURN:
            a string to be passed to neorj foor counting the number of nodes and edges returned
            Call() {
                MATCh(n0:gene)
                WHERE n0.chr =~ '(?i)chr1'
                RETURN COUNT(DISTINCT n0) AS n0_count
            }

            CALL() {
                MATCH(n1:gene)-[r0:transcibed_to]->(n2:transcript)
                WHERE n1.gene_name =~ '(?i)adef'
                RETURN COUNT(DISTINCT n1) AS n1_count,
                       COUNT(DISTINCT n2) AS n2_count
                       COUNT(DISTINCT r0) AS r0_count
            }

            RETURN n0_count + n1_count + n2_count as nodes, r0_count as edges

            N.B: nodes with id's do not use thier proproperties for matching
        """

        nodes = requests['nodes']

        if "predicates" in requests:
            predicates = requests["predicates"]
        else:
            predicates = None
    
        call_subqueries = [] # Array of each subquery
        cypher_queries = [] # Array of finalized query
        node_count = 0 # used for setting variable of nodes and edges to avoid repition when node is used multiple times 
        node_return = []
        edge_return = []
        unused_nodes = set(node_map.keys()) # a set of all keys
        
        if not predicates:
    
            for node in nodes:
                node['node_count'] = f"n{node_count}"
                node_count += 1
                query_str = self.construct_call_node(node)
                call_subqueries.append(query_str[0])
                node_return.append(query_str[1])
            
            return f"{' '.join(call_subqueries)} RETURN {' + '.join(node_return)} AS node_count" 
    
        for index, edge in enumerate(predicates):
    
            source_node = node_map[edge['source']]
            source_node['node_count'] = f"n{node_count}"
            node_count += 1
    
            target_node = node_map[edge['target']] 
            target_node['node_count'] = f"n{node_count}"
            node_count += 1
            
            edge = {'type': edge['type'].replace(' ','_'), 'variable': f'r{index}'}
            
            query_str = self.construct_call_edge(source_node, target_node, edge)
            
            call_subqueries.append(query_str[0])
            node_return.append(query_str[1][0])
            node_return.append(query_str[1][1])
            edge_return.append(query_str[1][2])
            
            # remove source and target node ids from unused set
            if source_node['node_id'] in unused_nodes:
                unused_nodes.remove(source_node['node_id'])
            
            if target_node['node_id'] in unused_nodes:
                unused_nodes.remove(target_node['node_id'])
    
        for node_id in unused_nodes:
            node = node_map[node_id]
            query_str = self.construct_call_node(node)
            call_subqueries.append(query_str[0])
            node_return.append(query_str[1])
                
        return f"{' '.join(call_subqueries)} RETURN {' + '.join(node_return)} AS node_count, {' + '.join(edge_return)} AS edge_count" 

    def construct_call_node(self, node: dict) -> List[str]:
        """
            takes in a node as an argument and returns a string and the return of the call subquery
            node: -> dict 
                a dict with same structure as input request node
            RETURN:
                at index 0 => stirng of type
                    CALl () {
                       MATCH (n1:gene)
                       WHERE n1.chr =~ '(?i)chr1'
                       RETURN COUNT(DISTINCT n1) AS n1_count
                    }
                at index 1 => string of type
                    n1_count
    
        """    
    
        node_id_type = f"{node['node_count']}: {node['type']}" # creates n1:gene for gene node with node_id n1
        where_properties = [] # list of propeorties 
        return_variable = f"{node['node_count']}_count" # for n1 n1_count
        return_mask = f"COUNT(DISTINCT {node['node_count']}) AS {return_variable}"
        
        if node['id'] != '': # if the node has id key donot add checks against propoerty
            node_identifier = f"id:'{node['id']}'"
            return  [f"MATCH ({node_id_type}) {{{node_identifier}}}) RETURN {return_mask}", return_variable]
        
        for key, value in node['properties'].items(): # construct case insensitve where filter for properties
            where_properties.append(f"{node['node_count']}.{key} =~ '(?i){value}'")
        
        if len(where_properties) > 0:
               return  [f"CALL () {{ MATCH ({node_id_type}) WHERE {' AND '.join(where_properties)} RETURN {return_mask} }}", return_variable]
        
        return [f"CALL () {{ MATCH ({node_id_type}) RETURN {return_mask} }}", return_variable]
    
    def construct_call_edge(self, source_node, target_node, edge) -> List[Union[str, List[str]]]:
        """
            takes in a two nodes and the relationship between them  as an argument and returns a string and a list of the names of the returns from the subquery
            source: -> dict 
                a dict with same structure as input request node
            target: -> dict
                same structure as input request node
            edge: -> dict
                with keys type -> type of the edge and  variable -> variable to be given to edge
            RETURN:
                at index 0 => stirng of type
                    CALl () {
                       MATCH (n1:gene)-[r0:transcribed_to]->(n2:transcript)
                       WHERE n1.chr =~ '(?i)chr1'
                       RETURN COUNT(DISTINCT n1) AS n1_count, COUNT(DISTINCT n2) AS n2_count, COUNT(DISTINCT r0) AS r0_count
                    }
                at index 1  => List  with values
                    [n1_count, n2_count, r0_count]
        """
        source_node_id_type = f"{source_node['node_count']}:{source_node['type']}"
        target_node_id_type = f"{target_node['node_count']}:{target_node['type']}"
        edge_identifier = f"[{edge['variable']}:{edge['type']}]"
        return_variables = [f"{source_node['node_count']}_count", f"{target_node['node_count']}_count", f"{edge['variable']}_count"]
        return_mask = f"COUNT (DISTINCT {source_node['node_count']}) AS {return_variables[0]}, COUNT (DISTINCT {target_node['node_count']}) AS {return_variables[1]}, COUNT(DISTINCT {edge['variable']}) AS {return_variables[2]}"
        where_properties = [] 
        
        # if source node has id create it as (n1:gene {id:'ENSG000123'}) and set propoerties key to empty dict
        if source_node['id'] != '':
            source_node_identifier = f"({source_node_id_type} {{id: '{source_node['id']}'}})"
            source_node['properties'] = {}
        else:
            source_node_identifier = f"({source_node_id_type})"
        
        if target_node['id'] != '':
            target_node_identifier = f"({target_node_id_type} {{id: '{target_node['id']}'}})"
            target_node['properties'] = {}
        else:
            target_node_identifier = f"({target_node_id_type})"
    
        for key, value in source_node['properties'].items():
            where_properties.append(f"{source_node['node_count']}.{key} =~ '(?i){value}'")
    
        for key, value in target_node['properties'].items():
            where_properties.append(f"{target_node['node_count']}.{key} =~ '(?i){value}'")
    
        if len(where_properties) > 0:
            return [f"CALL () {{ MATCH {source_node_identifier}-{edge_identifier}->{target_node_identifier} WHERE {' AND  '.join(where_properties)} RETURN {return_mask} }}", return_variables]
    
        return [f"CALL () {{ MATCH {source_node_identifier}-{edge_identifier}->{target_node_identifier} RETURN {return_mask} }}", return_variables]

    def construct_clause(self, match_clause, return_clause, where_no_preds):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{match_clause} {where_clause} {return_clause}"
        return f"{match_clause} {return_clause}"

    def construct_union_clause(self, match_preds, return_preds, where_preds ,match_no_preds, return_no_preds, where_no_preds):
        where_clause = ""
        where_no_clause = ""
        match_preds = f"MATCH {', '.join(match_preds)}"
        tmp_return_preds = return_preds
        return_preds = f"RETURN {', '.join(return_preds)} , null AS {', null AS '.join(return_no_preds)}"
        if len(where_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_preds)}"
        match_no_preds = f"MATCH {', '.join(match_no_preds)}"
        return_no_preds = f"RETURN  {', '.join(return_no_preds)} , null AS {', null AS '.join(tmp_return_preds)}"
        if len(where_no_preds) > 0:
            where_no_clause = f"WHERE {' AND '.join(where_no_preds)}"
        query = f"{match_preds} {where_clause} {return_preds} UNION {match_no_preds} {where_no_clause} {return_no_preds}"
        return query

    def match_node(self, node, var_name):
        if node['id']:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        else:
            return f"({var_name}:{node['type']})"

    def where_construct(self, node, var_name):
        properties = []
        if node['id']: 
            return properties

        for key, property in node['properties'].items():
            properties.append(f"{var_name}.{key} =~ '(?i){property}'")
        return properties

    def parse_neo4j_results(self, results, all_properties):
        (nodes, edges, _, _) = self.process_result(results, all_properties)
        return {"nodes": nodes, "edges": edges}

    def parse_count_result(self, results):
        output_dict = {}
        for record in results:
            for key, value in record.items():
                output_dict[key] = value

        if 'edge_count' not in output_dict:
            output_dict['edge_count'] = 0
            
        return output_dict

            

    def parse_and_serialize(self, input, schema, all_properties):
        parsed_result = self.parse_neo4j_results(input[0], all_properties)
        parse_count = self.parse_count_result(input[1])
        return parsed_result["nodes"], parsed_result["edges"], parse_count['node_count'], parse_count['edge_count']

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
