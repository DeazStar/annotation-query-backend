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

    def run_query(self, query_code, run_count=True):
        results = []
        if isinstance(query_code, list):
            find_query = query_code[0]
            total_count_query = query_code[1]
            label_count_query = query_code[2]
        else:
            find_query = query_code
            total_count_query = None
            label_count_query = None
        
        with self.driver.session() as session:
            results.append(list(session.run(find_query)))
        if run_count:
            if total_count_query:
                try:
                    with self.driver.session() as session:
                        results.append(list(session.run(total_count_query)))
                except:
                    results.append([])
            if label_count_query:
                try:
                    with self.driver.session() as session:
                        results.append(list(session.run(label_count_query)))
                except:
                    results.append([])
                return results

        return results

    def query_Generator(self, requests, node_map, limit=None, node_only=False):
        nodes = requests['nodes']
        predicate_map = {}

 
        if "predicates" in requests and len(requests["predicates"]) > 0:
            predicates = requests["predicates"]

            init_pred = predicates[0]

            if 'predicate_id' not in init_pred:
                for idx, pred in enumerate(predicates):
                    pred['predicate_id'] = f'p{idx}'
                for predicate in predicates:
                    predicate_map[predicate['predicate_id']] = predicate
            else:
                for predicate in predicates:
                    predicate_map[predicate['predicate_id']] = predicate
 
        if "predicates" in requests:
            predicates = requests["predicates"]
            for predicate in predicates:
                predicate_map[predicate['predicate_id']] = predicate
 
        else:
            predicates = None

        cypher_queries = []
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
        if not predicates:
            list_of_node_ids = []
            # Case when there are no predicates
            for node in nodes:
                var_name = f"{node['node_id']}"
                match_no_preds.append(self.match_node(node, var_name))
                if node['properties']:
                    where_no_preds.extend(self.where_construct(node, var_name))
                return_no_preds.append(var_name)
                list_of_node_ids.append(var_name)
            if node_only:
                cypher_query = self.construct_optional_clause(match_no_preds, return_no_preds, where_no_preds, limit)
            else:
                cypher_query = self.construct_clause(match_no_preds, return_no_preds, where_no_preds, limit)
            cypher_queries.append(cypher_query)
            query_clauses = {
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "predicates": predicates
                }
            count = self.construct_count_clause(query_clauses, node_map, predicate_map)
            cypher_queries.extend(count)
        else:
            for predicate in predicates:
                predicate_id = predicate['predicate_id']
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

                match_preds.append(f"({source_var})-[{predicate_id}:{predicate_type}]->{target_match}")
                return_preds.append(predicate_id)

                used_nodes.add(predicate['source'])
                used_nodes.add(predicate['target'])
                node_ids.add(source_var)
                node_ids.add(target_var)

            for node_id, node in node_map.items():
                if node_id not in used_nodes:
                    var_name = f"{node_id}"
                    match_no_preds.append(self.match_node(node, var_name))
                    where_no_preds.extend(self.where_construct(node, var_name))
                    return_no_preds.append(var_name)

            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()
            full_return_preds = return_preds + list_of_node_ids
                
            if (len(match_no_preds) == 0):
                cypher_query = self.construct_clause(match_preds, full_return_preds, where_preds, limit)
                cypher_queries.append(cypher_query)

                query_clauses = {
                    "match_preds": match_preds, 
                    "full_return_preds": full_return_preds,
                    "where_preds": where_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "return_preds": return_preds,
                    "predicates": predicates
                }
                count = self.construct_count_clause(query_clauses, node_map, predicate_map)
                cypher_queries.extend(count)
            else:
                query_clauses = {
                    "match_preds": match_preds, 
                    "full_return_preds": full_return_preds,
                    "where_preds": where_preds,
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                    "return_preds": return_preds,
                    "predicates": predicates
                }
                cypher_query = self.construct_union_clause(query_clauses, limit)
                cypher_queries.append(cypher_query)

                count = self.construct_count_clause(query_clauses, node_map, predicate_map)
                cypher_queries.extend(count)
        return cypher_queries
    
    def construct_clause(self, match_clause, return_clause, where_no_preds, limit):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{match_clause} {where_clause} {return_clause} {self.limit_query(limit)}"
        return f"{match_clause} {return_clause} {self.limit_query(limit)}"
    
    def construct_optional_clause(self, match_clause, return_clause, where_no_preds, limit):
        optional_clause = ""

        for match in match_clause:
            optional_clause += f"OPTIONAL MATCH {match} "

        return_clause = f"RETURN {', '.join(return_clause)}"
        if len(where_no_preds) > 0:
            where_clause = f"WHERE {' AND '.join(where_no_preds)}"
            return f"{optional_clause} {where_clause} {return_clause} {self.limit_query(limit)}"
        return f"{optional_clause} {return_clause} {self.limit_query(limit)}"

    def construct_union_clause(self, query_clauses, limit):
        match_no_clause = ''
        where_no_clause = ''
        return_count_no_preds_clause = ''
        match_clause = ''
        where_clause = ''
        return_count_preds_clause = ''

        # Check and construct clause for match with no predicates
        if 'match_no_preds' in query_clauses and query_clauses['match_no_preds']:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if 'where_no_preds' in query_clauses and query_clauses['where_no_preds']:
                where_no_clause = f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"
            return_count_no_preds_clause = "RETURN " + ', '.join(query_clauses['return_no_preds'])

        # Construct a clause for match with predicates
        if 'match_preds' in query_clauses and query_clauses['match_preds']:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if 'where_preds' in query_clauses and query_clauses['where_preds']:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"
            return_count_preds_clause = "RETURN " + ', '.join(query_clauses['full_return_preds'])

        clauses = {}

        # Update the query_clauses dictionary with the constructed clauses
        clauses['match_no_clause'] = match_no_clause
        clauses['where_no_clause'] = where_no_clause
        clauses['return_no_clause'] = return_count_no_preds_clause
        clauses['match_clause'] = match_clause
        clauses['where_clause'] = where_clause
        clauses['return_clause'] = return_count_preds_clause
        
        query = self.construct_call_clause(clauses, limit)
        return query

    def construct_count_clause(self, query_clauses, node_map, predicate_map):
        match_no_clause = ''
        where_no_clause = ''
        match_clause = ''
        where_clause = ''
        return_preds = []
        collect_node_and_edge = ''

        # Construct clause for match with no predicates
        if 'match_no_preds' in query_clauses and query_clauses['match_no_preds']:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if 'where_no_preds' in query_clauses and query_clauses['where_no_preds']:
                where_no_clause = f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"

        # Construct clause for match with predicates
        if 'match_preds' in query_clauses and query_clauses['match_preds']:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if 'where_preds' in query_clauses and query_clauses['where_preds']:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"

        if "return_no_preds" in query_clauses and "return_preds" in query_clauses:
            query_clauses['list_of_node_ids'].extend(query_clauses['return_no_preds'])

        if "return_preds" in query_clauses:
            return_preds = query_clauses['return_preds']

        for node_ids in query_clauses['list_of_node_ids']:
            collect_node_and_edge += f"COLLECT(DISTINCT {node_ids}) AS {node_ids}_count, "
        
        if "return_preds" in query_clauses:
            for predicate in query_clauses['predicates']:
                predicate_id = predicate['predicate_id']
                collect_node_and_edge += f"COLLECT(DISTINCT {predicate_id}) AS {predicate_id}_count, "
        collect_node_and_edge = f"WITH {collect_node_and_edge.rstrip(', ')}"


        # Construct the WITH and UNWIND clauses
        combined_nodes = ' + '.join([f"{var}_count" for var in query_clauses['list_of_node_ids']])
        combined_edges = None
        if 'return_preds' in query_clauses:
            combined_edges = ' + '.join([f"{var}_count" for var in query_clauses['return_preds']])
        with_clause = f"WITH {combined_nodes} AS combined_nodes {f',{combined_edges} AS combined_edges' if combined_edges else ''}"
        unwind_clause = f"UNWIND combined_nodes AS nodes"

        # Construct the RETURN clause
        return_clause = f"RETURN COUNT(DISTINCT nodes) AS total_nodes {', SIZE(combined_edges) AS total_edges ' if combined_edges else ''}"


        # build the query for total node and edge count
        total_count = f'''
            {match_no_clause}
            {where_no_clause}
            {match_clause}
            {where_clause}
            {collect_node_and_edge}
            {with_clause}
            {unwind_clause}
            {return_clause}
        '''

        # start building query for counting by label for both ndoe and edges

        if return_preds:
        # count query
            count_clause = ''
            for node in query_clauses['list_of_node_ids']:
                count_clause += f"COUNT(DISTINCT {node}) AS {node}_{node_map[node]['type']}, "
            for edge in query_clauses['predicates']:
                edge_id = edge['predicate_id']
                count_clause += f"COUNT(DISTINCT {edge_id}) AS {edge_id}_{predicate_map[edge_id]['type'].replace(' ', '_')}, "
            return_clause = "RETURN " + count_clause.rstrip(', ')
            label_count_query = f'''{match_no_clause} {where_no_clause} {match_clause} {where_clause} {return_clause}'''
        else:
            count_clause = ''
            for node in query_clauses['list_of_node_ids']:
                count_clause += f"COUNT(DISTINCT {node}) AS {node}_{node_map[node]['type']}, "
            return_clause = "RETURN " + count_clause.rstrip(', ')
            label_count_query = f'''{match_no_clause} {where_no_clause} {return_clause}'''

        return [total_count, label_count_query]


    def limit_query(self, limit):
        if limit:
            curr_limit = min(1000, int(limit))
        else:
            curr_limit = 1000
        return f"LIMIT {curr_limit}"

    def construct_call_clause(self, clauses, limit=None):
        if not ("match_no_clause" in clauses or "match_clause" in clauses):
            raise Exception("Either 'match_clause' or 'match_no_clause' must be present")

        # Build CALL clauses
        call_clauses = []

        # For both nodes without predicate and with predicate
        if "match_no_clause" in clauses and clauses["match_no_clause"]:
            call_clauses.append(
                f'CALL() {{ {clauses["match_no_clause"]} '
                f'{clauses.get("where_no_clause", "")} '
                f'{clauses["return_no_clause"]} '
                f'{self.limit_query(limit) if "return_count_sum" not in clauses else ""} }}'
            )

        if "match_clause" in clauses and clauses["match_clause"]:
            call_clauses.append(
                f'CALL() {{ {clauses["match_clause"]} '
                f'{clauses.get("where_clause", "")} '
                f'{clauses["return_clause"]} '
                f'{self.limit_query(limit) if "return_count_sum" not in clauses else ""} }}'
            )

        # Add any additional return clause sum/normal query
        final_clause = clauses.get("return_count_sum", "RETURN *")
        call_clauses.append(final_clause)

        # Combine clauses into a single string
        return " ".join(call_clauses)



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

    def parse_neo4j_results(self, results, graph_components):
 
        (nodes, edges, _, _, meta_data) = self.process_result(results, graph_components)
        return {"nodes": nodes, "edges": edges, "node_count": meta_data['node_count'], 
                "edge_count": meta_data['edge_count'], "node_count_by_label": meta_data['node_count_by_label'], 
                "edge_count_by_label": meta_data['edge_count_by_label']
                }

    def parse_and_serialize(self, input, schema, graph_components):
        parsed_result = self.parse_neo4j_results(input, graph_components)
        return parsed_result

    def convert_to_dict(self, results, schema, graph_components):
 
        match_result=results[0]
        graph_components['properties'] = True
        (_, _, node_dict, edge_dict, _) = self.graph_result_nodes(match_result, graph_components)
        return (node_dict, edge_dict)

   


    def graph_result_nodes(self, match_result, properties):
        """Extracts nodes and edges from the match result."""
 
        properties['properties'] = True
        print("i have passed")
        (_, _, node_dict, edge_dict, _) = self.process_result(match_result, properties)
        return (node_dict, edge_dict)

    def process_result(self, match_results,properties):
        match_result = match_results
        node_count_by_label = []
        edge_count_by_label = []
        node_count = 0
        edge_count = 0
        node_and_edge_count = []
        count_by_label = []

        if len(match_results) > 2:
            node_and_edge_count = match_results[1]
        if len(match_results) > 1:
            count_by_label = match_results[2]

 
        nodes = []
        edges = []
        node_dict = {}
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        visited_relations = set()
 
        named_types = ['gene_name', 'transcript_name', 'protein_name', 'pathway_name', 'term_name']

        for record in match_result:
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
                            if properties['properties']:
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
                    source_label = list(item.start_node.labels)[0]
                    target_label = list(item.end_node.labels)[0]
                    source_id = f"{list(item.start_node.labels)[0]} {item.start_node['id']}"
                    target_id = f"{list(item.end_node.labels)[0]} {item.end_node['id']}"
                    edge_data = {
                        "data": {
 
                            "edge_id": f"{source_label}_{item.type}_{target_label}",
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
 
        request_data={'nodes':nodes,'edges':edges}

        return request_data

    def count_by_label(self, count_by_label, properties):
        """Computes node and edge count by label."""
        node_count_by_label = []
        edge_count_by_label = []

        if not count_by_label:
            return node_count_by_label, edge_count_by_label

        node_count_aggregate = {}
        edge_count_aggregate = {}

        for node in properties['nodes']:
            node_type = node['type']
            node_count_aggregate[node_type] = {'count': 0}

        for predicate in properties['predicates']:
            edge_type = predicate['type'].replace(" ", "_").lower()
            edge_count_aggregate[edge_type] = {'count': 0}

        for count_record in count_by_label:
            for key, value in count_record.items():
                node_type_key = '_'.join(key.split('_')[1:])
                if node_type_key in node_count_aggregate:
                    node_count_aggregate[node_type_key]['count'] += value

                edge_type_key = '_'.join(key.split('_')[1:])
                if edge_type_key in edge_count_aggregate:
                    edge_count_aggregate[edge_type_key]['count'] += value

        for key, value in node_count_aggregate.items():
            node_count_by_label.append({'label': key, 'count': value['count']})

        for key, value in edge_count_aggregate.items():
            edge_count_by_label.append({'label': key, 'count': value['count']})

        return node_count_by_label, edge_count_by_label

    def count_node_edges(self, node_and_edge_count):
        """Computes total node and edge count."""
        node_count = 0
        edge_count = 0
 
        if node_and_edge_count:
            for count_record in node_and_edge_count:
                node_count += count_record.get('total_nodes', 0)
                edge_count += count_record.get('total_edges', 0)

 
        return node_count, edge_count

    # def process_result(self, results, graph_components):
    #     """Processes the results and returns structured graph data."""
    #     match_result = results[0]
    #     node_and_edge_count = results[1] if len(results) > 2 else []
    #     count_by_label = results[2] if len(results) > 1 else []

    #     nodes, edges, node_to_dict, edge_to_dict = self.graph_result_nodes(match_result, graph_components)
    #     node_count_by_label, edge_count_by_label = self.count_by_label(count_by_label, graph_components)
    #     node_count, edge_count = self.count_node_edges(node_and_edge_count)
 
    #     if count_by_label:
    #         # build edge type set
    #         node_count_aggregate = {}
    #         ege_count_aggregate = {}

    #         # initialize node count aggreate dictionary where the key is the label.
    #         for node in graph_components['nodes']:
    #             node_type = node['type']
    #             node_count_aggregate[node_type] = {'count': 0}

    #         # initialize edge count aggreate dictionary where the key is the label.
    #         for predicate in graph_components['predicates']:
    #             edge_type = predicate['type'].replace(" ", "_").lower()
    #             ege_count_aggregate[edge_type] = {'count': 0}

    #         for count_record in count_by_label:
    #             # update node count aggregate dictionary with the count of each label
    #             for key, value in count_record.items():
    #                 node_type_key = '_'.join(key.split('_')[1:])
    #                 if node_type_key in node_count_aggregate:
    #                     node_count_aggregate[node_type_key]['count'] += value

    #             # update edge count aggregate dictionary with the count of each label
    #             for key, value in count_record.items():
    #                 edge_type_key = '_'.join(key.split('_')[1:])
    #                 if edge_type_key in ege_count_aggregate:
    #                     ege_count_aggregate[edge_type_key]['count'] += value

    #             # update the way node count by label and edge count by label are represented
    #             for key, value in node_count_aggregate.items():
    #                 node_count_by_label.append({'label': key, 'count': value['count']})
    #             for key, value in ege_count_aggregate.items(): 
    #                 edge_count_by_label.append({'label': key, 'count': value['count']})
 
    #     meta_data = {
    #         "node_count": node_count,
    #         "edge_count": edge_count,
    #         "node_count_by_label": node_count_by_label,
    #         "edge_count_by_label": edge_count_by_label
    #     }
 

    #     return nodes, edges, node_to_dict, edge_to_dict, meta_data

 
    
    #     return (nodes, edges, node_to_dict, edge_to_dict, meta_data)

 
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