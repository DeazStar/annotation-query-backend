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
        print(query_code)
        results = []
        if isinstance(query_code, list):
            find_query = query_code[0]
            #count_query = query_code[1]
        else:
            find_query = query_code
            count_query = None
        
        with self.driver.session() as session:
            results.append(list(session.run(find_query)))

       # if count_query:
            #with self.driver.session() as session:
                #results.append(list(session.run(count_query)))

        return results

    def query_Generator(self, requests, node_map, limit=None):
        nodes = requests['nodes']
        predicates = requests.get("predicates", [])
        logic = requests.get("logic", None)

        cypher_queries = []

        match_preds = []
        return_preds = []
        where_preds = []
        match_no_preds = []
        return_no_preds = []
        where_no_preds = []
        node_ids = set()
        # Track nodes that are included in relationships
        used_nodes = set()
        no_label_ids = None
        where_logic = None
        return_clause_or=None
        value=False

        # define a set of nodes with predicates
        node_predicates = {p['source'] for p in predicates}.union({p['target'] for p in predicates})

        predicate_map = {}

        if logic:
            for predicate in predicates:
                if predicate['predicate_id'] not in predicate_map:
                    predicate_map[predicate['predicate_id']] = predicate
                else:
                    raise Exception('Repeated predicate_id')
            where_logic, no_label_ids,return_clause_or,nodes= self.apply_boolean_operation(logic['children'], node_map, node_predicates, predicate_map)
            #return_clause_or+=f","
            
        if not predicates:
            list_of_node_ids = []
            # Case when there are no predicates
            for node in nodes:
                if node['properties']:
                    where_no_preds.extend(self.where_construct(node))
                if where_logic:

                    where_no_preds.extend(where_logic['where_no_preds'])
                
                match_no_preds.append(self.match_node(node, no_label_ids['no_node_labels'] if no_label_ids else None))
                return_no_preds.append(node['node_id'])
                list_of_node_ids.append(node['node_id'])
                query_clauses = {
                    "match_clause": match_no_preds,
                    "return_clause": return_no_preds,
                    "where_clause": where_no_preds,
                }
            cypher_query = self.construct_clause(query_clauses, limit)
            print("_____________________________final code ____________________________________")
            print(cypher_query)
            cypher_queries.append(cypher_query)
            query_clauses = {
                    "match_no_preds": match_no_preds,
                    "return_no_preds": return_no_preds,
                    "where_no_preds": where_no_preds,
                    "list_of_node_ids": list_of_node_ids,
                }
            count = self.construct_count_clause(query_clauses)
            cypher_queries.append(count)
            print("---------------------------final_code after count ____________________________________")
            print(len(cypher_queries))

        else:
            for i, predicate in enumerate(predicates):
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]
                source_var = source_node['node_id']
                target_var = target_node['node_id']

                # Common match and return part
                if logic and predicate['predicate_id'] in no_label_ids['no_predicate_labels']:
                    source_match = source_var
                    target_match = target_var
                    match_preds.append(f"({source_var})-[{predicate['predicate_id']}]->({target_var})")
                    return_preds.append(predicate['predicate_id'])
                else:
                    source_match = self.match_node(source_node)
                    where_preds.extend(self.where_construct(source_node))
                    match_preds.append(source_match)

                    target_match = self.match_node(target_node)
                    where_preds.extend(self.where_construct(target_node))

                    match_preds.append(f"({source_var})-[r{i}:{predicate_type}]->{target_match}")

                    if return_clause_or:  # Add comma if return_clause_or is not empty
                        return_clause_or += ", "
                    return_clause_or += f" r{i} AS r{i} "
                    return_preds.append(f"r{i}")



                used_nodes.add(predicate['source'])
                used_nodes.add(predicate['target'])
                node_ids.add(source_var)
                node_ids.add(target_var)

            for node_id, node in node_map.items():
                if node_id not in used_nodes:
                    match_no_preds.append(self.match_node(node))
                    where_no_preds.extend(self.where_construct(node))
                    return_no_preds.append(node_id)

            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()

            # Depending on the presence of return_clause_or, decide how to construct return_preds
            
            full_return_preds = return_preds + list(node_ids)
            
            if len(match_no_preds) == 0:
           
                
                if where_logic:
                    if where_logic['where_preds']:
                        where_preds.extend(where_logic['where_preds'])
                    
                       
                    if where_logic['where_pred_properties']:
                        where_preds=[]
                        h=list(where_logic['where_pred_properties'])[0]

                        where_preds.append(h)
                        print('++++++++++++++++=')
                        print(h)
                             

                     

                # Build query clauses
                query_clauses = {
                    'match_clause': match_preds,
                    'return_clause': full_return_preds,
                    'where_clause': where_preds
                     
                }

                # Debugging output
                print("say hi **********************************************")
                print(query_clauses['match_clause'])
                print(query_clauses['return_clause'])
                print(query_clauses['where_clause'])
             
                print("say buy *********************************88")

                # Construct Cypher query
                cypher_query = self.construct_clause(query_clauses, limit, return_clause_or)
                cypher_queries.append(cypher_query)

                # Final query clauses dictionary
                query_clauses = {
                    "match_preds": match_preds,
                    "full_return_preds": full_return_preds,
                    "where_preds": where_preds,
                    "list_of_node_ids": list(node_ids),
                    "return_preds": return_preds
                }



            else:
                if where_logic:
                    where_no_preds.extend(where_logic['where_no_preds'])
                    where_preds.extend(where_logic['where_preds'])
                
                query_clauses = {
                "match_preds": match_preds,
                "full_return_preds": full_return_preds,
                "where_preds": where_preds,
                "match_no_preds": match_no_preds,
                "return_no_preds": return_no_preds,
                "where_no_preds": where_no_preds,
                "list_of_node_ids": list(node_ids),
                "return_preds": return_preds
}

                cypher_query = self.construct_union_clause(query_clauses, limit,return_clause_or)
                cypher_queries.append(cypher_query)

                count = self.construct_count_clause(query_clauses)
                cypher_queries.append(count)
            print("----------------------------------------------**************************--------------------------------------------------")
            print(return_clause_or)
            print("_________________________________final________________________________")
            print(cypher_queries)
            print("__________________________________final _______________________________")
        return cypher_queries
            
    
    def construct_clause(self, query_clauses, limit,return_clause_or=None,):
        
        match_clause = f"MATCH {', '.join(query_clauses['match_clause'])}"
        print("----------------------------------------------before************--------------------------------------------------")
        print(return_clause_or)
        if return_clause_or !=None:
            return_clause=f"RETURN {return_clause_or}"
        else:
            return_clause = f"RETURN {', '.join(query_clauses['return_clause'])}"
        print(return_clause)
        
        if len(query_clauses['where_clause']) > 0:
            

            where_clause = f"WHERE {' AND '.join(query_clauses['where_clause'])}"
        
             
        
            return f"{match_clause} {where_clause} {return_clause} {self.limit_query(limit)}"

        print(" ______________after reterun clause *******************________________")
        print("match clause",match_clause)
        
        print("return_clause",return_clause)

        return f"{match_clause} {return_clause} {self.limit_query(limit)}"

    def construct_union_clause(self, query_clauses, limit,return_clause_or):
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
            
            if return_clause_or:
                return_count_preds_clause=f"RETURN {return_clause_or}"
            else:
                print("_______________________________hello this is return clause ____________________")
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

    def construct_count_clause(self, query_clauses):
        match_no_clause = ''
        where_no_clause = ''
        match_clause = ''
        where_clause = ''
        count_clause = ''
        with_clause = ''
        unwind_clause = ''
        return_clause = ''

        # Check and construct clause for match with no predicates
        if 'match_no_preds' in query_clauses and query_clauses['match_no_preds']:
            match_no_clause = f"MATCH {', '.join(query_clauses['match_no_preds'])}"
            if 'where_no_preds' in query_clauses and query_clauses['where_no_preds']:
                where_no_clause = f"WHERE {' AND '.join(query_clauses['where_no_preds'])}"

        # Construct a clause for match with predicates
        if 'match_preds' in query_clauses and query_clauses['match_preds']:
            match_clause = f"MATCH {', '.join(query_clauses['match_preds'])}"
            if 'where_preds' in query_clauses and query_clauses['where_preds']:
                where_clause = f"WHERE {' AND '.join(query_clauses['where_preds'])}"

        # Construct the COUNT clause
        if 'return_no_preds' in query_clauses and 'return_preds' in query_clauses:
            query_clauses['list_of_node_ids'].extend(query_clauses['return_no_preds'])
        for node_ids in query_clauses['list_of_node_ids']:
            count_clause += f"COLLECT(DISTINCT {node_ids}) AS {node_ids}_count, "
        if 'return_preds' in query_clauses:
            for edge_ids in query_clauses['return_preds']:
                count_clause += f"COLLECT(DISTINCT {edge_ids}) AS {edge_ids}_count, "
        count_clause = f"WITH {count_clause.rstrip(', ')}"


        # Construct the WITH and UNWIND clauses
        combined_nodes = ' + '.join([f"{var}_count" for var in query_clauses['list_of_node_ids']])
        combined_edges = None
        if 'return_preds' in query_clauses:
            combined_edges = ' + '.join([f"{var}_count" for var in query_clauses['return_preds']])
        with_clause = f"WITH {combined_nodes} AS combined_nodes {f',{combined_edges} AS combined_edges' if combined_edges else ''}"
        unwind_clause = f"UNWIND combined_nodes AS nodes"

        # Construct the RETURN clause
        return_clause = f"RETURN COUNT(DISTINCT nodes) AS total_nodes {', SIZE(combined_edges) AS total_edges ' if combined_edges else ''}"

        query = f'''
            {match_no_clause}
            {where_no_clause}
            {match_clause}
            {where_clause}
            {count_clause}
            {with_clause}
            {unwind_clause}
            {return_clause}
        '''
        return query
    
   
    def apply_boolean_operation(self, logics, node_map, node_predicates, predicate_map):
        where_clauses = {'where_no_preds': [], 'where_preds': [],'where_pred_properties':set()}
        no_label_ids = {'no_node_labels': set(), 'no_predicate_labels': set()}
        return_query_or = None  # Initialize return_query_or in case it's used

        for logic in logics:
            if logic['operator'] == "NOT":
                where_query, no_label_id = self.construct_not_operation(logic, node_map, predicate_map)

                if 'nodes' in logic:
                    node_id = logic['nodes']['node_id']
                    if node_id not in node_predicates:
                        where_clauses['where_no_preds'].append(where_query)
                    else:
                        where_clauses['where_preds'].append(where_query)
                    no_label_ids['no_node_labels'].update(no_label_id['no_node_labels'])

                elif 'predicates' in logic:
                    where_clauses['where_preds'].append(where_query)
                    no_label_ids['no_predicate_labels'].update(no_label_id['no_predicate_labels'])

            elif logic['operator'] == "OR":
                where_query, return_query_or,nodes=self.construct_or_operation(logic, node_map, predicate_map)
                if 'predicates' in logic:
                    for node_id in nodes:
                        if node_map[node_id]["id"]:
                            where_clauses['where_preds'].append(where_query)
                if 'predicates' in logic:
                    for node_id in nodes:
                        if node_map[node_id]["properties"]:
                            where_clauses['where_pred_properties'].add(where_query)
                if 'nodes' in  logic :
                    where_clauses['where_no_preds'].append(where_query)
        print("hyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
        print(where_clauses['where_pred_properties'])

        # Return statement should be outside the loop, after all iterations
        return where_clauses, no_label_ids, return_query_or,nodes

                        
                        
              

    def construct_not_operation(self, logic, node_map, predicate_map):
        where_clause = ''
        no_label_id = {'no_node_labels': set(), 'no_predicate_labels': set()}

        if 'nodes' in logic:
            node_id = logic['nodes']['node_id']
            if 'properties' in logic['nodes']:
                properties = logic['nodes']['properties']
                where_clause = ' AND '.join([f"{node_id}.{property} <> '{value}'" for property, value in properties.items()])
            else:
                node_type = node_map[node_id]['type']
                no_label_id['no_node_labels'].add(node_id)
                where_clause = f'NOT ({node_id}: {node_type})'

        elif 'predicates' in logic:
            predicate_id = logic['predicates']['predicate_id']
            predicate = predicate_map[predicate_id]
            label = predicate['type'].replace(" ", "_").lower()
            no_label_id['no_node_labels'].update([predicate['source'], predicate['target']])
            no_label_id['no_predicate_labels'].add(predicate_id)
            where_clause = f"type({predicate_id}) <> '{label}'"
        return where_clause, no_label_id
    def construct_or_operation(self, logic, node_map, predicate_map):
        where_clause = ""  # Initialize as a string to hold combined conditions
        # where_clause_dict = {}
        # return_clause_or = ""
        # return_clause = ""
        # all_nodes = []
        # nodes = []
        # final_nodes = []
        # no_label_ids = {'no_node_labels': set(), 'no_predicate_labels': set()}
        where_clause = []
        where_clause_dict = {}
        return_clause_or = ""
        return_clause = ""
        All_nodes = []
        nodes = []
        final_nodes = []
        no_label_ids = {'no_node_labels': set(), 'no_predicate_labels': set()}

      
        if logic.get('nodes'):
            where_clauses = []  # Collect all conditions
            node_id = logic['nodes'].get('node_id', None)  # Safely get node_id
            properties = logic['nodes'].get('properties', {})  # Safely get properties

            # Construct WHERE clause conditions
            for key, values in properties.items():
                # Join the conditions for each property using OR
                conditions = " OR ".join([f"{node_id}.{key} = '{value}'" for value in values])
                where_clauses.append(conditions)

            # Combine all property-based conditions with the specified operator
            if where_clauses:
                where_clause = f"({') OR ('.join(where_clauses)})"
                
                print("WHERE clause:", where_clause)

  

        # Handle predicates
       

        if logic and 'predicates' in logic:
            for predicate_id in logic['predicates']:
                if predicate_id in predicate_map:
                    predicate = predicate_map[predicate_id]
                    source_node = predicate['source']
                    target_node = predicate['target']
                    All_nodes.append(source_node)
                    All_nodes.append(target_node)

                    # For source node
                    if source_node in node_map and "id" in node_map[source_node]:
                        if node_map[source_node]['id'] != "":
                            condition = f"{source_node}.id = '{node_map[source_node]['id']}'"
                            where_clause.append(condition)
                            where_clause_dict[condition] = source_node
                            nodes.append(source_node)
                    if target_node in node_map and "id" in node_map[target_node]:
                        if node_map[target_node]['id'] != "":
                            condition = f"{target_node}.id = '{node_map[target_node]['id']}'"
                            where_clause.append(condition)
                            where_clause_dict[condition] = target_node
                            nodes.append(target_node)
                            #for properity


                    if source_node in node_map and "properties" in node_map[source_node]:
                        if node_map[source_node]['properties']:  # Check if properties are not empty
                            for key, value in node_map[source_node]['properties'].items():
                                condition = f"{source_node}.{key} = '{value}'"
                                where_clause.append(condition)
                                where_clause_dict[condition] = source_node
                                nodes.append(source_node)

                    if target_node in node_map and "properties" in node_map[target_node]:  # Fixed to check target_node
                        if node_map[target_node]['properties']:  # Check if properties are not empty
                            for key, value in node_map[target_node]['properties'].items():
                                condition = f"{target_node}.{key} = '{value}'"
                                where_clause.append(condition)
                                where_clause_dict[condition] = target_node
                                nodes.append(target_node)


                     
                    

            if where_clause:
                combined_conditions = " OR ".join(where_clause)
                where_clause = f"({combined_conditions})"
                print("mannnnnnn_____________________________________________")
                print(where_clause)
            for condition, node in where_clause_dict.items():
                return_clause += f"CASE WHEN {condition} THEN {node} ELSE NULL END AS {node}, "

            # Convert lists to sets for subtraction, then back to list
            final_nodes = list(set(All_nodes) - set(nodes))

            for node in final_nodes:
                if node in node_map:
                    return_clause += f"{node} AS {node}, "

            return_clause_or = return_clause.rstrip(", ")
        print("i want this all nodes  man _____________________________________________")
        print(nodes) 
        return where_clause, return_clause_or,nodes




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



    def match_node(self, node, no_label_ids=None):
        if no_label_ids and node['node_id'] in no_label_ids:
            return f"({node['node_id']})"

        if node['id']:
            return f"({node['node_id']}:{node['type']} {{id: '{node['id']}'}})"
        else:
            return f"({node['node_id']}:{node['type']})"

    def where_construct(self, node):
        properties = []
        if node['id']: 
            return properties
        for key, property in node['properties'].items():
            properties.append(f"{node['node_id']}.{key} =~ '(?i){property}'")
        return properties

    def parse_neo4j_results(self, results, all_properties):
        (nodes, edges, _, _, node_count, edge_count) = self.process_result(results, all_properties)
        return {"nodes": nodes, "edges": edges, "node_count": node_count, "edge_count": edge_count}

    def parse_and_serialize(self, input, schema, all_properties):
        parsed_result = self.parse_neo4j_results(input, all_properties)
        return parsed_result

    def convert_to_dict(self, results, schema):
        (_, _, node_dict, edge_dict, _, _) = self.process_result(results, True)
        return (node_dict, edge_dict)

    def process_result(self, results, all_properties):
        match_result = results[0]
        if len(results) > 1:
            count_result = results[1]
        else:
            count_result = None
        node_count = 0
        edge_count = 0
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

        if count_result:
            for count_record in count_result:
                node_count = count_record.get('total_nodes')
                edge_count = count_record.get('total_edges', 0)
    
        return (nodes, edges, node_to_dict, edge_to_dict, node_count, edge_count)

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
