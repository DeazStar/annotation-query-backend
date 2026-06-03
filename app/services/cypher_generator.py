from typing import List
import logging
from dotenv import load_dotenv
import neo4j
from app.services.query_generator_interface import QueryGeneratorInterface
from neo4j import GraphDatabase
import glob
import os
from neo4j.graph import Node, Relationship
from app.error import ThreadStopException

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CypherQueryGenerator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.human_driver = GraphDatabase.driver(
            os.getenv('HUMAN_NEO4J_URI'),
            auth=(os.getenv('HUMAN_NEO4J_USERNAME'), os.getenv('HUMAN_NEO4J_PASSWORD'))
        )
        self.fly_driver = GraphDatabase.driver(
            os.getenv('FLY_NEO4J_URI'),
            auth=(os.getenv('FLY_NEO4J_USERNAME'), os.getenv('FLY_NEO4J_PASSWORD'))
        )
        from app.lib.result_formatter import Result_Formatter
        self.formatter = Result_Formatter()
        # self.dataset_path = dataset_path
        # self.load_dataset(self.dataset_path)

    def close(self):
        self.driver.close()

    def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")

        paths = glob.glob(os.path.join(path, "**/*.cypher"), recursive=True)
        if not paths:
            raise ValueError(
                f"No .cypher files found in dataset path '{path}'.")

        # Separate nodes and edges
        nodes_paths = [p for p in paths if p.endswith("nodes.cypher")]
        edges_paths = [p for p in paths if p.endswith("edges.cypher")]

        # Helper function to process files
        def process_files(file_paths, file_type):
            for file_path in file_paths:
                logger.info(
                    f"Start loading {file_type} dataset from '{file_path}'...")
                try:
                    with open(file_path, 'r') as file:
                        data = file.read()
                        for line in data.splitlines():
                            self.run_query(line)
                except Exception as e:
                    logger.error(
                        f"Error loading {file_type} dataset from '{file_path}': {e}")

        # Process nodes and edges files
        process_files(nodes_paths, "nodes")
        process_files(edges_paths, "edges")

        logger.info(
            f"Finished loading {len(nodes_paths)} nodes and {len(edges_paths)} edges datasets.")

    def run_query(self, query_code, stop_event=None,  species="human"):
        results = []
        driver = self.human_driver if species == "human" else self.fly_driver
        # use lazy loading for improved performance
        with driver.session() as session:
            result = session.run(query_code)
            for record in result:
                if stop_event is not None and stop_event.is_set():
                    raise ThreadStopException('Query runner is stopped')
                results.append(record)
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
        else:
            predicates = None

        cypher_queries = []
        match_preds = []
        return_preds = []
        where_preds = []
        match_no_preds = []
        return_no_preds = []
        where_no_preds = []
        node_ids = set()
        clause_list = []

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
                cypher_query = self.construct_optional_clause(
                    match_no_preds, return_no_preds, where_no_preds, limit)
            else:
                cypher_query = self.construct_clause(
                    match_no_preds, return_no_preds, where_no_preds, limit)
            cypher_queries.append(cypher_query)
            query_clauses = {
                "match_no_preds": match_no_preds,
                "return_no_preds": return_no_preds,
                "where_no_preds": where_no_preds,
                "list_of_node_ids": list_of_node_ids,
                "predicates": predicates
            }
            count = self.construct_count_clause(
                query_clauses, node_map, predicate_map)
            cypher_queries.extend(count)
        else:
            for i, predicate in enumerate(predicates):
                predicate_id = predicate['predicate_id']
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]
                source_var = source_node['node_id']
                target_var = target_node['node_id']

                source_match = self.match_node(source_node, source_var)
                target_match = self.match_node(target_node, target_var)

                tmp_where_preds = []
                if source_var not in node_ids:
                    tmp_where_preds.extend(self.where_construct(source_node, source_var))
                    where_preds.extend(
                        self.where_construct(source_node, source_var))
                if target_var not in node_ids:
                    tmp_where_preds.extend(self.where_construct(target_node, target_var))
                    where_preds.extend(
                        self.where_construct(target_node, target_var))

                return_preds.append(predicate_id)
                node_ids.add(source_var)
                node_ids.add(target_var)

                match_preds.append(
                    f"{source_match}-[{predicate_id}:{predicate_type}]->{target_match}"
                )

                # Construct the MATCH clause
                match_clause = f"MATCH {source_match}-[{predicate_id}:{predicate_type}]->{target_match}"

                # Construct the WHERE clause if there are conditions
                where_clause = f"WHERE {' AND '.join(tmp_where_preds)}" if len(tmp_where_preds) >= 1 else ''

                if i == len(predicates) - 1:
                    # Construct the RETURN clause
                    return_clause = f"RETURN {', '.join(return_preds)}, {', '.join(node_ids)}"

                    # Combine all clauses into a single query
                    clause_list.append(f"{match_clause} {where_clause} {return_clause}")
                else:
                    with_clause = f"WITH {', '.join(return_preds)}, {', '.join(node_ids)}"

                    clause_list.append(f"{match_clause} {where_clause} {with_clause}")

            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()
            full_return_preds = return_preds + list_of_node_ids


            cypher_query = ' '.join(clause_list)
            cypher_queries.append(cypher_query)
            query_clauses = {
                "match_preds": match_preds,
                "full_return_preds": full_return_preds,
                "where_preds": where_preds,
                "list_of_node_ids": list_of_node_ids,
                "return_preds": return_preds,
                "predicates": predicates
            }
            count = self.construct_count_clause(
                query_clauses, node_map, predicate_map)
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
            query_clauses['list_of_node_ids'].extend(
                query_clauses['return_no_preds'])

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
        combined_nodes = ' + '.join(
            [f"{var}_count" for var in query_clauses['list_of_node_ids']])
        combined_edges = None
        if 'return_preds' in query_clauses:
            combined_edges = ' + '.join(
                [f"{var}_count" for var in query_clauses['return_preds']])
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
        '''
        for now remove the limit from the backend
        and handle it from the client side
        '''
        # if limit:
            # curr_limit = min(1000, int(limit))
        # else:
            # curr_limit = 1000
        if limit:
            return f"LIMIT {limit}"
        return f""

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
            if key == "start":
                properties.append(f"{var_name}.{key} >= {property}")
            elif key == "end":
                properties.append(f"{var_name}.{key} <= {property}")
            else:
                properties.append(f"{var_name}.{key} =~ '(?i){property}'")
        return properties

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        return self.formatter.format_result(input, "neo4j", graph_components, result_type)

    def convert_to_dict(self, results, schema, graph_components):
        graph_components['properties'] = True
        res = self.formatter.format_result(results, "neo4j", graph_components, result_type='graph')
        return (res['nodes'], res['edges'])

    def parse_id(self, request):
        nodes = request["nodes"]
        named_types = {"gene": "gene_name", "transcript": "transcript_name"}
        prefixes = ["ensg", "enst"]

        for node in nodes:
            is_named_type = node['type'] in named_types
            id = node["id"].lower()
            is_name_as_id = all(not id.startswith(prefix)
                                for prefix in prefixes)
            no_id = node["id"] != ''
            if is_named_type and is_name_as_id and no_id:
                node_type = named_types[node['type']]
                node['properties'][node_type] = node["id"]
                node['id'] = ''
            node["id"] = node["id"].lower()
        return request

    def list_query_generator_source_target(self, source, target, target_ids, relationship):
        source_node = self.match_node(source, "source")
        target_node = self.match_node(target, "target")

        where_clause = ""
        for key, properties in source['properties'].items():
            where_clause += f"source.{key} = '{properties}' AND "

        where_clause += f"target.id IN target_ids"

        where_clause = f"WHERE {where_clause}"

        with_clause = f"WITH {str(target_ids)} AS target_ids"

        match_clause = f"MATCH {source_node}-[{relationship}]->{target_node}"

        return_clause = f"RETURN COLLECT(DISTINCT source.id) AS source_ids, target.id AS target_ids"

        query= f"""
        {with_clause}
        {match_clause}
        {where_clause}
        {return_clause}
        """

        return query

    def list_query_generator_both(self, source, target, source_ids, target_ids, relationship):
        source_node = self.match_node(source, "source")
        target_node = self.match_node(target, "target")

        where_clause = f"source.id IN source_ids AND "
        where_clause += f"target.id IN target_ids"
        where_clause = f"WHERE {where_clause}"

        with_clause = f"WITH {str(source_ids)} AS source_ids, {str(target_ids)} AS target_ids"

        match_clause = f"MATCH {source_node}-[{relationship}]->{target_node}"

        return_clause = "RETURN target.id AS target_ids, COLLECT(DISTINCT source.id) AS source_ids"

        query= f"""
        {with_clause}
        {match_clause}
        {where_clause}
        {return_clause}
        """

        return query

    def parse_list_query(self, results):
        paresed_result = {}
        for result in results:
            source_ids = result['source_ids']
            target_ids = result['target_ids']

            paresed_result[target_ids] = {'node_ids': []}
            paresed_result[target_ids]['node_ids'] = source_ids

        return paresed_result
