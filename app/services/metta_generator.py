import glob
import os
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
import logging
from .query_generator_interface import QueryGeneratorInterface
from .metta import Metta_Ground, metta_seralizer

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MeTTa_Query_Generator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.metta = MeTTa()
        self.initialize_space()
        self.dataset_path = dataset_path
        self.load_dataset(self.dataset_path)
        self.initialize_grounatoms()
        from app.lib.result_formatter import Result_Formatter
        self.formatter = Result_Formatter()

    def initialize_space(self):
        self.metta.run("!(bind! &space (new-space))")

    def initialize_grounatoms(self):
        Metta_Ground(self.metta)

    def load_dataset(self, path: str) -> None:
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")
        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")
        for path in paths:
            logging.info(f"Start loading dataset from '{path}'...")
            try:
                self.metta.run(f'''
                    !(load-ascii &space {path})
                    ''')
            except Exception as e:
                logging.error(f"Error loading dataset from '{path}': {e}")
        logging.info(f"Finished loading {len(paths)} datasets.")

    def generate_id(self):
        import uuid
        return str(uuid.uuid4())[:8]

    def construct_node_representation(self, node, identifier):
        node_type = node['type']
        node_representation = ''
        for key, value in node['properties'].items():
            node_representation += f' ({key} ({node_type + " " + identifier}) {value})'
        return node_representation

    def query_Generator(self, requests ,node_map, limit=None, node_only=False):
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
            
            
        match_preds = []
        return_preds = []
        node_representation = ''

        match_clause = '''!(match &space (,'''
        return_clause = ''' ('''
        metta_output = ''
 
        # if there is no predicate
        if not predicates:
            for node in nodes:
                node_type = node["type"]
                node_id = node["node_id"]
                node_identifier = '$' + node["node_id"]

                if node["id"]:
                    essemble_id = node["id"]
                    match_preds.append(f'({node_type} {essemble_id})')
                    return_preds.append(f'({node_type} {essemble_id})')
                else:
                    if len(node["properties"]) == 0:
                        match_preds.append(f'({node_type} ${node_id})')
                    else:
                        match_preds.append(self.construct_node_representation(node, node_identifier))
                    return_preds.append(f'({node_type} {node_identifier})')
                    
            query_clause = {
                "match_preds": match_preds,
                "return_preds": return_preds 
            }
            
            if node_only:
                queries = []
                for i, match_query in enumerate(match_preds):
                    queries.append(f'{match_clause} {match_query}) ({return_preds[i]}))')
                    
                queries = ' '.join(queries)
                return [queries, None, None]

            count_query = self.count_query_generator(query_clause, node_only=True)
            match_clause += ' '.join(match_preds)
            return_clause += ' '.join(return_preds)
            metta_output += f'{match_clause}){return_clause}))'
            return [metta_output, count_query[0], count_query[1]]

        for predicate in predicates:
            predicate_type = predicate['type'].replace(" ", "_")
            source_id = predicate['source']
            target_id = predicate['target']

            # Handle source node
            source_node = node_map[source_id]
            if not source_node['id']:
                node_identifier = "$" + source_id
                node_representation += self.construct_node_representation(source_node, node_identifier)
                source = f'({source_node["type"]} {node_identifier})'
            else:
                source = f'({str(source_node["type"])} {str(source_node["id"])})'


            # Handle target node
            target_node = node_map[target_id]
            if not target_node['id']:
                target_identifier = "$" + target_id
                node_representation += self.construct_node_representation(target_node, target_identifier)
                target = f'({target_node["type"]} {target_identifier})'
            else:
                target = f'({str(target_node["type"])} {str(target_node["id"])})'

            
            # Add relationship
            match_preds.append(f'{node_representation} ({predicate_type} {source} {target})')
            return_preds.append((predicate_type, source, target))

        query_clause = {
            "match_preds": match_preds,
            "return_preds": return_preds 
        }
        count = self.count_query_generator(query_clause, node_only=False)
        match_clause += ' '.join(match_preds)
        return_output = []
        for returns in return_preds:
            predicate_type, source, target = returns
            return_output.append(f'({predicate_type} {source} {target})')
        return_clause += ' '.join(return_output)
        metta_output += f'{match_clause}){return_clause}))'

        return [metta_output, count[0], count[1]]

    def count_query_generator(self, query_clauses, node_only):
        metta_output = '''(match &space (,'''
        output = ''' ('''

        match_clause = ' '.join(query_clauses['match_preds'])
        return_clause = []

        for returns in query_clauses['return_preds']:
            if node_only:
                return_clause.append(f'(node {returns})')
            else:
                predicate_type, source, target = returns
                return_clause.append(f'((edge {predicate_type}) (node {source}) (node {target}))')
                
            
        output += ' '.join(return_clause)
        
        metta_output += f'{match_clause}){output}))'
        
        total_count_query = f'''!(total_count (collapse {metta_output}))'''
        lable_count_query = f'''!(label_count (collapse {metta_output}))'''

        return [total_count_query, lable_count_query]

        
    def run_query(self, query_code, stop_event=True):
        result = self.metta.run(query_code)
        return result

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        if result_type == 'graph':
            input = self.prepare_query_input(input, schema) # Now returns (results, identifiers)
        return self.formatter.format_result(input, "metta", graph_components, result_type)
        
    def convert_to_dict(self, results, schema=None):
        result = self.prepare_query_input(results, schema)
        res = self.formatter.format_result(result, "metta", {"properties": True}, result_type='graph')
        return (res['nodes'], res['edges'])

    def prepare_query_input(self, inputs, schema):
        result = []
        for input in inputs:
            if len(input) == 0:
                continue
            tuples = metta_seralizer(input)
            for tuple in tuples:
                if len(tuple) == 2:
                    src_type, src_id = tuple
                    result.append({
                        "source": f"{src_type} {src_id}"
                    })
                else:
                    predicate, src_type, src_id, tgt_type, tgt_id = tuple
                    result.append({
                    "predicate": predicate,
                    "source": f"{src_type} {src_id}",
                    "target": f"{tgt_type} {tgt_id}"
                    })
        query = self.get_node_properties(result, schema)
        res = self.run_query(query)
        return res, result

    def parse_id(self, request):
        nodes = request["nodes"]
        named_types = {"gene": "gene_name", "transcript": "transcript_name"}
        prefixes = ["ENSG", "ENST"]

        for node in nodes:
            is_named_type = node['type'] in named_types
            is_name_as_id = all(not node["id"].startswith(prefix) for prefix in prefixes)
            no_id = node["id"] != ''
            if is_named_type and is_name_as_id and no_id:
                node_type = named_types[node['type']]
                node['properties'][node_type] = node["id"]
                node['id'] = ''
        return request
