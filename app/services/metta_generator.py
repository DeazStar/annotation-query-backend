import glob
import os
import re
from hyperon import MeTTa, SymbolAtom, ExpressionAtom, GroundedAtom
import logging
from .query_generator_interface import QueryGeneratorInterface
from .metta import Metta_Ground, metta_seralizer
from app.error import LogicValidationError
# Set up logging configuration

logger = logging.getLogger(__name__)

class MeTTa_Query_Generator(QueryGeneratorInterface):
    def __init__(self, dataset_path: str):
        self.metta = MeTTa()
        self.initialize_space()
        self.dataset_path = dataset_path
        self.load_dataset(self.dataset_path)
        self.initialize_grounatoms()

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
            logger.info(f"Start loading dataset from '{path}'...")
            try:
                self.metta.run(f'''
                    !(load-ascii &space {path})
                    ''')
            except Exception as e:
                logger.error(f"Error loading dataset from '{path}': {e}")
        logger.info(f"Finished loading {len(paths)} datasets.")

    def generate_id(self):
        import uuid
        return str(uuid.uuid4())[:8]

    def _serialize_metta_value(self, value):
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        if value is None:
            raise ValueError("MeTTa property values cannot be null")
        if not isinstance(value, str):
            value = str(value)

        value = value.strip()
        if not value:
            raise ValueError("MeTTa property values cannot be empty")
        if any(character in value for character in ('\n', '\r', '\t', '(', ')', '"', '\\')):
            raise ValueError("MeTTa property values contain unsupported characters")
        if value.startswith(("$", ";")):
            raise ValueError("MeTTa property values cannot start with '$' or ';'")

        if ":" in value and not value.startswith(("http://", "https://")):
            value = value.split(":", 1)[1]
        value = value.replace(" ", "_").replace("->", "-").strip("_")
        value = re.sub(r"[^A-Za-z0-9_:.\-]", "", value)
        if not value or any(character.isspace() for character in value):
            raise ValueError("MeTTa property values must contain a valid atom")
        return value

    def construct_node_representation(self, node, identifier):
        node_type = node['type']
        node_representation = ''
        for key, value in node['properties'].items():
            serialized = self._serialize_metta_value(value)
            node_representation += f' ({key} ({node_type + " " + identifier}) {serialized})'
        return node_representation

    class _FakeGroundedValue:
        """Mimics hyperon's grounded-atom .get_object().value interface so
        process_result_count can read a Python-computed count dict directly."""
        def __init__(self, value):
            self._value = value
        def get_object(self):
            return self
        @property
        def value(self):
            return self._value

    def _boolean_spec_metta(self, node):
        """
        Build one OR/NOT spec for a single property-filtered node. The spec
        always runs one match per property (METTA has no OR/NOT operator),
        computes the real matching-entity set once, and that same set backs
        the query result and both counts — total_count/label_count are never
        run as separate METTA queries, since each is a pre-aggregated grounded
        value, not something that can be unioned or subtracted per-entity.
        """
        node_type = node["type"]
        ident = "$" + node["node_id"]
        logic = node.get("logic", "AND")

        clauses = [
            self.construct_node_representation({"type": node_type, "properties": {k: v}}, ident).strip()
            for k, v in node["properties"].items()
        ]
        base = {"logic": logic, "node_type": node_type, "ident": ident}
        if logic == "OR":
            base["clauses"] = clauses
        else:
            base["bare"] = f"({node_type} {ident})"
            base["full"] = self.construct_node_representation(node, ident).strip()

        return {kind: dict(base, kind=kind) for kind in ("query", "total_count", "label_count")}

    def _run_boolean_query_metta(self, spec):
        node_type = spec["node_type"]
        return_expr = f"({node_type} {spec['ident']})"

        def run_inner(inner):
            q = f"!(match &space (, {inner}) ({return_expr}))"
            result = self.metta.run(q)
            return result[0] if result and result[0] else []

        if spec["logic"] == "OR":
            merged, seen = [], set()
            for clause in spec["clauses"]:
                for atom in run_inner(clause):
                    key = str(atom)
                    if key not in seen:
                        seen.add(key)
                        merged.append(atom)
        else:
            excluded = {str(atom) for atom in run_inner(spec["full"])}
            merged = [atom for atom in run_inner(spec["bare"]) if str(atom) not in excluded]

        kind = spec["kind"]
        if kind == "query":
            return [merged]
        count = len(merged)
        if kind == "total_count":
            return [[self._FakeGroundedValue({"total_nodes": count, "total_edges": 0})]]
        return [[self._FakeGroundedValue({
            "node_label_count": {node_type: {"count": count}},
            "edge_label_count": {},
        })]]

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
            if len(nodes) == 1 and nodes[0].get("logic", "AND") != "AND" and not nodes[0].get("id") and nodes[0].get("properties"):
                if node_only:
                    raise LogicValidationError("logic OR/NOT is not supported with node_only on MeTTa yet")
                spec = self._boolean_spec_metta(nodes[0])
                return [spec["query"], spec["total_count"], spec["label_count"]]
            if any(n.get("logic", "AND") != "AND" for n in nodes):
                raise LogicValidationError("logic OR/NOT is only supported for single-node queries with properties on MeTTa")
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

        for node in nodes:
            if node.get("logic", "AND") != "AND":
                raise LogicValidationError("logic OR/NOT is not supported together with predicates on MeTTa yet")
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

        
    def run_query(self, query_code, stop_event=True, species="human"):
        if isinstance(query_code, dict):
            return self._run_boolean_query_metta(query_code)
        result = self.metta.run(query_code)
        return result

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        if result_type == 'graph':

            result = self.prepare_query_input(input, schema)
            
            result = self.parse_and_serialize_properties(result, graph_components, result_type)
            return result
        else:
            (_,_,_,_, meta_data) = self.process_result(input, graph_components, result_type)
            return {
                "node_count": meta_data.get('node_count', 0),
                "edge_count": meta_data.get('edge_count', 0),
                "node_count_by_label": meta_data.get('node_count_by_label', []),
                "edge_count_by_label": meta_data.get('edge_count_by_label', []),
            }

    LOCALIZATION_PREDICATES = ("located_in", "part_of")

    def find_localized_proteins(self, protein_ids, location_ids, species="human"):
        """
        Returns (protein_id, location_id) pairs for proteins located_in or
        part_of the given cellular_component location ids.
        """
        pairs_found = []
        for protein_id in protein_ids:
            for location_id in location_ids:
                source = f"protein {protein_id}"
                target = f"cellular_component {location_id}"
                for predicate in self.LOCALIZATION_PREDICATES:
                    match_clause = f"({predicate} ({source}) ({target}))"
                    return_clause = f"(edge {predicate} ({source}) ({target}))"
                    query_code = f"!(match &space (, {match_clause}) (, {return_clause}))"
                    try:
                        result = self.run_query(query_code)
                    except Exception as e:
                        logger.warning(f"MeTTa query error: {e}\nQuery: {query_code}")
                        result = []
                    matched = result[0] if result and isinstance(result[0], list) else result
                    if matched:
                        pairs_found.append((protein_id, location_id))
                        break
        return pairs_found
        
    def parse_and_serialize_properties(self, input, graph_components, result_type):
        (nodes, edges, _, _, meta_data) = self.process_result(input, graph_components, result_type)
        return {"nodes": nodes[0], "edges": edges[0],
                "node_count": meta_data.get('node_count', 0),
                "edge_count": meta_data.get('edge_count', 0),
                "node_count_by_label": meta_data.get('node_count_by_label', []),
                "edge_count_by_label": meta_data.get('edge_count_by_label', [])
        }

    def get_node_properties(self, results, schema, species="human"):
        """
        Runs one match query per (entity, property) pair instead of one giant
        conjunction, then merges every returned atom into a single flat list.
        METTA's match requires every conjunct to have a matching fact, so
        bundling an entity's full property set into one match fails whenever
        any single declared property has no stored value for that entity.
        """
        merged = []
        nodes_seen = set()

        def run_single(pattern, template):
            q = f"!(match &space (, {pattern}) ({template}))"
            res = self.run_query(q, species=species)
            atoms = res[0] if res and res[0] else []
            merged.extend(atoms)

        for result in results:
            source = result['source']
            source_node_type = result['source'].split(' ')[0]

            if source not in nodes_seen:
                for property, _ in schema[species]['nodes'][source_node_type]['properties'].items():
                    var = self.generate_id()
                    run_single(f"({property} ({source}) ${var})", f"(node {property} ({source}) ${var})")
                nodes_seen.add(source)

            if "target" in result and "predicate" in result:
                target = result['target']
                target_node_type = result['target'].split(' ')[0]
                if target not in nodes_seen:
                    for property, _ in schema[species]['nodes'][target_node_type]['properties'].items():
                        var = self.generate_id()
                        run_single(f"({property} ({target}) ${var})", f"(node {property} ({target}) ${var})")
                    nodes_seen.add(target)

                predicate = result['predicate']
                edge_props = schema[species]['edges'].get(predicate, {}).get('properties', {})
                for property, _ in edge_props.items():
                    var = self.generate_id()
                    run_single(
                        f"({property} ({predicate} ({source}) ({target})) ${var})",
                        f"(edge {property} ({predicate} ({source}) ({target})) ${var})",
                    )

        return merged

    def convert_to_dict(self, results, schema=None):
        result = self.prepare_query_input(results, schema)
        (_, node_dict, edge_dict) = self.process_result(result[0], True)
        return (node_dict, edge_dict)

    # Won't work because of we don't try to parse node count and count by labels
    def process_result(self, results, graph_components, result_type):
        node_and_edge_count = {}
        count_by_label = {}
        nodes = []
        edges = []
        node_to_dict = {}
        edge_to_dict = {}
        meta_data = {}


        if result_type == 'graph':
            nodes, edges, node_to_dict, edge_to_dict = self.process_result_graph(
                    results[0], graph_components)

        if result_type == 'count':
            if len(results) > 0:
                node_and_edge_count = results[0]

            if len(results) > 1:
                count_by_label = results[1]

            meta_data = self.process_result_count(
                node_and_edge_count, count_by_label, graph_components)

        return (nodes, edges, node_to_dict, edge_to_dict, meta_data)
        
    def process_result_graph(self, results, graph_components):
        nodes = {}
        relationships_dict = {}
        node_result = []
        edge_result = []
        node_to_dict = {}
        edge_to_dict = {}
        node_type = set()
        edge_type = set()
        tuples = metta_seralizer(results)
        named_types = ['gene_name', 'transcript_name', 'protein_name',
                       'pathway_name', 'term_name']

        for match in tuples:
            graph_attribute = match[0]
            match = match[1:]

            if graph_attribute == "node":
                if len(match) > 4:
                    predicate = match[0]
                    src_type = match[1]
                    src_value = match[2]
                    tgt = list(match[3:])
                else:
                    predicate, src_type, src_value, tgt = match
                if (src_type, src_value) not in nodes:
                    nodes[(src_type, src_value)] = {
                        "id": f"{src_type} {src_value}",
                        "type": src_type,
                    }
                
                if graph_components['properties']:
                     nodes[(src_type, src_value)][predicate] = tgt
                else:
                    if predicate in named_types:
                        nodes[(src_type, src_value)]['name'] = tgt

                if 'synonyms' in nodes[(src_type, src_value)]:
                    del nodes[(src_type, src_value)]['synonyms']
                if src_type not in node_type:
                    node_type.add(src_type)
                    node_to_dict[src_type] = []
                node_data = {}
                node_data["data"] = nodes[(src_type, src_value)]
                node_to_dict[src_type].append(node_data)
            elif graph_attribute == "edge":
                property_name, predicate, source, source_id, target, target_id = match[:6]
                value = ' '.join(match[6:])

                key = (predicate, source, source_id, target, target_id)
                if key not in relationships_dict:
                    relationships_dict[key] = {
                        "edge_id": f'{source}_{predicate}_{target}',
                        "label": predicate,
                        "source": f"{source} {source_id}",
                        "target": f"{target} {target_id}",
                    }
                
                if property_name == "source":
                    relationships_dict[key]["source_data"] = value
                else:
                    relationships_dict[key][property_name] = value
                
                if predicate not in edge_type:
                    edge_type.add(predicate)
                    edge_to_dict[predicate] = []
                edge_data = {}
                edge_data['data'] = relationships_dict[key]
                edge_to_dict[predicate].append(edge_data)
        node_list = [{"data": node} for node in nodes.values()]
        relationship_list = [{"data": relationship} for relationship in relationships_dict.values()]

        node_result.append(node_list)
        edge_result.append(relationship_list)
        return (node_result, edge_result, node_to_dict, edge_to_dict)
    
    def process_result_count(self, node_and_edge_count, count_by_label, graph_components):
        if len(node_and_edge_count) != 0:
            node_and_edge_count = node_and_edge_count[0].get_object().value
        node_count_by_label = []
        edge_count_by_label = []
        
        if len(count_by_label) != 0:
            count_by_label = count_by_label[0].get_object().value
            node_label_count = count_by_label['node_label_count']
            edge_label_count = count_by_label['edge_label_count']

            # update the way node count by label and edge count by label are represented
            for key, value in node_label_count.items():
                node_count_by_label.append(
                    {'label': key, 'count': value['count']}) 
            for key, value in edge_label_count.items():
                edge_count_by_label.append(
                    {'label': key, 'count': value['count']})
            
        meta_data = {
            "node_count": node_and_edge_count.get('total_nodes', 0),
            "edge_count": node_and_edge_count.get('total_edges', 0),
            "node_count_by_label": node_count_by_label if node_count_by_label else [],
            "edge_count_by_label": edge_count_by_label if edge_count_by_label else []
        }
        
        return meta_data

    def prepare_query_input(self, inputs, schema, species="human"):
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
        merged_atoms = self.get_node_properties(result, schema, species=species)
        return [merged_atoms]

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
