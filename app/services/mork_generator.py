from .mork import MORK, ManagedMORK, get_total_counts, get_count_by_label
import os
import glob
from pathlib import Path
from hyperon import MeTTa
from .metta import metta_seralizer
from app import app, perf_logger
from dotenv import load_dotenv
import time
import datetime

from resilience.circuit_breaker import CircuitBreaker
from resilience.exceptions import classify_database_exception
from resilience.executor import execute_with_resilience
from resilience.retry_policy import get_retry_policy

load_dotenv()

from app.error import is_mork_transient
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
import pybreaker

# Resilience Configuration
MORK_RETRY_ATTEMPTS = int(os.getenv("MORK_RETRY_ATTEMPTS", 5))
MORK_RETRY_MIN_WAIT = int(os.getenv("MORK_RETRY_MIN_WAIT", 1))
MORK_RETRY_MAX_WAIT = int(os.getenv("MORK_RETRY_MAX_WAIT", 10))
MORK_BREAKER_FAIL_MAX = int(os.getenv("MORK_BREAKER_FAIL_MAX", 5))
MORK_BREAKER_RESET_TIMEOUT = int(os.getenv("MORK_BREAKER_RESET_TIMEOUT", 30))

def exclude_non_transient(e):
    return not is_mork_transient(e)

mork_breaker = pybreaker.CircuitBreaker(
    fail_max=MORK_BREAKER_FAIL_MAX, 
    reset_timeout=MORK_BREAKER_RESET_TIMEOUT,
    exclude=[exclude_non_transient]
)
class MorkQueryGenerator:
    def __init__(self, dataset_path):
        cb_threshold = int(os.getenv("MORK_CB_THRESHOLD", os.getenv("NEO4J_CB_THRESHOLD", "3")))
        cb_recovery = float(os.getenv("MORK_CB_RECOVERY_TIMEOUT", os.getenv("NEO4J_CB_RECOVERY_TIMEOUT", "30")))
        self._circuit_breaker = CircuitBreaker(
            threshold=cb_threshold, recovery_timeout=cb_recovery
        )
        self._retry_policy = get_retry_policy()
        self.server = self.connect()
        self.metta = MeTTa()
        from app.lib.result_formatter import Result_Formatter
        self.formatter = Result_Formatter()
        # self.clear_space()
        # self.load_dataset(dataset_path)

    def _init_driver_with_cb(self):
        circuit_breaker = self._circuit_breaker
        retry_no_jitter = get_retry_policy(with_jitter=False)
        def _operations():
            try:
                mork_url = os.getenv('MORK_URL')
                server = ManagedMORK.connect(url=mork_url)
                return server
            except Exception as exc:
                raise classify_database_exception(exc,backend='mork') from exc
        return execute_with_resilience(_operations, circuit_breaker=circuit_breaker,retry_policy=retry_no_jitter)
    def connect(self):
        return self._init_driver_with_cb()
    

    def clear_space(self):
        self.server.clear()

    def load_dataset(self, path):
        if not os.path.exists(path):
            raise ValueError(f"Dataset path '{path}' does not exist.")
        paths = glob.glob(os.path.join(path, "**/*.metta"), recursive=True)
        if not paths:
            raise ValueError(f"No .metta files found in dataset path '{path}'.")
        with self.server.work_at("annotation") as annotation:
            for path in paths:
                path = Path(path)
                file_url = path.resolve().as_uri()
                try:
                    annotation.sexpr_import_(file_url).block()
                except Exception as e:
                    print(f"Error loading data: {e}")

    def generate_id(self):
        import uuid
        return str(uuid.uuid4())[:8]

    def construct_node_representation(self, node, identifier):
        node_type = node['type']
        node_representation = ''
        for key, value in node['properties'].items():
            node_representation += f' ({key} ({node_type + " " + identifier}) {value})'
        return node_representation

    @mork_breaker
    @retry(
        stop=stop_after_attempt(MORK_RETRY_ATTEMPTS),
        wait=wait_exponential(multiplier=1, min=MORK_RETRY_MIN_WAIT, max=MORK_RETRY_MAX_WAIT),
        retry=retry_if_exception(is_mork_transient),
        reraise=True
    )
    def run_query(self, query, stop_event=None, species='human'):
        with app.config["annotation_lock"]:
            start_time = time.time()
            timestamp = datetime.datetime.utcnow().isoformat()
            pattern, template, type = query

            with self.server.work_at("annotation") as annotation:
                annotation.transform(pattern, template).block()
                result = annotation.download("(tmp $x)", "($x)")
                with annotation.work_at("tmp") as tmp:
                    tmp.clear()

            metta_result = self.metta.parse_all(result.data)
            duration = (time.time() - start_time) * 1000
            perf_logger.info(
                "Query executed",
                extra={
                    "query": str(query),
                    "timestamp": timestamp,
                    "duration_ms": duration,
                    "species": species,
                    "status": "success",
                },
            )
            print("RES: ", metta_result, flush=True)
            return [metta_result]

    def run_query(self, query, stop_event=None, species='human'):
        def _operation():
            try:
                return self._run_query_once(query, stop_event, species)
            except Exception as exc:
                raise classify_database_exception(exc, backend="mork") from exc

        return execute_with_resilience(
            _operation,
            self._circuit_breaker,
            retry_policy=self._retry_policy,
        )

    def query_Generator(self, requests, node_map, limit=None, node_only=False):
        # this will do only transfomration
        nodes = requests['nodes']
        predicate_map = {}

        pattern = []
        template = []

        node_representation = ''

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

        #if there is no predicate
        if not predicates:
            for node in nodes:
                node_type = node["type"]
                node_id = node["node_id"]
                node_identifier = '$' + node["node_id"]

                if node["id"]:
                    essemble_id = node["id"]
                    pattern.append(f'({node_type} {essemble_id})')
                    template.append(f'(tmp ({node_type} {essemble_id}))')
                else:
                    if len(node["properties"]) == 0:
                        pattern.append(f'({node_type} ${node_id})')
                    else:
                        pattern.append(self.construct_node_representation(node, node_identifier))
                    template.append(f'(tmp ({node_type} {node_identifier}))')

            query = (tuple(pattern), tuple(template), 'query')
            total_count_query = (tuple(pattern), tuple(template), 'total_count')
            label_count_query = (tuple(pattern), tuple(template), 'label_count')

            return [query, total_count_query, label_count_query]
        for predicate in predicates:
            predicate_type = predicate['type'].replace(" ", "_")
            source_id = predicate['source']
            target_id = predicate['target']

            # Handle source node
            source_node = node_map[source_id]
            if not source_node['id']:
                node_identifier = "$" + source_id
                node_representation = self.construct_node_representation(source_node, node_identifier)
                if node_representation != '':
                    pattern.append(node_representation)
                source = f'({source_node["type"]} {node_identifier})'
            else:
                source = f'({str(source_node["type"])} {str(source_node["id"])})'


            # Handle target node
            target_node = node_map[target_id]
            if not target_node['id']:
                target_identifier = "$" + target_id
                node_representation = self.construct_node_representation(target_node, target_identifier)
                if node_representation != '':
                    pattern.append(node_representation)
                target = f'({target_node["type"]} {target_identifier})'
            else:
                target = f'({str(target_node["type"])} {str(target_node["id"])})'


            # Add relationship
            pattern.append(f'({predicate_type} {source} {target})')
            template.append(f'(tmp ({predicate_type} {source} {target}))')

        query = (tuple(pattern), tuple(template), 'query')
        total_count_query = (tuple(pattern), tuple(template), 'total_count')
        label_count_query = (tuple(pattern), tuple(template), 'label_count')
        return [query, total_count_query, label_count_query]

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

        if len(result) == 0:
            return "()", [[]], []

        query = self.get_node_properteis(result, schema)
        res = self.run_query(query)
        return query, res, result

    def get_node_properteis(self, results, schema, species="human"):
        pattern = []
        template = []
        nodes = set()

        for result in results:
            source = result['source']
            source_node_type = result['source'].split(' ')[0]

            if source not in nodes:
                for property in schema[species]['nodes'][source_node_type]['properties']:
                    id = self.generate_id()
                    pattern.append(f'({property} ({source}) ${id})')
                    template.append(f'(tmp (node {property} ({source}) ${id}))')
                nodes.add(source)

            if "target" in result and "predicate" in result:
                target = result['target']
                target_node_type = result['target'].split(' ')[0]

                if target not in nodes:
                    for property in schema[species]['nodes'][target_node_type]['properties']:
                        id = self.generate_id()
                        pattern.append(f'({property} ({target}) ${id})')
                        template.append(f'(tmp (node {property} ({target}) ${id}))')
                    nodes.add(target)

                predicate = result['predicate']
                for property in schema[species]['edges'][predicate]['properties']:
                    random = self.generate_id()
                    pattern.append(f'({property} ({predicate} ({source}) ({target})) ${random})')
                    template.append(f'(tmp (edge {property} ({predicate} ({source}) ({target})) ${random}))')


        query = (tuple(pattern), tuple(template), 'query')
        return query

    def parse_and_serialize(self, input, schema, graph_components, result_type):
        if result_type == 'graph':
            _, results, identifiers = self.prepare_query_input(input, schema)
            input = (results, identifiers)
        return self.formatter.format_result(input, "mork", graph_components, result_type)

    def convert_to_dict(self, results, schema=None):
        query, result, prev_result = self.prepare_query_input(results, schema)
        res = self.formatter.format_result((result, prev_result), "mork", {"properties": True}, result_type='graph')
        return (res['nodes'], res['edges'])

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
