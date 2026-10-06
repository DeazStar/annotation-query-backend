import networkx as nx
import re

# Operators allowed in the optional 'logic' section of a request.
LOGIC_OPERATORS = frozenset({"AND", "OR", "NOT"})

# A logic group holds exactly one of these keys.
LOGIC_GROUP_KINDS = ("nodes", "predicates", "children")

# Upper bound on nesting, so a malformed or hostile request cannot recurse without limit.
MAX_LOGIC_DEPTH = 10

# Genomic-position filters have interval semantics in the query generators,
# so they cannot be combined with OR/NOT in v1.
POSITIONAL_PROPERTIES = frozenset(
    {"start", "end", "interval_type", "upstream_distance", "downstream_distance"}
)


class LogicValidationError(ValueError):
    """Raised when the optional 'logic' section of a request is invalid."""


def clean_string(s):
    return re.sub(r'[-_]', '', s)


def _with_chr_prefix(value):
    """Return a chromosome value in the stored format, e.g. '16' -> 'chr16'."""
    value = str(value)
    if value and not value.startswith('chr'):
        return 'chr' + value
    return value

def validate_request(request, schema, source):
    if 'nodes' not in request:
        raise Exception("'nodes' key is missing from the request")

    nodes = request['nodes']

    # validate nodes
    if not isinstance(nodes, list):
        raise Exception("'nodes' must be a list")

    for node in nodes:
        if not isinstance(node, dict):
            raise Exception("each item in 'nodes' must be a dictionary")
        if 'id' not in node:
            raise Exception("'id' is required for each node")
        if 'type' not in node or node['type'] == "":
            raise Exception("'type' is required for each node")
        if 'node_id' not in node or node['node_id'] == "":
            raise Exception("'node_id' is required for each node")
        
        # format the node id into approperiate format
        node_id = node['node_id']
        node['node_id'] = clean_string(node_id)

        node.setdefault('properties', {})

        if 'chr' in node["properties"]:
            node["properties"]["chr"] = _with_chr_prefix(node["properties"]["chr"])

    ''''
    # validate properties of nodes
    for node in nodes:
        properties = node['properties']
        node_type = node['type']
        for property in properties.keys():
            if property not in schema[node_type]['properties']:
                raise Exception(f"{property} doesn't exsist in the schema!")
    '''

    node_map = {}
    for node in nodes:
        if node['node_id'] not in node_map:
            node_map[node['node_id']] = node
        else:
            raise Exception(f"duplicate node_id '{node['node_id']}': node IDs must be unique within the request")

    if 'predicates' not in request:
        raise Exception("'predicates' key is missing from the request")

    predicates = request['predicates']

    if not isinstance(predicates, list):
        raise Exception("'predicates' must be a list")

    for i, predicate in enumerate(predicates):
        if 'type' not in predicate or predicate['type'] == "":
            raise Exception("'type' is required for each predicate")
        if 'source' not in predicate or predicate['source'] == "":
            raise Exception("'source' is required for each predicate")
        if 'target' not in predicate or predicate['target'] == "":
            raise Exception("'target' is required for each predicate")

        predicate['source'] = clean_string(predicate['source'])
        predicate['target'] = clean_string(predicate['target'])

        if 'predicate_id' not in predicate:
            predicate['predicate_id'] = f'p{i}'

        if predicate['source'] not in node_map:
            raise Exception(f"source node '{predicate['source']}' does not exist in the nodes list")
        if predicate['target'] not in node_map:
            raise Exception(f"target node '{predicate['target']}' does not exist in the nodes list")

        predicate_type = '_'.join(predicate['type'].split(' '))
        source_type = node_map[predicate['source']]['type']
        target_type = node_map[predicate['target']]['type']
        predicate_type = f'{source_type}_{predicate_type}_{target_type}'
        if predicate_type not in schema:
            raise Exception(f"invalid predicate '{predicate['type']}': no such relationship between '{source_type}' and '{target_type}'")

    if request.get('logic') is not None:
        # 'logic' refers to predicates by id, so every id must name exactly one predicate.
        # Checked only here: requests without 'logic' keep accepting repeated ids as before.
        predicate_map = {}
        for predicate in predicates:
            predicate_id = predicate['predicate_id']
            if predicate_id in predicate_map:
                raise LogicValidationError(
                    f"duplicate predicate_id '{predicate_id}'; when 'logic' is used, "
                    "give every predicate its own predicate_id"
                )
            predicate_map[predicate_id] = predicate
        validate_logic(request['logic'], node_map, predicate_map)

    if source != 'hypothesis':
        if check_disconnected_graph(request):
            raise Exception("disconnected subgraph: all nodes must be connected through predicates")

    return node_map


def check_disconnected_graph(request):
    # create a networkx graph
    nodes = request['nodes']
    edges = request['predicates']

    G = nx.Graph()

    # create the nodes
    for node in nodes:
        G.add_node(node["node_id"])

    # create the edges
    for edge in edges:
        G.add_edge(edge["source"], edge["target"])

    # identify subgraphs
    connected_components = list(nx.connected_components(G))

    if len(connected_components) > 1:
        return True

    return False


def validate_logic(logic, node_map, predicate_map):
    """Validate and normalize the optional 'logic' section of a request, in place.

    The section is a tree of groups. Each group has an 'operator' (AND, OR or
    NOT) and exactly one of:
      - 'nodes':      conditions on the properties of one node,
      - 'predicates': a list of predicate ids,
      - 'children':   nested groups.

    Normalization done here, so later steps (canonicalization, fingerprinting,
    query generation) can rely on one shape:
      - operators are upper-case, and the root operator defaults to AND;
      - node ids are cleaned with clean_string, like the nodes themselves;
      - every property value becomes a non-empty list of strings.

    Args:
        logic: The value of request['logic'].
        node_map: node_id -> node, as built by validate_request.
        predicate_map: predicate_id -> predicate, after ids were assigned.

    Raises:
        LogicValidationError: If the section is malformed, refers to unknown
            nodes or predicates, or uses a feature not supported yet.
    """
    if not isinstance(logic, dict):
        raise LogicValidationError("'logic' must be an object")
    if 'children' not in logic or any(key in logic for key in ('nodes', 'predicates')):
        raise LogicValidationError("'logic' must contain a 'children' list and no 'nodes' or 'predicates'")

    logic['operator'] = _normalize_operator(logic.get('operator', 'AND'))
    is_and_root = logic['operator'] == 'AND'

    for child in _require_children(logic):
        # Only children of an AND root may mix nodes and predicates (v1 scope).
        _validate_group(child, node_map, predicate_map, depth=1, at_and_root=is_and_root)

    if not is_and_root:
        _require_single_node(logic)


def _validate_group(group, node_map, predicate_map, depth, at_and_root):
    """Validate one logic group and, recursively, everything under it."""
    if depth > MAX_LOGIC_DEPTH:
        raise LogicValidationError(f"'logic' is nested too deeply: the maximum depth is {MAX_LOGIC_DEPTH}")
    if not isinstance(group, dict):
        raise LogicValidationError("each logic group must be an object")

    group['operator'] = _normalize_operator(group.get('operator'))

    kinds = [kind for kind in LOGIC_GROUP_KINDS if kind in group]
    if len(kinds) != 1:
        raise LogicValidationError("each logic group needs exactly one of 'nodes', 'predicates', 'children'")

    if kinds[0] == 'nodes':
        _validate_property_group(group, node_map)
    elif kinds[0] == 'predicates':
        if not at_and_root:
            raise LogicValidationError(
                "not supported yet: predicate groups must sit directly under a root with operator AND"
            )
        _validate_predicate_group(group, predicate_map)
    else:
        for child in _require_children(group):
            _validate_group(child, node_map, predicate_map, depth + 1, at_and_root=False)
        _require_single_node(group)


def _validate_property_group(group, node_map):
    """Validate a group of conditions on one node's properties."""
    target = group['nodes']
    if not isinstance(target, dict):
        raise LogicValidationError("'nodes' in a logic group must be an object with 'node_id' and 'properties'")

    node_id = target.get('node_id')
    if not isinstance(node_id, str) or not node_id:
        raise LogicValidationError("each property group needs a 'node_id'")
    node_id = clean_string(node_id)
    target['node_id'] = node_id
    if node_id not in node_map:
        raise LogicValidationError(f"logic refers to unknown node '{node_id}'")

    properties = target.get('properties')
    if properties is None or properties == {}:
        if group['operator'] == 'NOT':
            raise LogicValidationError(
                f"not supported yet: NOT on the whole node '{node_id}'; list the properties to exclude instead"
            )
        raise LogicValidationError(f"logic group on node '{node_id}' has no properties")
    if not isinstance(properties, dict):
        raise LogicValidationError(f"'properties' of the logic group on node '{node_id}' must be an object")

    own_properties = node_map[node_id].get('properties', {})
    for name, value in properties.items():
        if name in POSITIONAL_PROPERTIES:
            raise LogicValidationError(f"not supported yet: position filter '{name}' inside 'logic'")
        if name in own_properties:
            raise LogicValidationError(
                f"property '{name}' of node '{node_id}' is in both 'properties' and 'logic'; keep it in one place"
            )
        properties[name] = _normalize_values(name, value)


def _validate_predicate_group(group, predicate_map):
    """Validate a group of predicate ids."""
    predicate_ids = group['predicates']
    if not isinstance(predicate_ids, list) or not predicate_ids:
        raise LogicValidationError("'predicates' in a logic group must be a non-empty list of predicate ids")

    for predicate_id in predicate_ids:
        if not isinstance(predicate_id, str) or predicate_id not in predicate_map:
            raise LogicValidationError(f"logic refers to unknown predicate '{predicate_id}'")

    if group['operator'] == 'OR' and len(predicate_ids) > 1:
        endpoints = [
            {predicate_map[pid]['source'], predicate_map[pid]['target']} for pid in predicate_ids
        ]
        if not set.intersection(*endpoints):
            raise LogicValidationError(
                f"not supported yet: OR between predicates {predicate_ids} that share no node"
            )


def _normalize_operator(operator):
    """Return the operator in upper case, or raise if it is not AND, OR or NOT."""
    if not isinstance(operator, str) or operator.upper() not in LOGIC_OPERATORS:
        raise LogicValidationError(f"invalid operator {operator!r}; expected AND, OR or NOT")
    return operator.upper()


def _require_children(group):
    """Return a group's 'children', or raise if it is not a non-empty list."""
    children = group.get('children')
    if not isinstance(children, list) or not children:
        raise LogicValidationError("'children' must be a non-empty list")
    return children


def _normalize_values(name, value):
    """Turn a property value into a non-empty list of non-empty strings."""
    values = value if isinstance(value, list) else [value]
    if not values:
        raise LogicValidationError(f"property '{name}' in 'logic' must not be an empty list")

    normalized = []
    for item in values:
        # bool is a subclass of int, so exclude it explicitly.
        if isinstance(item, bool) or not isinstance(item, (str, int, float)):
            raise LogicValidationError(f"property '{name}' in 'logic' must be a string or a list of strings")
        text = str(item).strip()
        if not text:
            raise LogicValidationError(f"property '{name}' in 'logic' must not be empty")
        normalized.append(_with_chr_prefix(text) if name == 'chr' else text)
    return normalized


def _require_single_node(group):
    """Raise unless every condition under the group is on the same node (v1 scope)."""
    node_ids = _collect_node_ids(group)
    if len(node_ids) != 1:
        raise LogicValidationError(
            f"not supported yet: a combined group must refer to exactly one node, got {sorted(node_ids)}"
        )


def _collect_node_ids(group):
    """Return the node ids of all property groups under a group."""
    if 'nodes' in group:
        return {group['nodes']['node_id']}
    node_ids = set()
    for child in group.get('children', []):
        node_ids |= _collect_node_ids(child)
    return node_ids