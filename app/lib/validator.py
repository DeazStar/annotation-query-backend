import difflib
import networkx as nx
import re

# Operators allowed in the optional 'logic' section of a request.
LOGIC_OPERATORS = frozenset({"AND", "OR", "NOT"})

# A logic group holds exactly one of these keys.
LOGIC_GROUP_KINDS = ("nodes", "predicates", "children")

# Every key a logic group may have, and every key of its 'nodes' object.
# Anything else is a mistake (e.g. "operater"): rejected, never ignored.
LOGIC_GROUP_KEYS = frozenset({"operator", *LOGIC_GROUP_KINDS})
LOGIC_NODE_KEYS = frozenset({"node_id", "properties"})

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
      - a root that is a single group, e.g. {"operator": "OR", "nodes": ...},
        is wrapped as {"operator": "AND", "children": [that group]};
      - operators are upper-case, and the root operator defaults to AND;
      - node ids are cleaned with clean_string, like the nodes themselves;
      - every property value becomes a non-empty list of strings.

    Args:
        logic: The value of request['logic'].
        node_map: node_id -> node, as built by validate_request.
        predicate_map: predicate_id -> predicate, after ids were assigned.

    Raises:
        LogicValidationError: If the section is malformed, has unknown keys,
            refers to unknown nodes or predicates, or uses a feature not
            supported yet.
    """
    if not isinstance(logic, dict):
        raise LogicValidationError("'logic' must be an object")
    _require_known_keys(logic, LOGIC_GROUP_KEYS, "'logic'")
    kinds = [kind for kind in LOGIC_GROUP_KINDS if kind in logic]
    if len(kinds) != 1:
        raise LogicValidationError("'logic' needs exactly one of 'children', 'nodes', 'predicates'")

    if kinds[0] != 'children':
        # One condition on its own: the same as an AND root with one child.
        # The root defaults to AND, so a single group without an operator does too.
        single_group = {**logic, 'operator': logic.get('operator', 'AND')}
        logic.clear()
        logic.update({'operator': 'AND', 'children': [single_group]})

    logic['operator'] = _normalize_operator(logic.get('operator', 'AND'))
    is_and_root = logic['operator'] == 'AND'

    for child in _require_children(logic):
        # Only children of an AND root may mix nodes and predicates (v1 scope).
        _validate_group(child, node_map, predicate_map, depth=1, at_and_root=is_and_root)

    _require_predicates_in_one_group(logic['children'])

    if not is_and_root:
        _require_single_node(logic)


def _validate_group(group, node_map, predicate_map, depth, at_and_root):
    """Validate one logic group and, recursively, everything under it."""
    if depth > MAX_LOGIC_DEPTH:
        raise LogicValidationError(f"'logic' is nested too deeply: the maximum depth is {MAX_LOGIC_DEPTH}")
    if not isinstance(group, dict):
        raise LogicValidationError("each logic group must be an object")
    _require_known_keys(group, LOGIC_GROUP_KEYS, "a logic group")
    if 'operator' not in group:
        raise LogicValidationError("each logic group needs an 'operator': AND, OR or NOT")

    group['operator'] = _normalize_operator(group['operator'])

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
    _require_known_keys(target, LOGIC_NODE_KEYS, "'nodes' of a logic group")

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

    if group['operator'] == 'NOT':
        all_predicates = list(predicate_map.values())
        for predicate_id in predicate_ids:
            negated_predicate_ends(predicate_map[predicate_id], all_predicates)

    if group['operator'] == 'OR' and len(predicate_ids) > 1:
        or_predicate_ends(predicate_ids, list(predicate_map.values()))


def negated_predicate_ends(predicate, predicates):
    """Return (kept_node_id, far_node_id) for a predicate under NOT.

    "NOT p" means: return the kept node only when no edge like p connects it
    to a node like the far node. The far node is never returned; it only
    describes the edge that must not exist. Every backend uses this function,
    so they all agree on which end is which:
      - the far node is the end that no other predicate uses;
      - if neither end is used elsewhere (p is the whole pattern, e.g.
        "genes with no pathway"), the edge's source is kept;
      - if both ends are used elsewhere, it is not supported yet.

    Args:
        predicate: The negated predicate.
        predicates: All predicates of the request, including this one.

    Raises:
        LogicValidationError: If no end can be the far node.
    """
    predicate_id = predicate['predicate_id']
    source, target = predicate['source'], predicate['target']
    if source == target:
        raise LogicValidationError(
            f"not supported yet: NOT on predicate '{predicate_id}', which starts and ends at the same node"
        )

    used_elsewhere = {
        node_id
        for other in predicates
        if other['predicate_id'] != predicate_id
        for node_id in (other['source'], other['target'])
    }
    if source in used_elsewhere and target in used_elsewhere:
        raise LogicValidationError(
            f"not supported yet: NOT on predicate '{predicate_id}', whose two nodes are both used by other predicates"
        )
    if target in used_elsewhere:
        return target, source
    return source, target


def counted_nodes_and_predicates(request):
    """Return (nodes, predicates) of a validated request that can appear in its results.

    Predicates under NOT and their far nodes (see negated_predicate_ends) only
    describe edges that must not exist, so they are left out of label counts;
    otherwise a count would show e.g. "pathway: 0" for a node never returned.
    """
    nodes = request.get('nodes', [])
    predicates = request.get('predicates', [])
    logic = request.get('logic')
    if not logic or logic.get('operator') != 'AND':
        return nodes, predicates

    negated_ids = {
        predicate_id
        for group in logic.get('children', [])
        if 'predicates' in group and group.get('operator') == 'NOT'
        for predicate_id in group['predicates']
    }
    if not negated_ids:
        return nodes, predicates

    far_ids = {
        negated_predicate_ends(predicate, predicates)[1]
        for predicate in predicates
        if predicate['predicate_id'] in negated_ids
    }
    return (
        [node for node in nodes if node['node_id'] not in far_ids],
        [predicate for predicate in predicates if predicate['predicate_id'] not in negated_ids],
    )


def or_predicate_ends(predicate_ids, predicates):
    """Return (kept_node_id, {predicate_id: far_node_id}) for an OR group of predicates.

    "OR [p0, p1]" means: return the kept node when at least one of these
    edges connects it to a node like that edge's far node, together with
    whichever of those edges exist. Every backend uses this function:
      - the predicates must share exactly one node, which is kept;
      - each predicate's other end is its far node. Other predicates may
        continue from the far node; they are ANDed inside that alternative
        ("a transcript that makes a protein" OR "a pathway");
      - nothing beyond a far node may lead back to the kept node, so each
        alternative stands alone (two alternatives joined by another edge
        could not be ORed).

    Args:
        predicate_ids: The ids in the OR group (two or more).
        predicates: All predicates of the request.

    Raises:
        LogicValidationError: If the group does not have that shape.
    """
    by_id = {predicate['predicate_id']: predicate for predicate in predicates}
    group = [by_id[predicate_id] for predicate_id in predicate_ids]

    for predicate in group:
        if predicate['source'] == predicate['target']:
            raise LogicValidationError(
                f"not supported yet: OR on predicate '{predicate['predicate_id']}', "
                "which starts and ends at the same node"
            )

    shared = set.intersection(*({predicate['source'], predicate['target']} for predicate in group))
    if not shared:
        raise LogicValidationError(f"not supported yet: OR between predicates {predicate_ids} that share no node")
    if len(shared) > 1:
        raise LogicValidationError(f"not supported yet: OR between predicates {predicate_ids} that share both nodes")
    kept_id = shared.pop()

    far_ids = {}
    for predicate in group:
        predicate_id = predicate['predicate_id']
        far_id = predicate['target'] if predicate['source'] == kept_id else predicate['source']
        if _leads_back_to(far_id, kept_id, predicate_id, predicates):
            raise LogicValidationError(
                f"not supported yet: OR on predicate '{predicate_id}', whose node '{far_id}' "
                f"connects back to node '{kept_id}' through other predicates"
            )
        far_ids[predicate_id] = far_id
    return kept_id, far_ids


def _leads_back_to(start_id, kept_id, skipped_predicate_id, predicates):
    """Return True if a walk from start_id over the other predicates reaches kept_id.

    The walk never steps onto kept_id, so it covers exactly the part of the
    pattern that hangs off start_id: what gets ANDed into that alternative.
    """
    others = [p for p in predicates if p['predicate_id'] != skipped_predicate_id]
    seen, to_visit = {start_id}, [start_id]
    while to_visit:
        node_id = to_visit.pop()
        for other in others:
            ends = (other['source'], other['target'])
            if node_id not in ends:
                continue
            if kept_id in ends:
                return True
            for end in ends:
                if end not in seen:
                    seen.add(end)
                    to_visit.append(end)
    return False


def _require_predicates_in_one_group(groups):
    """Raise if a predicate id appears in more than one predicate group."""
    seen = set()
    for group in groups:
        for predicate_id in group.get('predicates', []):
            if predicate_id in seen:
                raise LogicValidationError(f"predicate '{predicate_id}' is used in more than one predicate group")
            seen.add(predicate_id)


def _require_known_keys(obj, allowed, where):
    """Raise if obj has a key outside allowed, naming the key and the closest allowed one.

    A misspelled key must fail loudly: an ignored "operater": "NOT" would
    leave the group on its default operator and return the opposite results.
    """
    for key in sorted(set(obj) - allowed, key=str):
        message = f"unknown key {key!r} in {where}; allowed keys are {', '.join(sorted(allowed))}"
        close = difflib.get_close_matches(str(key), allowed, n=1)
        if close:
            message += f" (did you mean {close[0]!r}?)"
        raise LogicValidationError(message)


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