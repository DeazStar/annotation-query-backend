import copy
import hashlib
import json
from collections import deque

def canonicalize_graph(request):
    """
    Deterministically renames node_id and predicate source/target/predicate_id
    to stable ids (e.g. n0, n1, ...; p0, p1, ...).
    References inside the optional 'logic' section are renamed the same way.
    Returns a deep copy of the request with canonicalized IDs, and a map from old_id to new_id.
    Pathological symmetric cases may still collide, but this covers most real graphs.
    """
    req_copy = copy.deepcopy(request)
    nodes = req_copy.get('nodes', [])
    predicates = req_copy.get('predicates', [])

    # 1. Compute basic sort key for each node
    def get_node_sort_key(node):
        node_type = node.get('type', '')
        node_id_val = str(node.get('id', ''))
        props = node.get('properties', {})
        canon_props_str = json.dumps(props, sort_keys=True)
        return (node_type, node_id_val, canon_props_str)

    # Attach old_id and sort key
    for i, node in enumerate(nodes):
        node['_old_id'] = node.get('node_id')
        node['_sort_key'] = get_node_sort_key(node)
        node['_orig_idx'] = i

    # 2. Build adjacency for BFS
    adj = {node['_old_id']: [] for node in nodes}
    for p in predicates:
        source = p.get('source')
        target = p.get('target')
        if source in adj and target in adj:
            # We treat undirected for BFS distance
            adj[source].append(target)
            adj[target].append(source)

    # 3. BFS to compute distance from "anchored" nodes
    # Anchored nodes have a non-empty 'id' property.
    anchored_nodes = [n for n in nodes if n.get('id', '') != '']
    anchored_nodes.sort(key=lambda x: str(x.get('id', '')))

    distances = {n['_old_id']: float('inf') for n in nodes}
    queue = deque()

    for i, n in enumerate(anchored_nodes):
        distances[n['_old_id']] = 0
        queue.append((n['_old_id'], 0))

    while queue:
        curr, dist = queue.popleft()
        for neighbor in adj[curr]:
            if distances[neighbor] > dist + 1:
                distances[neighbor] = dist + 1
                queue.append((neighbor, dist + 1))

    # 4. Final sort: Sort by sort_key, then BFS distance, then original index
    nodes.sort(key=lambda n: (n['_sort_key'], distances[n['_old_id']], n['_orig_idx']))

    # 5. Assign new IDs
    old_to_new_node = {}
    for i, node in enumerate(nodes):
        new_id = f"n{i}"
        old_to_new_node[node['_old_id']] = new_id
        node['node_id'] = new_id
        # Clean up internal fields
        del node['_old_id']
        del node['_sort_key']
        del node['_orig_idx']

    # 6. Update predicates
    for p in predicates:
        if p.get('source') in old_to_new_node:
            p['source'] = old_to_new_node[p['source']]
        if p.get('target') in old_to_new_node:
            p['target'] = old_to_new_node[p['target']]

    # 7. Renumber predicates based on their source/target/type (already canonicalized)
    def get_predicate_sort_key(p):
        return (p.get('source', ''), p.get('target', ''), p.get('type', ''), json.dumps(p.get('properties', {}), sort_keys=True))
    
    # Remember each predicate's id before renumbering, so 'logic' references can follow.
    for p in predicates:
        p['_old_id'] = p.get('predicate_id')

    predicates.sort(key=get_predicate_sort_key)

    old_to_new_predicate = {}
    for i, p in enumerate(predicates):
        p['predicate_id'] = f"p{i}"
        old_id = p.pop('_old_id')
        if old_id is not None:
            old_to_new_predicate[old_id] = p['predicate_id']

    req_copy['nodes'] = nodes
    req_copy['predicates'] = predicates

    # 8. Rename references inside the optional boolean 'logic' section
    if req_copy.get('logic') is not None:
        req_copy['logic'] = _canonicalize_logic(req_copy['logic'], old_to_new_node, old_to_new_predicate)

    return req_copy, old_to_new_node


def _canonicalize_logic(group, old_to_new_node, old_to_new_predicate):
    """
    Returns a canonical copy of a validated logic group (see validate_logic).

    Node and predicate ids are renamed to their canonical ids, and the order of
    children, predicate ids and property values is made deterministic, with
    duplicates removed. Neither changes the meaning: AND, OR and NOT
    ("none of them") all ignore order and repetition. Keys that are not part
    of the format are dropped.
    """
    canonical = {'operator': group['operator']}

    if 'nodes' in group:
        target = group['nodes']
        old_id = target['node_id']
        canonical['nodes'] = {
            'node_id': old_to_new_node.get(old_id, old_id),
            'properties': {name: sorted(set(values)) for name, values in target['properties'].items()},
        }
    elif 'predicates' in group:
        canonical['predicates'] = sorted(
            {old_to_new_predicate.get(pid, pid) for pid in group['predicates']}
        )
    else:
        children = [
            _canonicalize_logic(child, old_to_new_node, old_to_new_predicate)
            for child in group['children']
        ]
        canonical['children'] = sorted(children, key=lambda child: json.dumps(child, sort_keys=True))

    return canonical

def query_fingerprint(canonical_request, species, data_source, limit, node_only, properties, db_type=None):
    """
    Computes a stable SHA-256 hash of the normalized request components.
    """
    # Extract only the parts that change results
    payload = {
        "nodes": canonical_request.get('nodes', []),
        "predicates": canonical_request.get('predicates', []),
        "species": species,
        "data_source": data_source,
        "limit": limit,
        "node_only": node_only,
        "properties": properties,
        "db_type": db_type,
    }
    
    # Added only when present, so fingerprints of requests without logic stay
    # identical to the ones already stored with existing annotations.
    logic = canonical_request.get('logic')
    if logic is not None:
        payload["logic"] = logic

    # Strip annotation_id and question if they somehow made it in
    if 'annotation_id' in payload:
        del payload['annotation_id']
    if 'question' in payload:
        del payload['question']

    payload_str = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_str.encode('utf-8')).hexdigest()