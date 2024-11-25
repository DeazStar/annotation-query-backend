def validate_request(request, schema):
    if 'nodes' not in request:
        raise Exception("nodes array is missing")

    nodes = request['nodes']
    if not isinstance(nodes, list):
        raise Exception("nodes should be a list")

    node_map = {}
    for node in nodes:
        if not isinstance(node, dict):
            raise Exception("Each node must be a dictionary")
        if 'id' not in node:
            raise Exception("id is required!")
        if 'type' not in node or not node['type']:
            raise Exception("type is required")
        if 'node_id' not in node or not node['node_id']:
            raise Exception("node_id is required")
        
        node.setdefault('properties', {})
        if 'chr' in node["properties"]:
            chr_property = str(node["properties"]["chr"])
            if chr_property and not chr_property.startswith('chr'):
                node["properties"]["chr"] = 'chr' + chr_property
        
        if node['node_id'] in node_map:
            raise Exception(f"Repeated node_id: {node['node_id']}")
        node_map[node['node_id']] = node

    # Validate predicates
    predicates = request.get('predicates', [])
    if not isinstance(predicates, list):
        raise Exception("predicates should be a list")
    
    for predicate in predicates:
        if 'id' not in predicate or not predicate['id']:
            raise Exception("predicate id is required")
        if 'type' not in predicate or not predicate['type']:
            raise Exception("predicate type is required")
        if 'source' not in predicate or not predicate['source']:
            raise Exception("source is required")
        if 'target' not in predicate or not predicate['target']:
            raise Exception("target is required")
        if predicate['source'] not in node_map:
            raise Exception(f"Source node {predicate['source']} does not exist")
        if predicate['target'] not in node_map:
            raise Exception(f"Target node {predicate['target']} does not exist")

    # Validate logic
    logic = request.get('logic', {})
    if not isinstance(logic, dict):
        raise Exception("logic should be a dictionary")
    
    children = logic.get('children', [])
    if not isinstance(children, list):
        raise Exception("logic.children should be a list")

    for child in children:
        if not isinstance(child, dict):
            raise Exception("Each logic child must be a dictionary")
        if 'operator' not in child or not child['operator']:
            raise Exception("operator is required in each logic child")
        if child['operator'] == 'NOT' and 'nodes' not in child:
            raise Exception("nodes are required for NOT operator")
    
    return node_map
