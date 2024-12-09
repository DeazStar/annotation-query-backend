def validate_request(request, schema):
    if 'nodes' not in request:
        raise Exception("node is missing")

    nodes = request['nodes']
        
    # validate nodes
    if not isinstance(nodes, list):
        raise Exception("nodes should be a list")

    for node in nodes:
        if not isinstance(node, dict):
            raise Exception("Each node must be a dictionary")
        if 'id' not in node:
            raise Exception("id is required!")
        if 'type' not in node or node['type'] == "":
            raise Exception("type is required")
        if 'node_id' not in node or node['node_id'] == "":
            raise Exception("node_id is required")
        
        node.setdefault('properties', {})
       
        if 'chr' in node["properties"]:
            chr_property = node["properties"]["chr"]
            chr_property = str(chr_property)
            if chr_property and not chr_property.startswith('chr'):
                node["properties"]["chr"] = 'chr' + chr_property

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
            raise Exception('Repeated Node_id')

    # validate predicates
    if 'predicates' in request:
        predicates = request['predicates']
        predicate_ids = []

        if not isinstance(predicates, list):
            raise Exception("Predicate should be a list")
        for predicate in predicates:
            if "predicate_id" in predicate:
                predicate_ids.append(predicate["predicate_id"])

            if 'type' not in predicate or predicate['type'] == "":
                raise Exception("predicate type is required")
            if 'source' not in predicate or predicate['source'] == "":
                raise Exception("source is required")
            if 'target' not in predicate or predicate['target'] == "":
                raise Exception("target is required")

            if predicate['source'] not in node_map:
                raise Exception(f"Source node {predicate['source']} does not exist in the nodes object")
            if predicate['target'] not in node_map:
                raise Exception(f"Target node {predicate['target']} does not exist in the nodes object")
            
            # format the predicate type using _
            predicate_type = predicate['type'].split(' ')
            predicate_type = '_'.join(predicate_type)
            
            source_type = node_map[predicate['source']]['type']
            target_type = node_map[predicate['target']]['type']

            predicate_type = f'{source_type}-{predicate_type}-{target_type}'
            if predicate_type not in schema:
                raise Exception(f"Invalid source and target for the predicate {predicate['type']}")
    
    # validate the logic if present
    if 'logic' in request:
        logic = request['logic']

        if not isinstance(logic, dict):
            raise Exception("logic should be a dict")

        if 'children' not in logic:
            raise Exception("children Key is required")

        children = logic["children"]
        if not isinstance(children, list):
            raise Exception("children should be a list")

        for child in children:
            if not isinstance(child, dict):
                raise Exception("child should be a dict")

            if 'operator' not in child:
                raise Exception("operator key is required")
            if not isinstance(child['operator'], str):
                raise Exception("operator value must be instance of string")
            if child['operator'] not in ["NOT"]:
                raise Exception("operator value must be in NOT")

            if "nodes" in child:
                if not isinstance(child["nodes"], dict):
                    raise Exception("nodes value must be a dict")

                nodes = child["nodes"]
            
                if "node_id" not in nodes:
                    raise Exception("nodes value must have node_id key")
                if not isinstance(nodes["node_id"], str):
                    raise Exception("node_id must be an instance of a string")
                if nodes["node_id"] not in node_map:
                    raise Exception("invalid node_id: no matching node_id found in the declared nodes above")

                if "properties" in nodes:
                    
                    if not isinstance(nodes["properties"], dict):
                        raise Exception("properties should be a dict")
                    
            if "predicates" in child:
                
                if not isinstance(child["predicates"], dict):
                    raise Exception("predicates value must be a dict")

                predicates = child["predicates"]
                if "predicate_id" not in predicates:
                    raise Exception("predicate value must have a prdicate_id key")
                if not isinstance(nodes["predicate_id"], str):
                    raise Exception("predicare_id value must be an instance of a string")
                if predicates["predicate_id"] not in predicate_ids:
                    raise Exception("invalid predicate_id: no matching predicate_id fournd in the decalred predicates above")


    return node_map
