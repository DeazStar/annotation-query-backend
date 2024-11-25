from app.services.query_generator_interface import QueryGeneratorInterface

class Query(QueryGeneratorInterface):
    def __init__(self):
        pass

    def query_Generator(self, requests, node_map):
        nodes = requests['nodes']
        predicates = requests.get("predicates", [])
        logic = requests.get("logic", None)
        
        cypher_queries = []
        match_clauses = []
        where_clauses = []
        return_clauses = []

        used_nodes = set()  # Track nodes used in predicates
        node_ids=set()
        # Process nodes without predicates
        if not predicates:
            for node in nodes:
                var_name = f"n_{node['node_id']}"
                match_clauses.append(self.match_node(node, var_name))
                if node['properties']:
                    where_clauses.extend(self.where_construct(node, var_name))
                return_clauses.append(var_name)
            cypher_query = self.construct_clause(match_clauses, return_clauses, where_clauses)
            cypher_queries.append(cypher_query)
        else:
            # Process predicates
            match_predicates = []
            return_predicates = []
            where_predicates = []

            for i, predicate in enumerate(predicates):
                predicate_type = predicate['type'].replace(" ", "_").lower()
                source_var = f"n_{predicate['source']}"
                target_var = f"n_{predicate['target']}"
                source_node = node_map[predicate['source']]
                target_node = node_map[predicate['target']]

                match_predicates.append(self.match_node(source_node, source_var))
                where_predicates.extend(self.where_construct(source_node, source_var))
                match_predicates.append(self.match_node(target_node, target_var))
                where_predicates.extend(self.where_construct(target_node, target_var))
                match_predicates.append(f"({source_var})-[r{i}:{predicate_type}]->({target_var})")
                return_predicates.append(f"r{i}")
                node_ids.add(source_var)
                node_ids.add(target_var)
                used_nodes.update({predicate['source'], predicate['target']})

            # Handle unused nodes
            for node_id, node in node_map.items():
                if node_id not in used_nodes:
                    var_name = f"n_{node_id}"
                    match_clauses.append(self.match_node(node, var_name))
                    where_clauses.extend(self.where_construct(node, var_name))
                    return_clauses.append(var_name)
            list_of_node_ids = list(node_ids)
            list_of_node_ids.sort()
            return_predicates.extend(list(list_of_node_ids))
            # Process logic conditions
            if logic:
                logic_condition = self.construct_logic(logic, predicates)
                where_predicates.append(logic_condition)

            # Construct query for predicates
            cypher_query = self.construct_clause(match_predicates, return_predicates, where_predicates)
            cypher_queries.append(cypher_query)

            # Construct query for unused nodes
            if match_clauses:
                union_query = self.construct_union_clause(
                    match_predicates, return_predicates, where_predicates,
                    match_clauses, return_clauses, where_clauses
                )
                cypher_queries.append(union_query)

        return cypher_queries

    def construct_logic(self, logic, predicates):
        def build_condition(logic_node):
            operator = logic_node.get("operator")
            if operator == "AND":
                return " AND ".join([self.predicate_to_condition(p, predicates) for p in logic_node["predicates"]])
            elif operator == "OR":
                return " OR ".join([self.predicate_to_condition(p, predicates) for p in logic_node["predicates"]])
            elif operator == "NOT":
                node_id = logic_node["nodes"]["node_id"]
                return f"NOT (n_{node_id})"
            return ""

        conditions = [build_condition(child) for child in logic.get("children", [])]
        return f"({' AND '.join(conditions)})"

    def predicate_to_condition(self, predicate_id, predicates):
        predicate = next(p for p in predicates if p["id"] == predicate_id)
        source_var = f"n_{predicate['source']}"
        target_var = f"n_{predicate['target']}"
        predicate_type = predicate['type'].replace(" ", "_").lower()
        return f"({source_var})-[:{predicate_type}]->({target_var})"

    def match_node(self, node, var_name):
        if node['id']:
            return f"({var_name}:{node['type']} {{id: '{node['id']}'}})"
        return f"({var_name}:{node['type']})"

    def where_construct(self, node, var_name):
        properties = []
        for key, value in node.get('properties', {}).items():
            properties.append(f"{var_name}.{key} =~ '(?i){value}'")
        return properties

    def construct_clause(self, match_clause, return_clause, where_clauses):
        match_clause = f"MATCH {', '.join(match_clause)}"
        return_clause = f"RETURN {', '.join(return_clause)}"
        if where_clauses:
            where_clause = f"WHERE {' AND '.join(where_clauses)}"
            return f"{match_clause} {where_clause} {return_clause}"
        return f"{match_clause} {return_clause}"

    def construct_union_clause(self, match_preds, return_preds, where_preds, match_no_preds, return_no_preds, where_no_preds):
        match_preds = f"MATCH {', '.join(match_preds)}"
        return_preds_clause = f"RETURN {', '.join(return_preds)}"
        where_preds_clause = f"WHERE {' AND '.join(where_preds)}" if where_preds else ""

        match_no_preds = f"MATCH {', '.join(match_no_preds)}"
        return_no_preds_clause = f"RETURN {', '.join(return_no_preds)}"
        where_no_preds_clause = f"WHERE {' AND '.join(where_no_preds)}" if where_no_preds else ""

        return f"{match_preds} {where_preds_clause} {return_preds_clause} UNION {match_no_preds} {where_no_preds_clause} {return_no_preds_clause}"
