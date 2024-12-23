from hyperon import GroundedAtom, S, Atom, MeTTa, OperationAtom, ExpressionAtom, E

class Metta_ground:

    def __init__(self, metta):
        self.metta = metta
        self.register_functions()

    def register_functions(self):
        # create not_node and not_id to be operational atoms
        not_node = OperationAtom("not_node", self.not_nodes, unwrap=False)
        not_id = OperationAtom("not_id", self.not_ids, unwrap=False) 
        not_property = OperationAtom("not_property", self.not_propertys, unwrap=False)

        # register the functions into the atom space
        self.metta.register_atom("not_node", not_node)
        self.metta.register_atom("not_id", not_id)
        self.metta.register_atom("not_property", not_property)
    
    def not_nodes(self, *nodes):
        query = f"!(match &space ($a $n1) ($a $n1))"
        results = self.metta.run(query)
        output = []
        for result in results[0]:
            if result.get_children()[0] not in nodes:
                output.append(result)

        return output
    
    def not_ids(self, node, *node_ids):
        query = f"!(match &space ({node} $n1) ({node} $n1))"
        results = self.metta.run(query)
        output = []
        for result in results[0]:
            if result.get_children()[1] not in node_ids:
                output.append(result)

        return output

    def not_propertys(self, node, n_property, value):

        properties = []
        if isinstance(n_property, ExpressionAtom):
            for child in n_property.get_children():
                properties.append(child)
        
        values = []
        if isinstance(value, ExpressionAtom):
            for child in value.get_children():
                values.append(child)

        pro_val = list(zip(properties, values))

        if len(properties) == 0:
            query = f"!(match &space ({n_property} ({node} $n1) {value}) ({node} $n1))"
        else:
            query = f"!(match &space (, "
            for p_v in pro_val:
                query += f"({p_v[0]} ({node} $n1) {p_v[1]}) "
            query += f") ({node} $n1))"

        
        results = self.metta.run(query)
        nodes = []
        for result in results[0]:
            nodes.append(str(result.get_children()[1]))

        query = f"!(not_id {node} " + " ".join(nodes) + ")"
        results = self.metta.run(query)

        return results[0] 
