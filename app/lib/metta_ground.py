from hyperon import GroundedAtom, S, Atom, MeTTa, OperationAtom, ExpressionAtom

class Metta_ground:

    def __init__(self, metta):
        self.metta = metta
        self.register_functions()

    def register_functions(self):
        # create not_node and not_id to be operational atoms
        not_node = OperationAtom("not_node", self.not_nodes, unwrap=False)
        not_id = OperationAtom("not_id", self.not_ids, unwrap=False) 
        
        # register the functions into the atom space
        self.metta.register_atom("not_node", not_node)
        self.metta.register_atom("not_id", not_id)

    
    def not_nodes(self, *nodes):
        query = f"!(match &space ($a $n1) ($a $n1))"
        results = self.metta.run(query)
        output = []
        for result in results[0]:
            if result.get_children()[0] not in nodes:
                output.append(result)
        return output
    
    def not_ids(self, node, node_id:str):
        query = f"!(match &space ({node} $n1) ({node} $n1))"
        results = self.metta.run(query)
        output = []
        for result in results[0]:
            if result.get_children()[1] != node_id:
                output.append(result)
        return output
