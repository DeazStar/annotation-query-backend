import json
import hashlib

class Canonicalizer:
    @staticmethod
    def canonicalize(request_data, species, data_source):
        """
        Normalizes a graph query request into a stable hash.
        """
        nodes = request_data.get('nodes', [])
        predicates = request_data.get('predicates', [])
        
        # Build sorting key and sort nodes
        def node_sort_key(node):
            node_type = node.get('type', '')
            props = json.dumps(node.get('properties', {}), sort_keys=True)
            return (node_type, props)
        
        sorted_nodes = sorted(nodes, key=node_sort_key)
        
        # Map old IDs to canonical IDs (node_0, node_1...)
        id_mapping = {}
        canonical_nodes = []
        for i, node in enumerate(sorted_nodes):
            old_id = node.get('id')
            new_id = f"node_{i}"
            id_mapping[old_id] = new_id
            
            canonical_nodes.append({
                "id": new_id,
                "type": node.get('type', ''),
                "properties": node.get('properties', {})
            })
            
 