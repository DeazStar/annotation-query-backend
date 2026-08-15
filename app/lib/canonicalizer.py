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
            
        # Rewrite predicates using new IDs
        canonical_predicates = []
        for p in predicates:
            canonical_predicates.append({
                "source": id_mapping.get(p.get('source'), p.get('source')),
                "target": id_mapping.get(p.get('target'), p.get('target')),
                "type": p.get('type', ''),
                "properties": p.get('properties', {})
            })
            
        # Sort predicates to ensure stable order
        def predicate_sort_key(p):
            props = json.dumps(p.get('properties', {}), sort_keys=True)
            return (p.get('source'), p.get('target'), p.get('type'), props)
            
        canonical_predicates.sort(key=predicate_sort_key)
        
        # Final canonical object
        canonical_query = {
            "species": species,
            "data_source": data_source,
            "nodes": canonical_nodes,
            "predicates": canonical_predicates
        }
        
        # Stable JSON string and Hashing
        canonical_str = json.dumps(canonical_query, sort_keys=True)
        return hashlib.sha256(canonical_str.encode('utf-8')).hexdigest()
