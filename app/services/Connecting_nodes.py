import json
from collections import defaultdict, deque
import re 

from .schema_data import get_final_json

data = get_final_json()   
data = json.loads(data)   
 
def find_paths(data, source, target, id=None, properties=None):
   
    graph = defaultdict(list)
     
    for relation, mapping in data['result'].items():
        src_type = mapping["source"]
        tgt_type = mapping["target"]
        graph[src_type].append((tgt_type, relation))
         
    
    # BFS to find all paths from source to target
    def bfs_paths(graph, source, target):
        queue = deque([(source, [source])])
        
        all_paths = []
        while queue:
            current_node, path = queue.popleft()
             
            if current_node == target:
                if path not in all_paths:
                    all_paths.append(path)
            for neighbor, relation in graph.get(current_node, []):
                if neighbor not in path:  # Avoid cycles
                    queue.append((neighbor, path + [neighbor] ))
                     
        return all_paths

    paths = bfs_paths(graph, source, target)
    
    print("path",paths)
    process_paths(paths, graph)

    
def process_paths(paths, graph):
    results = []

    for path in paths:
        node_map = {}
        nodes = []
        predicates = []
        node_id_counter = 1
        predicate_id_counter = 1

        for i in range(len(path)):
            node = path[i]
           
            if node not in node_map:
                node_id = f"n{node_id_counter}"
                node_map[node] = node_id

                nodes.append({
                    "node_id": node_id,
                    "id": "",
                    "type": node,
                    "properties": {}
                })
                 
                node_id_counter += 1

             
            if i < len(path) - 1:
                next_node = path[i + 1]
                
                if next_node not in node_map:
                    node_id = f"n{node_id_counter}"
                    node_map[next_node] = node_id

                    nodes.append({
                        "node_id": node_id,
                        "id": "",
                        "type": next_node,
                        "properties": {}
                    })
                    node_id_counter += 1
                 
                relation = next(
                    (r for n, r in graph.get(path[i], []) if n == next_node),
                    None
                )
                 

                if relation:
                    predicates.append({
                        "predicate_id": f"p{predicate_id_counter}",
                        "type": clean_number(relation),
                        "source": node_map[path[i]],
                        "target": node_map[next_node]
                    })
                    predicate_id_counter += 1
                 
        results.append({
            "nodes": nodes,
            "predicates": predicates
        })
    

    return json.dumps(results, indent=2)     
def clean_number(experssion):
    return re.sub(r'\d+$','',experssion)
 
 


# Example usage: Find all paths from 'promoter' to 'protein'
result_json = find_paths(data, source="snp", target="uberon", id=None, properties=None)
 
 

