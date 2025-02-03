import json
from collections import defaultdict, deque
import re 
from .schema_data import SchemaManager
from .schema_data import get_final_json

data=get_final_json()
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
                all_paths.append(path)
            for neighbor, relation in graph.get(current_node, []):
                if neighbor not in path:  # Avoid cycles
                    queue.append((neighbor, path + [neighbor]))
                     
        return all_paths

    paths = bfs_paths(graph, source, target)
    print("path",paths)

    
    node_map = {}
    nodes = []
    predicates = []
    node_id_counter = 1
    predicate_id_counter = 1

    for path in paths:
        for i in range(len(path)):
            node = path[i]

            # Ensure node exists in node_map
            if node not in node_map:
                node_id = f"n{node_id_counter}"
                node_map[node] = node_id

                nodes.append({
                    "node_id": node_id,
                    "id": id if id else "",
                    "type": node,
                    "properties": properties if properties else {}
                })
                node_id_counter += 1

            # Ensure path[i+1] exists before using it
            if i < len(path) - 1 and path[i + 1] not in node_map:
                next_node = path[i + 1]
                node_id = f"n{node_id_counter}"
                node_map[next_node] = node_id

                nodes.append({
                    "node_id": node_id,
                    "id": id if id else "",
                    "type": next_node,
                    "properties": properties if properties else {}
                })
                node_id_counter += 1

            if i < len(path) - 1:
                relation = next(
                    (r for n, r in graph.get(path[i], []) if n == path[i + 1]), None
                )

                # Ensure relation is not None
                if relation is not None:
                    predicates.append({
                        "predicate_id": f"p{predicate_id_counter}",
                        "type": clean_number(relation),
                        "source": node_map[path[i]],
                        "target": node_map[path[i + 1]]  # Fixed dictionary syntax
                    })
                    predicate_id_counter += 1

    result = {
        "nodes": nodes,
        "predicates": predicates
    }
    return json.dumps(result, indent=2)

def clean_number(experssion):
    return re.sub(r'\d+$','',experssion)
 
 


# Example usage: Find all paths from 'promoter' to 'protein'
result_json = find_paths(data, source="gene", target="protein", id=None, properties=None)
print(result_json)
