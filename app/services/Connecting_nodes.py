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
         
    def shortest_path(graph, start,end):
        queue= deque([(start, [start])])
        visted=set()
        while queue:
            current_node,path=queue.popleft()
            if current_node == end:
                return path 
            if current_node not in  visted:
                visted.add(current_node)
                for neighbor,relation in graph.get(current_node,[]):
                    if neighbor not in visted:
                        queue.append((neighbor,path+[neighbor]))
        return []


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

def find_shortest_path(paths):
    return min(paths, key=len)    
def process_paths(paths, graph):
   
    shortest_path = find_shortest_path(paths)
    ordered_paths = [shortest_path] + [path for path in paths if path != shortest_path]   
    results = []

    for path in ordered_paths:
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
    
    print(json.dumps(results, indent=2))
    return json.dumps(results, indent=2)     
def clean_number(experssion):
    return re.sub(r'\d+$','',experssion)
 
 


# Example usage: Find all paths from 'promoter' to 'protein'
result_json = find_paths(data, source="snp", target="uberon", id=None, properties=None)
 
 

