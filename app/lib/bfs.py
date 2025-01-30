from app import schema_manager
from collections import deque, defaultdict


class Bfs:

    def __init__(self, schema):
        self.edge_list = self.get_edges(schema)
        self.edge_mapping = self.parse_edge(self.edge_list)
        self.adjeceny_dict = self.get_adjecency_dict(self.edge_mapping)

    def get_edges(self, schema: dict):

        output = []
        for i, v in schema.items():
    
            if v['represented_as'] == 'edge':
                if 'source' in v and 'target' in v:
                    if not isinstance(v['target'], list): 
                        output.append((i, (v['source'], v['target'])))
                    else: # to destructure cases where target is a list
                        for t in v['target']:
                            output.append((i, (v['source'], t)))

        return output

    def parse_edge(self, inputs): 
    
        output = {}
        for i in inputs:
            edge = i[0]
            source = i[1][0]
            target = i[1][1]
    
            edge = edge.removeprefix(f"{source}_")
            edge = edge.removesuffix(f"_{target}")
    
            if isinstance(target, list): # to guard against classes that have a list as a target
                i[1][1] = tuple(target)
    
            key = tuple(i[1])
            output[key] = edge
    
    
        return output

    def get_adjecency_dict(self, edge_dict):
        
        adjecency_dict = {}
        
        for key in edge_dict.keys():
            if key[0] in adjecency_dict:
                adjecency_dict[key[0]].append(key[1])
            else:
                adjecency_dict[key[0]] = [key[1]]
    
            if key[1] not in adjecency_dict: # to handle case against nodes that are always target nodes
                adjecency_dict[key[1]] = []
    
        return adjecency_dict
            


    def bfs_all_paths(self, source, target):

        adjecency_dict = self.adjeceny_dict 
        queue = deque([[source]])  # Queue holds paths instead of just nodes
        all_paths = []  # List to store all valid paths
    
        while queue:
            path = queue.popleft()  # Dequeue the current path
            node = path[-1]  # Get the last node in the path

            if node == target:
                all_paths.append(path)  # If target is reached, store the path
                continue  # Continue exploring other paths
    
            for neighbor in adjecency_dict[node]:
                if neighbor not in path:  # Avoid cycles in the same path
                    new_path = path + [neighbor]  # Create a new extended path
                    queue.append(new_path)  # Enqueue the new path
    
        return all_paths

    def generate_request(self, paths):

        node_template = {"node_id": "", "id": "", "type": ""} 
        predicate_template = {"predicate_id": "", "type": "", "source": "", "target": ""}
        requests = []
        
        for path in paths:
            nodes = []
            predicates = []
            request = {"requests": {}}
            p_index = 1
            for index, node in enumerate(path):

                node_dict = node_template.copy()
                node_dict['node_id'] =  f"n{index + 1}"
                node_dict['type'] = node
                nodes.append(node_dict)
    
                if len(path) > index + 1:
                    predicate_dict = predicate_template.copy()
                    target = path[index + 1]
                    predicate_dict['predicate_id'] = f"p{p_index}"
                    predicate_dict['type'] = self.edge_mapping[(node, target)]
                    predicate_dict['source'] = node 
                    predicate_dict['target'] = target
                    predicates.append(predicate_dict)
                    p_index += 1

            request['requests']['nodes'] = nodes
            request['requests']['predicates'] = predicates

            requests.append(request)

        return requests 
