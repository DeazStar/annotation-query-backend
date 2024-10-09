from map_graph import map_graph

class TestCase:
    def __init__(self):
        self.tests_run = 0
        self.tests_failed = 0

    def assertEqual(self, first, second, msg=None):
        self.tests_run += 1
        if first != second:
            self.tests_failed += 1
            print(f"AssertionError: {msg or ''}")
            print(f"Expected: {first}")
            print(f"Got: {second}")

    def run_test(self, test_func):
        test_func(self)

def test_simple_graph(test):
    graph = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "2"}},
            {"data": {"source": "2", "target": "3"}}
        ]
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [[0], [1], []], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {"1": 0, "2": 1, "3": 2}, "Node ID to index mapping mismatch")

def test_graph_with_isolated_node(test):
    graph = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}},
            {"data": {"id": "4"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "2"}},
            {"data": {"source": "2", "target": "3"}}
        ]
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [[0], [1], [], []], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [3], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {"1": 0, "2": 1, "3": 2, "4": 3}, "Node ID to index mapping mismatch")

def test_empty_graph(test):
    graph = {
        "nodes": [],
        "edges": []
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {}, "Node ID to index mapping mismatch")

def test_graph_with_only_nodes(test):
    graph = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}}
        ],
        "edges": []
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [[], [], []], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [0, 1, 2], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {"1": 0, "2": 1, "3": 2}, "Node ID to index mapping mismatch")

def test_graph_with_multiple_edges(test):
    graph = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "2"}},
            {"data": {"source": "1", "target": "3"}},
            {"data": {"source": "2", "target": "3"}},
            {"data": {"source": "3", "target": "1"}}
        ]
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [[0, 1], [2], [3]], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {"1": 0, "2": 1, "3": 2}, "Node ID to index mapping mismatch")

def test_graph_with_self_loop(test):
    graph = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "1"}},
            {"data": {"source": "1", "target": "2"}}
        ]
    }
    edge_indices, single_node_idx, node_id_to_index = map_graph(graph)
    
    test.assertEqual(edge_indices, [[0, 1], []], "Edge indices mismatch")
    test.assertEqual(single_node_idx, [], "Single node indices mismatch")
    test.assertEqual(node_id_to_index, {"1": 0, "2": 1}, "Node ID to index mapping mismatch")

def run_tests():
    test_case = TestCase()
    test_functions = [
        test_simple_graph,
        test_graph_with_isolated_node,
        test_empty_graph,
        test_graph_with_only_nodes,
        test_graph_with_multiple_edges,
        test_graph_with_self_loop
    ]
    
    for test_func in test_functions:
        test_case.run_test(test_func)
    
    print(f"\nTests run: {test_case.tests_run}")
    print(f"Tests failed: {test_case.tests_failed}")
    
    if test_case.tests_failed == 0:
        print("All tests passed!")
    else:
        print(f"Some tests failed. Please check the output above.")

if __name__ == '__main__':
    run_tests()