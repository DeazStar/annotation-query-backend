from limit_graph import limit_graph, map_graph

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

def test_limit_graph(test):
    # Test case 1: Graph with more nodes than threshold
    graph1 = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}},
            {"data": {"id": "4"}},
            {"data": {"id": "5"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "2"}},
            {"data": {"source": "2", "target": "3"}},
            {"data": {"source": "3", "target": "4"}},
            {"data": {"source": "4", "target": "5"}}
        ]
    }
    threshold1 = 3
    result1 = limit_graph(graph1, threshold1)
    test.assertEqual(len(result1["nodes"]), 3, "Number of nodes should be limited to threshold")
    test.assertEqual(len(result1["edges"]), 2, "Number of edges should be limited")

    # Test case 2: Graph with fewer nodes than threshold
    graph2 = {
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
    threshold2 = 5
    result2 = limit_graph(graph2, threshold2)
    test.assertEqual(len(result2["nodes"]), 3, "All nodes should be included when below threshold")
    test.assertEqual(len(result2["edges"]), 2, "All edges should be included when below threshold")

    # Test case 3: Graph with isolated nodes
    graph3 = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}},
            {"data": {"id": "4"}},
            {"data": {"id": "5"}}
        ],
        "edges": [
            {"data": {"source": "1", "target": "2"}},
            {"data": {"source": "2", "target": "3"}}
        ]
    }
    threshold3 = 4
    result3 = limit_graph(graph3, threshold3)
    test.assertEqual(len(result3["nodes"]), 4, "Should include connected nodes and some isolated nodes")
    test.assertEqual(len(result3["edges"]), 2, "All edges should be included")

    # Test case 4: Empty graph
    graph4 = {
        "nodes": [],
        "edges": []
    }
    threshold4 = 5
    result4 = limit_graph(graph4, threshold4)
    test.assertEqual(len(result4["nodes"]), 0, "Empty graph should remain empty")
    test.assertEqual(len(result4["edges"]), 0, "Empty graph should have no edges")

    # Test case 5: Graph with only isolated nodes
    graph5 = {
        "nodes": [
            {"data": {"id": "1"}},
            {"data": {"id": "2"}},
            {"data": {"id": "3"}},
            {"data": {"id": "4"}},
            {"data": {"id": "5"}}
        ],
        "edges": []
    }
    threshold5 = 3
    result5 = limit_graph(graph5, threshold5)
    test.assertEqual(len(result5["nodes"]), 3, "Should include only threshold number of isolated nodes")
    test.assertEqual(len(result5["edges"]), 0, "Should have no edges")

def run_tests():
    test_case = TestCase()
    test_functions = [
        test_limit_graph
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