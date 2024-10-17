import unittest
from  validator import validate_request

class TestValidateRequest(unittest.TestCase):
    def test_valid_request(self):
        schema = {
            'Person-KNOWS-Person': {},
            'Person': {'properties': {'name': str, 'age': int}}
        }
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1', 'properties': {'name': 'Alice', 'age': 30}},
                {'id': '2', 'type': 'Person', 'node_id': 'p2', 'properties': {'name': 'Bob', 'age': 25}}
            ],
            'predicates': [
                {'type': 'KNOWS', 'source': 'p1', 'target': 'p2'}
            ]
        }
        result = validate_request(request, schema)
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 2)

    def test_missing_nodes(self):
        schema = {}
        request = {}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "node is missing")

    def test_invalid_nodes_type(self):
        schema = {}
        request = {'nodes': {}}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "nodes should be a list")

    def test_invalid_node_type(self):
        schema = {}
        request = {'nodes': [[]]}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "Each node must be a dictionary")

    def test_missing_node_id(self):
        schema = {}
        request = {'nodes': [{'type': 'Person'}]}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "id is required!")

    def test_missing_node_type(self):
        schema = {}
        request = {'nodes': [{'id': '1', 'node_id': 'p1'}]}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "type is required")

    def test_missing_node_node_id(self):
        schema = {}
        request = {'nodes': [{'id': '1', 'type': 'Person'}]}
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "node_id is required")

    def test_invalid_predicates_type(self):
        schema = {}
        request = {
            'nodes': [{'id': '1', 'type': 'Person', 'node_id': 'p1'}],
            'predicates': {}
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "Predicate should be a list")

    def test_missing_predicate_type(self):
        schema = {}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'source': 'p1', 'target': 'p2'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "predicate type is required")

    def test_missing_predicate_source(self):
        schema = {}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'type': 'KNOWS', 'target': 'p2'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "source is required")

    def test_missing_predicate_target(self):
        schema = {}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'type': 'KNOWS', 'source': 'p1'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "target is required")

    def test_invalid_predicate_source(self):
        schema = {'Person-KNOWS-Person': {}}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'type': 'KNOWS', 'source': 'p3', 'target': 'p2'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "Source node p3 does not exist in the nodes object")

    def test_invalid_predicate_target(self):
        schema = {'Person-KNOWS-Person': {}}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'type': 'KNOWS', 'source': 'p1', 'target': 'p3'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "Target node p3 does not exist in the nodes object")

    def test_invalid_predicate_schema(self):
        schema = {'Person-LIKES-Person': {}}
        request = {
            'nodes': [
                {'id': '1', 'type': 'Person', 'node_id': 'p1'},
                {'id': '2', 'type': 'Person', 'node_id': 'p2'}
            ],
            'predicates': [{'type': 'KNOWS', 'source': 'p1', 'target': 'p2'}]
        }
        with self.assertRaises(Exception) as context:
            validate_request(request, schema)
        self.assertEqual(str(context.exception), "Invalid source and target for the predicate KNOWS")

def run_tests():
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestValidateRequest)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result

if __name__ == '__main__':
    result = run_tests()
    if result.wasSuccessful():
        print("All tests passed!")
    else:
        print(f"Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")