import unittest
from abc import ABC
from query_generator import QueryGeneratorInterface # Replace 'your_module' with the actual module name

class TestQueryGeneratorInterface(unittest.TestCase):

    def test_is_abstract_base_class(self):
        self.assertTrue(issubclass(QueryGeneratorInterface, ABC))

    def test_abstract_methods(self):
        
        abstract_methods = [
            'query_Generator',
            'run_query',
            'parse_and_serialize',
            'convert_to_dict',
            'parse_id'
        ]
        for method in abstract_methods:
            self.assertTrue(hasattr(QueryGeneratorInterface, method))
            self.assertTrue(callable(getattr(QueryGeneratorInterface, method)))

    def test_method_signatures(self):
        # Test method signatures
        self.assertEqual(QueryGeneratorInterface.query_Generator.__annotations__,
                         {'data': None, 'schema': None, 'return': str})
        self.assertEqual(QueryGeneratorInterface.run_query.__annotations__,
                         {'query_code': None, 'return': list})
        self.assertEqual(QueryGeneratorInterface.parse_and_serialize.__annotations__,
                         {'input': None, 'schema': None, 'all_properties': None, 'return': list})
        self.assertEqual(QueryGeneratorInterface.convert_to_dict.__annotations__,
                         {'results': None, 'schema': None, 'return': tuple})
        self.assertEqual(QueryGeneratorInterface.parse_id.__annotations__,
                         {'request': None, 'return': dict})

    def test_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            QueryGeneratorInterface()

    def test_concrete_implementation(self):
        class ConcreteQueryGenerator(QueryGeneratorInterface):
            def query_Generator(self, data, schema) -> str:
                return "query"

            def run_query(self, query_code) -> list:
                return []

            def parse_and_serialize(self, input, schema, all_properties) -> list:
                return []

            def convert_to_dict(self, results, schema) -> tuple:
                return ({}, {})

            def parse_id(self, request) -> dict:
                return {}

        # This should not raise any errors
        concrete = ConcreteQueryGenerator()
        self.assertIsInstance(concrete, QueryGeneratorInterface)

    def test_incomplete_implementation(self):
        class IncompleteQueryGenerator(QueryGeneratorInterface):
            pass

        with self.assertRaises(TypeError):
            IncompleteQueryGenerator()

if __name__ == '__main__':
    unittest.main()