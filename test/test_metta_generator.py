import unittest
from unittest.mock import patch, MagicMock
from metta_generator import MeTTa_Query_Generator

class TestMeTTaQueryGenerator(unittest.TestCase):

    def setUp(self):
        self.dataset_path = "/path/to/dataset"
        with patch('os.path.exists', return_value=True), \
             patch('glob.glob', return_value=['/path/to/dataset/file1.metta']):
            self.generator = MeTTa_Query_Generator(self.dataset_path)

    def test_initialize_space(self):
        with patch.object(self.generator.metta, 'run') as mock_run:
            self.generator.initialize_space()
            mock_run.assert_called_once_with("!(bind! &space (new-space))")

    def test_load_dataset(self):
        with patch('os.path.exists', return_value=True), \
             patch('glob.glob', return_value=['/path/to/dataset/file1.metta', '/path/to/dataset/file2.metta']), \
             patch.object(self.generator.metta, 'run') as mock_run:
            self.generator.load_dataset(self.dataset_path)
            self.assertEqual(mock_run.call_count, 2)

    def test_load_dataset_no_files(self):
        with patch('os.path.exists', return_value=True), \
             patch('glob.glob', return_value=[]):
            with self.assertRaises(ValueError):
                self.generator.load_dataset(self.dataset_path)

    def test_generate_id(self):
        id1 = self.generator.generate_id()
        id2 = self.generator.generate_id()
        self.assertNotEqual(id1, id2)
        self.assertEqual(len(id1), 8)
        self.assertEqual(len(id2), 8)

    def test_construct_node_representation(self):
        node = {
            'type': 'Person',
            'properties': {
                'name': 'John',
                'age': '30'
            }
        }
        identifier = '$person1'
        expected = ' (name (Person $person1) John) (age (Person $person1) 30)'
        result = self.generator.construct_node_representation(node, identifier)
        self.assertEqual(result, expected)

    def test_query_Generator(self):
        data = {
            'nodes': [
                {'type': 'Person', 'node_id': '1', 'id': 'John', 'properties': {}},
                {'type': 'City', 'node_id': '2', 'id': '', 'properties': {'name': 'New York'}}
            ],
            'predicates': [
                {'type': 'LIVES_IN', 'source': '1', 'target': '2'}
            ]
        }
        node_map = {
            '1': {'type': 'Person', 'id': 'John', 'properties': {}},
            '2': {'type': 'City', 'id': '', 'properties': {'name': 'New York'}}
        }
        result = self.generator.query_Generator(data, node_map)
        expected = "!(match &space (, (Person John) (name (City $2) New York) (LIVES_IN (Person John) (City $2)) (, (LIVES_IN (Person John) (City $2)))))"
        self.assertEqual(result, expected)

    @patch('hyperon.MeTTa')
    def test_run_query(self, mock_metta):
        mock_metta.return_value.run.return_value = "query result"
        result = self.generator.run_query("test query")
        self.assertEqual(result, "query result")

    def test_parse_and_serialize(self):
        input_data = MagicMock()
        schema = {'Person': {'properties': {'name': 'string'}}}
        all_properties = True
        
        with patch.object(self.generator, 'prepare_query_input', return_value=[MagicMock()]), \
             patch.object(self.generator, 'process_result', return_value=(["result"], {}, {})):
            result = self.generator.parse_and_serialize(input_data, schema, all_properties)
            self.assertEqual(result, ["result"])

    def test_get_node_properties(self):
        results = [
            {'source': 'Person John', 'target': 'City NewYork', 'predicate': 'LIVES_IN'}
        ]
        schema = {
            'Person': {'properties': {'name': 'string', 'age': 'int'}},
            'City': {'properties': {'name': 'string'}},
            'Person-LIVES_IN-City': {'properties': {'since': 'date'}}
        }
        result = self.generator.get_node_properties(results, schema)
        self.assertIn("!(match &space (,", result)
        self.assertIn("(node name (Person John)", result)
        self.assertIn("(node age (Person John)", result)
        self.assertIn("(node name (City NewYork)", result)
        self.assertIn("(edge since (LIVES_IN (Person John) (City NewYork))", result)

    def test_metta_seralizer(self):
        mock_result = [
            MagicMock(get_children=lambda: [
                MagicMock(get_name=lambda: ","),
                MagicMock(get_children=lambda: [
                    MagicMock(get_name=lambda: "node"),
                    MagicMock(get_name=lambda: "name"),
                    MagicMock(get_name=lambda: "Person"),
                    MagicMock(get_name=lambda: "John")
                ])
            ])
        ]
        result = self.generator.metta_seralizer(mock_result)
        self.assertEqual(result, [('node', 'name', 'Person', 'John')])

    def test_convert_to_dict(self):
        results = MagicMock()
        schema = {'Person': {'properties': {'name': 'string'}}}
        
        with patch.object(self.generator, 'prepare_query_input', return_value=[MagicMock()]), \
             patch.object(self.generator, 'process_result', return_value=(None, {'Person': []}, {'LIVES_IN': []})):
            node_dict, edge_dict = self.generator.convert_to_dict(results, schema)
            self.assertEqual(node_dict, {'Person': []})
            self.assertEqual(edge_dict, {'LIVES_IN': []})

    def test_parse_id(self):
        request = {
            "nodes": [
                {"type": "gene", "id": "BRCA1", "properties": {}},
                {"type": "transcript", "id": "ENST00000123456", "properties": {}}
            ]
        }
        result = self.generator.parse_id(request)
        self.assertEqual(result["nodes"][0]["properties"]["gene_name"], "BRCA1")
        self.assertEqual(result["nodes"][0]["id"], "")
        self.assertEqual(result["nodes"][1]["id"], "ENST00000123456")

if __name__ == '__main__':
    unittest.main()