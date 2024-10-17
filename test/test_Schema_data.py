import unittest
from unittest.mock import patch, MagicMock
from schema_manager import SchemaManager
import yaml

class TestSchemaManager(unittest.TestCase):

    def setUp(self):
        self.schema_config_path = 'schema_config.yaml'
        self.biocypher_config_path = 'biocypher_config.yaml'
        
        # Mock BioCypher and its methods
        with patch('schema_manager.BioCypher') as MockBioCypher:
            mock_bcy = MockBioCypher.return_value
            mock_bcy._get_ontology_mapping.return_value._extend_schema.return_value = {
                'Gene': {
                    'represented_as': 'node',
                    'input_label': 'Gene',
                    'is_a': 'BiologicalEntity',
                    'properties': {'name': 'string'}
                },
                'Protein': {
                    'represented_as': 'node',
                    'input_label': 'Protein',
                    'is_a': 'BiologicalEntity',
                    'properties': {'name': 'string'}
                },
                'INTERACTS_WITH': {
                    'represented_as': 'edge',
                    'input_label': 'INTERACTS_WITH',
                    'is_a': 'Interaction',
                    'source': 'Protein',
                    'target': 'Protein',
                    'properties': {'score': 'float'}
                }
            }
            self.schema_manager = SchemaManager(self.schema_config_path, self.biocypher_config_path)

    def test_process_schema(self):
        result = self.schema_manager.schema
        self.assertEqual(len(result), 3)
        self.assertIn('Gene', result)
        self.assertIn('Protein', result)
        self.assertIn('Protein-INTERACTS_WITH-Protein', result)
        self.assertEqual(result['Gene']['key'], 'Gene')
        self.assertEqual(result['Protein-INTERACTS_WITH-Protein']['key'], 'Protein-INTERACTS_WITH-Protein')

    def test_parent_nodes(self):
        result = self.schema_manager.parent_nodes
        self.assertEqual(result, ['BiologicalEntity'])

    def test_parent_edges(self):
        result = self.schema_manager.parent_edges
        self.assertEqual(result, ['Interaction'])

    def test_get_nodes(self):
        result = self.schema_manager.get_nodes()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['parent_node'], 'BiologicalEntity')
        self.assertEqual(len(result[0]['child_nodes']), 2)
        child_types = [node['type'] for node in result[0]['child_nodes']]
        self.assertIn('Gene', child_types)
        self.assertIn('Protein', child_types)

    def test_get_edges(self):
        result = self.schema_manager.get_edges()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['parent_edge'], 'Interaction')
        self.assertEqual(len(result[0]['child_edges']), 1)
        self.assertEqual(result[0]['child_edges'][0]['type'], 'Protein-INTERACTS_WITH-Protein')
        self.assertEqual(result[0]['child_edges'][0]['source'], 'Protein')
        self.assertEqual(result[0]['child_edges'][0]['target'], 'Protein')

    def test_get_relations_for_node(self):
        result = self.schema_manager.get_relations_for_node('Protein')
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['type'], 'Protein-INTERACTS_WITH-Protein')
        self.assertEqual(result[0]['source'], 'Protein')
        self.assertEqual(result[0]['target'], 'Protein')

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data=yaml.dump({
        'Gene': {'source': 'HGNC', 'target': 'Symbol'},
        'Protein': {'source': 'UniProt', 'target': 'AccessionID'}
    }))
    def test_get_schema(self, mock_open):
        result = SchemaManager.get_schema()
        self.assertEqual(len(result), 2)
        self.assertIn('Gene', result)
        self.assertIn('Protein', result)
        self.assertEqual(result['Gene']['source'], 'HGNC')
        self.assertEqual(result['Gene']['target'], 'Symbol')
        self.assertEqual(result['Protein']['source'], 'UniProt')
        self.assertEqual(result['Protein']['target'], 'AccessionID')

if __name__ == '__main__':
    unittest.main()