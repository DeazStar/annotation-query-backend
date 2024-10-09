import unittest
from unittest.mock import patch, MagicMock
from flask import Flask
from app import app, databases, schema_manager
from app.services.schema_data import SchemaManager
from app.services.cypher_generator import CypherQueryGenerator
from app.services.metta_generator import MeTTa_Query_Generator

class TestAppSetup(unittest.TestCase):

    @patch('app.Flask')
    def test_flask_app_creation(self, mock_flask):
        self.assertIsInstance(app, Flask)
        mock_flask.assert_called_once_with(__name__)

    @patch('app.MeTTa_Query_Generator')
    @patch('app.CypherQueryGenerator')
    def test_databases_setup(self, mock_cypher_generator, mock_metta_generator):
        self.assertIn('metta', databases)
        self.assertIn('cypher', databases)
        mock_metta_generator.assert_called_once_with("Data")
        mock_cypher_generator.assert_called_once_with("./cypher_data")

    @patch('app.SchemaManager')
    def test_schema_manager_setup(self, mock_schema_manager):
        mock_schema_manager.assert_called_once_with(
            schema_config_path='./config/schema_config.yaml',
            biocypher_config_path='./config/biocypher_config.yaml'
        )

    @patch('app.routes')
    def test_routes_imported(self, mock_routes):
        # This test ensures that the routes module is imported
        import app
        # The assertion here is implicit: if the import succeeds without error,
        # it means the routes were imported successfully

    def test_app_configuration(self):
        self.assertFalse(app.config['TESTING'])  # Assuming default Flask configuration

    def test_database_instances(self):
        self.assertIsInstance(databases['metta'], MeTTa_Query_Generator)
        self.assertIsInstance(databases['cypher'], CypherQueryGenerator)

    def test_schema_manager_instance(self):
        self.assertIsInstance(schema_manager, SchemaManager)

if __name__ == '__main__':
    unittest.main()