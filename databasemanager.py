import os
import yaml
import logging
from app.services.metta_generator import MeTTa_Query_Generator
from app.services.cypher_generator import CypherQueryGenerator


class DatabaseManager:

    def __init__(self):
        self.db = self.get_database()

    def get_database(self):

        config = self.load_config()
        db = config['database']['type']

        if db == 'metta':
            self.db = MeTTa_Query_Generator("./Data")
        elif db == 'cypher':
            self.db = CypherQueryGenerator("./cypher_data")
        else:
            raise Exception('Wrong Databse in config file')

    def load_config(self):
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.yaml')

        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                logging.info("Configuration loaded successfully.")
            return config
        except FileNotFoundError:
            logging.error(f"Config file not found at: {config_path}")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
            raise
