from flask import Flask
import yaml
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from app.services.schema_data import SchemaManager
from app.services.cypher_generator import CypherQueryGenerator
from app.services.metta_generator import MeTTa_Query_Generator
from db import mongo_init
from app.services.llm_handler import LLMHandler
from app.persistence.storage_service import StorageService
import os
config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
)

mongo_init()
""" 
databases = {
    "metta": MeTTa_Query_Generator("./Data"),
    "cypher": CypherQueryGenerator("./cypher_data")
    
    # Add other database instances here
}"""
database_type = config['database']['type']
db_instance = None

if database_type == "cypher":
    db_instance = CypherQueryGenerator("./cypher_data")
elif database_type == "metta":
    db_instance = MeTTa_Query_Generator("./Data")
else:
    raise ValueError(f"Unsupported database type: {database_type}")

llm = LLMHandler()  # Initialize the LLMHandler
storage_service = StorageService() # Initialize the storage service

app.config['llm_handler'] = llm
app.config['storage_service'] = storage_service

schema_manager = SchemaManager(schema_config_path='./config/schema_config.yaml', biocypher_config_path='./config/biocypher_config.yaml')

# Import routes at the end to avoid circular imports
from app import routes

