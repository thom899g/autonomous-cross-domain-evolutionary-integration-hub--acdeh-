from typing import Dict, List
import json
from neo4j.exceptions import ServiceUnavailable

class KnowledgeBase:
    """Knowledge base for storing and managing integration rules."""
    
    def __init__(self):
        self.driver = None  # type: Any
        
    def connect(self) -> None:
        """Connect to the graph database."""
        try:
            from neo4j import GraphDatabase
            
            self.driver = GraphDatabase.driver(
                'bolt://localhost:7687',
                auth=('neo4j', 'password')
            )
            
            logging.info("Connected to knowledge base.")
            
        except ServiceUnavailable as e:
            logging.error(f"Failed to connect to knowledge base: {str(e)}")
            raise

    def validate_schema(self, module_id: str, config: Dict) -> None:
        """Validate a module's configuration against the schema."""
        try:
            # Query for existing schema
            with self.driver.session() as session:
                result = session.run(
                    "MATCH (n { moduleId: $moduleId }) RETURN n",
                    moduleId=module_id
                )
                
                if not list(result):
                    raise ValueError(f"Schema for {module_id} not found.")
                    
        except Exception as e:
            logging.error(f"Schema validation failed for {module_id}: {str(e)}")
            raise

    def get_transformation_rules(self, source: str, target: str) -> List[Dict]:
        """Retrieve transformation rules between modules."""
        try:
            with self.driver.session() as session:
                result = session.run(
                    """
                    MATCH (s)-[:CAN_INTEGRATE_WITH]->(t)
                    WHERE s.moduleId = $source AND t.moduleId = $target
                    RETURN s.rules AS rules
                    """,
                    source=source,
                    target