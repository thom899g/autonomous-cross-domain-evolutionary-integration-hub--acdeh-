from typing import Dict, List, Optional
import logging
from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaConsumer, KafkaProducer
from neo4j.exceptions import ServiceUnavailable
from .knowledge_base import KnowledgeBase

class IntegrationHub:
    """Main integration hub for ACDEH, managing cross-domain AI module interactions."""
    
    def __init__(self):
        self.modules = {}  # type: Dict[str, Dict]
        self.knowledge_base = KnowledgeBase()
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.dashboard_updates = FastAPI()

    def register_module(self, module_id: str, config: Dict) -> None:
        """Register a new AI module with the hub."""
        try:
            if module_id in self.modules:
                raise ValueError(f"Module {module_id} already registered.")
            
            # Validate schema
            self.knowledge_base.validate_schema(module_id, config)
            
            self.modules[module_id] = {
                'config': config,
                'status': 'active',
                'metrics': {'success': 0, 'failure': 0}
            }
            
            logging.info(f"Registered module {module_id}.")
            # Notify dashboard
            self._update_dashboard(f"Module {module_id} registered.")
            
        except Exception as e:
            logging.error(f"Failed to register module {module_id}: {str(e)}")
            raise

    def integrate(self, source: str, target: str, data: Dict) -> Optional[Dict]:
        """Integrate data from one module to another."""
        try:
            # Validate integration
            if not self.knowledge_base.has_integrity_rule(source, target):
                raise ValueError(f"No integration rule between {source} and {target}.")
            
            # Transform data based on rules
            transformed_data = self._transform_data(data, source, target)
            
            # Route to target module via API or message broker
            if self.modules[target].get('endpoint'):
                return self._call_module_api(target, 'integrate', transformed_data)
            else:
                # Publish to Kafka topic
                self.kafka_producer.send(f"{target}_data", value=transformed_data)
                
            logging.info(f"Integrated data from {source} to {target}.")
            self.modules[source]['metrics']['success'] += 1
            
        except Exception as e:
            logging.error(f"Integration failed: {str(e)}")
            self.modules[source]['metrics']['failure'] += 1
            raise

    def _transform_data(self, data: Dict, source: str, target: str) -> Dict:
        """Transform data according to integration rules."""
        try:
            # Get transformation rules from knowledge base
            rules = self.knowledge_base.get_transformation_rules(source, target)
            
            transformed = {}
            for rule in rules:
                if 'fields' in rule:
                    transformed[rule['target']] = data.get(rule['source'])
                
            return transformed
        except Exception as e:
            logging.error(f"Data transformation failed: {str(e)}")
            raise

    def _call_module_api(self, module_id: str, endpoint: str, data: Dict) -> Optional[Dict]:
        """Call an API endpoint of a registered module."""
        try:
            config = self.modules[module_id]['config']
            api_key = config.get('api_key')
            
            # Make authenticated request
            response = requests.post(
                f"{config['endpoint']}/{endpoint}",
                json=data,
                headers={'Authorization': f'Bearer {api_key}'}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise ValueError(f"API call failed: {response.text}")
                
        except requests.exceptions.RequestException as e:
            logging.error(f"API request to {module_id} failed: {str(e)}")
            raise

    def _update_dashboard(self, message: str) -> None:
        """Update dashboard with integration hub status."""
        try:
            # Send update via FastAPI
            self.dashboard_updates.add_route('/update', lambda: message)
            
        except Exception as e:
            logging.error(f"Failed to update dashboard: {str(e)}")