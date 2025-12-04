"""
Ephemeral node manager implementation for managing ephemeral nodes in a distributed system.
Simulation of distributed locks in Zookeeper-like environment.
"""

from ephemeral_node import EphemeralNode
from datetime import datetime, timezone
from typing import Dict, Optional
import logging

logging.basicConfig(filename='ephemeral_node_manager.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EphemeralNodeManager:
    def __init__(self):
        self.nodes: Dict[str, EphemeralNode] = {}  # Dictionary to hold ephemeral nodes

    def _get_parent_Path(self, path: str) -> str:
        if '/' not in path or path == '/':
            return ''
        return '/'.join(path.rstrip('/').split('/')[:-1])

    def create_node(self, path: str, client_id: str, session_expiry: int) -> EphemeralNode:
        logger.debug("Creating ephemeral node at path: %s for client: %s", path, client_id)
        parent_path = self._get_parent_Path(path)
        if parent_path:
            if not parent_path in self.nodes:
                # WE create the parent node first if it does not exist.
                logger.debug("Parent path %s does not exist. Creating parent node.", parent_path)
                parent_node = EphemeralNode(parent_path, client_id, session_expiry)
                self.nodes[parent_path] = parent_node
            else:
                logger.debug("Parent path %s exists for node creation at path: %s", parent_path, path)  
                # Get the current seq number for this parent path
                parent_node = self.nodes[parent_path]
                seq_num = parent_node.seq_num
                parent_node.increment_seq_num()
                path = f"{path}/{seq_num}"
            node = EphemeralNode(path, client_id, session_expiry)
            
        else:
            logger.error("Cannot create node at path: %s without a valid parent path.", path)
            raise Exception(f"Cannot create node at path: {path} without a valid parent path")

    def get_node(self, path: str) -> Optional[EphemeralNode]:
        logger.info("Retrieving ephemeral node at path: %s", path)
        return self.nodes.get(path)

    def delete_node(self, path: str) -> bool:
        logger.info("Deleting ephemeral node at path: %s", path)
        if path in self.nodes:
            del self.nodes[path]
            return True
        return False

    def cleanup_expired_nodes(self):
        logger.debug("Running cleanup for expired ephemeral nodes at time: %s", datetime.now(timezone.utc))
        current_time = datetime.now(timezone.utc)
        expired_paths = [path for path, node in self.nodes.items() if node.is_expired(current_time)]
        for path in expired_paths:
            logger.debug("Cleaning up expired ephemeral node at path: %s", path)
            self.delete_node(path)

