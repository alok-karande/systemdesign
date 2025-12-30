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

    # Internal helper to get parent path
    def _get_parent_path(self, path: str) -> str:
        if '/' not in path or path == '/':
            return ''
        return '/'.join(path.rstrip('/').split('/')[:-1])

    # Internal helper to get child nodes of a given parent path
    def _get_child_nodes(self, parent_path: str) -> Dict[str, EphemeralNode]:
        child_nodes = {}
        for path, node in self.nodes.items():
            if self._get_parent_path(path) == parent_path:
                child_nodes[path] = node
        return child_nodes
    
    def get_nodes(self) -> Dict[str, EphemeralNode]:
        # Retrieve all ephemeral nodes: Only used internally.
        logger.debug("Retrieving all ephemeral nodes.")
        return self.nodes
    
    # Get current lock owner for a given parent path
    def get_current_lock_owner(self, parent_path: str) -> Optional[str]:
        child_nodes = self._get_child_nodes(parent_path)
        if not child_nodes: # No child nodes exist
            return None
        # Find the child node with the smallest sequence number
        min_seq_node = min(child_nodes.values(), key=lambda n: n.seq_num)
        return min_seq_node.client_id

    # Create an ephemeral node
    def create_node(self, parent_path: str, client_id: str, session_expiry: int) -> str:
        logger.debug("Creating ephemeral node at path: %s for client: %s", parent_path, client_id)
        seq_num = 0
        if parent_path:
            if not parent_path in self.nodes:
                # We create the parent node first if it does not exist.
                logger.debug("Parent path %s does not exist. Creating parent node.", parent_path)
                parent_node = EphemeralNode(parent_path, None, session_expiry, seq_num = 0, is_parent=True)
                self.nodes[parent_path] = parent_node
            else:
                logger.debug("Parent path %s exists", parent_path)
                # Get the current seq number for this parent path
                parent_node = self.nodes[parent_path]
                parent_node.increment_seq_num()
                seq_num = parent_node.seq_num
            path = f"{parent_path}/{seq_num}"
            node = EphemeralNode(path, client_id, session_expiry, seq_num)
            current_owner = self.get_current_lock_owner(parent_path)
            if current_owner is None or current_owner == client_id:
                node.reset_creation_time()  
            self.nodes[path] = node
            return path      
        else:
            logger.error("Cannot create node without a valid parent path for client %s.", client_id)
            raise Exception(f"Cannot create node without a valid parent path for client {client_id}.")

    # Retrieve an ephemeral node by its path
    def get_node(self, path: str) -> Optional[EphemeralNode]:
        logger.info("Retrieving ephemeral node at path: %s", path)
        return self.nodes[path] if path in self.nodes else None

    # Delete an ephemeral node by its path
    def delete_node(self, path: str) -> bool:
        logger.info("Deleting ephemeral node at path: %s", path)
        if path in self.nodes:
            if self.nodes[path].is_parent_node():
                # If it's a parent node, ensure no child nodes exist
                child_nodes = self._get_child_nodes(path)
                if child_nodes:
                    logger.error("Cannot delete parent node at path: %s as it has child nodes.", path)
                    return False
            del self.nodes[path]
            return True
        return False

    # Cleanup expired ephemeral nodes
    def cleanup_expired_nodes(self):
        logger.debug("Running cleanup for expired ephemeral nodes at time: %s", datetime.now(timezone.utc))
        current_time = datetime.now(timezone.utc)
        expired_paths = [path for path, node in self.nodes.items() if node.is_expired(current_time)]
        for path in expired_paths:
            logger.debug("Cleaning up expired ephemeral node at path: %s", path)
            self.delete_node(path)
            # Find the paths's parent and check next current lock owner
            parent_path = self._get_parent_path(path)
            child_nodes = self._get_child_nodes(parent_path)
            if child_nodes:
                # Find the child node with the smallest sequence number
                min_seq_node = min(child_nodes.values(), key=lambda n: n.seq_num)
                logger.debug("Resetting creation time for new lock owner node at path: %s", min_seq_node.path)
                min_seq_node.reset_creation_time()
                logger.debug("New lock owner for parent path %s is client %s with creation time %s", parent_path, min_seq_node.client_id, min_seq_node.creation_time)
