"""
Python code to periodixally cleanup expired locks in a distributed lock system.
"""
from ephemeral_node_manager import EphemeralNodeManager
from datetime import datetime, timedelta, timezone
import time
import threading
import logging
import asyncio

logging.basicConfig(filename='cleanup.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExpiredLockCleaner:
    def __init__(self, node_manager: EphemeralNodeManager, cleanup_interval: int = 10):
        self.node_manager = node_manager
        self.cleanup_interval = cleanup_interval  # in seconds
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run_cleanup)
        
    async def start(self):
        logger.debug("Starting Expired Lock Cleaner thread.")
        self.thread.start()

    def stop(self):
        logger.debug("Stopping Expired Lock Cleaner thread.")
        self.stop_event.set()
        self.thread.join()

    def _run_cleanup(self):
        while not self.stop_event.is_set():
            time.sleep(self.cleanup_interval)
            self.cleanup_expired_locks()
            self.cleanup_leaf_nodes()

    def cleanup_expired_locks(self):
        logger.debug("Running cleanup for expired locks at time: %s", datetime.now(timezone.utc))   
        self.node_manager.cleanup_expired_nodes()

    def cleanup_leaf_nodes(self):
        logger.debug("Cleaning up leaf nodes at time: %s", datetime.now(timezone.utc))   
        items = list(self.node_manager.nodes.items())     
        for path, node in items:
            child_nodes = self.node_manager._get_child_nodes(path)
            if not child_nodes:
                logger.debug("Cleaning up leaf node at path: %s", path)
                self.node_manager.delete_node(path)

if __name__ == "__main__":
    node_manager = EphemeralNodeManager()
    cleaner = ExpiredLockCleaner(node_manager=node_manager, cleanup_interval=5)
    cleaner.start()
    """
    try:
        # Simulate the main application running
        while True:
            time.sleep(cleaner.cleanup_interval)
            cleaner.cleanup_expired_locks()

    except KeyboardInterrupt:
        cleaner.stop()
    """
    


    



