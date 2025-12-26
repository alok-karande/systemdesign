"""
Python code to periodixally cleanup expired locks in a distributed lock system.
"""
from lock_object_manager import LockObjectManager
from datetime import datetime, timedelta, timezone
import time
import threading
import logging
import asyncio

logging.basicConfig(filename='cleanup.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExpiredLockCleaner:
    def __init__(self, lock_manager: LockObjectManager, cleanup_interval: int = 10):
        self.lock_manager = lock_manager
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

    def cleanup_expired_locks(self):
        logger.debug("Running cleanup for expired locks at time: %s", datetime.now(timezone.utc))   
        items = list(self.lock_manager.locks.items())     
        for key, lock in items:
            # lock.update_status()
            if lock.get_status() == "expired":
                logger.debug("Cleaning up expired lock with key: %s", key)
                self.lock_manager.delete_lock(key, lock.client_id)


if __name__ == "__main__":
    lock_manager = LockObjectManager()
    cleaner = ExpiredLockCleaner(lock_manager, cleanup_interval=5)
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
    


    



