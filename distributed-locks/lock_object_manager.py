"""
This class manages unique lock objects in a distributed system.
It ensures that locks are created, retrieved, and deleted properly.
"""
from datetime import datetime, timedelta, timezone
from lock import Lock
from typing import Dict, Optional
from lock_exceptions import LockAlreadyHeldException
import asyncio
import logging

logging.basicConfig(filename='distributed_locks.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LockObjectManager:
    def __init__(self):
        self.locks: Dict[str, Lock] = {}  # Dictionary to hold lock objects

    def acquire_lock(self, key: str, client_id: str, expiry: int) -> Lock:
        logger.debug("Attempting to acquire lock with key: %s for client: %s", key, client_id)
        # Create a new lock object and store it in the dictionary.
        if not key in self.locks: 
            logger.debug("Creating new lock with key: %s for client: %s", key, client_id)
            lock = Lock(key, client_id, expiry)
            self.locks[key] = lock
            return lock
        elif self.locks[key].client_id == client_id:
            # If the lock already exists and is held by the same client, renew it.
            logger.debug("Renewing lock with key: %s for client: %s", key, client_id)
            self.locks[key].reset_start_time()
            return self.locks[key]
        elif self.locks[key].get_status() == "expired":
            # If the lock has expired, allow a new client to acquire it.
            logger.debug("Acquiring expired lock with key: %s for new client: %s", key, client_id)
            self.locks[key].client_id = client_id
            self.locks[key].reset_start_time()
            return self.locks[key]
        else:
            expiry =  timedelta(seconds=self.expiry) - datetime.now(timezone.utc) - self.start_time  
            logger.error("Lock with key: %s is already held by another client: %s", key, self.locks[key].client_id)

            raise LockAlreadyHeldException(f"Lock is already held by another client {self.locks[key].client_id}")
                        
    def get_lock(self, key: str) -> Optional[Lock]:
        # Retrieve a lock object by its key.
        logger.info("Retrieving lock with key: %s", key)
        return self.locks.get(key)
    
    def get_locks(self) -> Dict[str, Lock]:
        # Retrieve all lock objects: Only used for debugging purposes.
        logger.debug("Retrieving all locks.")
        return self.locks

    def delete_lock(self, key: str) -> bool:
        # Delete a lock object by its key.
        logger.info("Deleting lock with key: %s", key)
        if key in self.locks:
            del self.locks[key]
            return True
        return False    
