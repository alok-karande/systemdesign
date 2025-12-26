"""
Testing distributed lock functionality.
"""
import logging

from lock import Lock
from lock_object_manager import LockObjectManager
from lock_exceptions import LockAlreadyHeldException
from expired_lock_cleaner import ExpiredLockCleaner
import time
import asyncio
from flask import Flask, request


# Initialize Flask app, logger, lock manager, and expired lock cleaner
app = Flask(__name__)
logging.basicConfig(filename='distributed_locks.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
lock_manager = LockObjectManager()
expired_lock_cleaner = ExpiredLockCleaner(lock_manager, cleanup_interval=5)
asyncio.run(expired_lock_cleaner.start())


@app.route("/acquire_lock", methods=["POST"])
def acquire_lock():
    data = request.json
    key = data.get("key")
    client_id = data.get("client_id")
    expiry = data.get("expiry")
    try:
        lock = lock_manager.acquire_lock(key, client_id, expiry)
        return {"status": "success", "lock_key": lock.key, "lock_status": lock.get_status()}, 200
    except LockAlreadyHeldException as e:
        return {"status": "error", "message": str(e)}, 409

@app.route("/release_lock", methods=["POST"])
def release_lock():
    data = request.json
    key = data.get("key")
    client_id = data.get("client_id")
    try:
        if lock_manager.delete_lock(key, client_id):
            return {"status": "success", "message": f"Lock with key {key} released."}, 200
        else:
            return {"status": "error", "message": f"Lock with key {key} not found."}, 404
    except LockAlreadyHeldException as e:
        return {"status": "error", "message": str(e)}, 409

@app.route("/lock_status/<key>", methods=["GET"])
def lock_status(key):
    lock = lock_manager.get_lock(key)
    if lock:
        return {"status": "success", "lock_key": lock.key, "lock_status": lock.get_status(), "client_id": lock.client_id}, 200
    else:
        return {"status": "error", "message": f"Lock with key {key} not found."}, 404
    
@app.route("/all_locks", methods=["GET"])
def all_locks():
    locks = lock_manager.get_locks()
    locks_info = {key: lock.get_status() for key, lock in locks.items()}
    return {"status": "success", "locks": locks_info}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)

    '''
    # Lets create two dummy clients trying to acquire the same lock.
    client_1_id = "client_1"
    client_2_id = "client_2"
    lock_key = "resource_lock"
    lock_expiry = 5  # seconds
    lock_manager = LockObjectManager()
    expired_lock_cleaner = ExpiredLockCleaner(lock_manager, cleanup_interval=5)
    asyncio.run(expired_lock_cleaner.start())

    try:
        lock1 = lock_manager.acquire_lock(lock_key, client_1_id, lock_expiry)
        print(f"Client 1 acquired lock: {lock1.key} with status: {lock1.get_status()}")
        
        # Client 2 tries to acquire the same lock
        lock2 = lock_manager.acquire_lock(lock_key, client_2_id, lock_expiry)
        print(f"Client 2 acquired lock: {lock2.key} with status: {lock2.get_status()}")
    except LockAlreadyHeldException as e:
        print(e)

    # Wait for the lock to expire
    time.sleep(lock_expiry + 1)
    try:
        # Now client 2 should be able to acquire the lock
        lock2 = lock_manager.acquire_lock(lock_key, client_2_id, lock_expiry)
        print(f"Client 2 acquired lock: {lock2.key} with status: {lock2.get_status()}")
    except LockAlreadyHeldException as e:
        print(e)
    
    # Lets wait another 10 seconds to see if the cleanup works
    time.sleep(lock_expiry + 10)
    # Check the logs here

    '''
    